import streamlit as st
import pandas as pd
import glob
import os
from datetime import datetime

# ───────────────── PAGE CONFIG ─────────────────

st.set_page_config(
    page_title="MQ135 Live Dashboard",
    page_icon="🌿",
    layout="wide"
)

# ───────────────── WINDOWS → WSL PATH ─────────────────

BASE_PATH = r"\\wsl.localhost\Ubuntu\home\sures\output"

CONSUMER_PATH = os.path.join(BASE_PATH, "consumer")
ALERTS_PATH = os.path.join(BASE_PATH, "alerts")
WINDOW_PATH = os.path.join(BASE_PATH, "window")

# ───────────────── LOAD PARQUET ─────────────────

@st.cache_data(ttl=1)
def load_parquet(folder, limit=200):

    try:

        files = glob.glob(
            os.path.join(folder, "*.parquet")
        )

        if not files:
            return pd.DataFrame()

        # newest parquet files first
        files = sorted(
            files,
            key=os.path.getmtime,
            reverse=True
        )

        dfs = []

        # read latest parquet files only
        for f in files[:10]:

            try:

                df = pd.read_parquet(f)

                if not df.empty:
                    dfs.append(df)

            except Exception:
                continue

        if not dfs:
            return pd.DataFrame()

        final_df = pd.concat(
            dfs,
            ignore_index=True
        )

        # remove duplicate timestamps
        if 'timestamp' in final_df.columns:

            final_df = final_df.drop_duplicates(
                subset=['timestamp']
            )

            final_df = final_df.sort_values(
                'timestamp'
            )

        return final_df.tail(limit)

    except Exception as e:

        st.error(f"Parquet Read Error: {e}")

        return pd.DataFrame()

# ───────────────── LOAD DATA ─────────────────

df = load_parquet(CONSUMER_PATH)

alerts_df = load_parquet(ALERTS_PATH)

window_df = load_parquet(WINDOW_PATH)

# ───────────────── TITLE ─────────────────

st.title("🌿 Real-Time Air Quality Monitoring Dashboard")

st.caption(
    "Kafka • Spark Structured Streaming • MQ135 Sensor • Streamlit"
)

st.markdown("---")

# ───────────────── EMPTY CHECK ─────────────────

if df.empty:

    st.warning(
        "No live streaming data available yet."
    )

    st.info(
        "Make sure producer.py and spark_consumer.py are running."
    )

    st.stop()

# ───────────────── CLEAN DATA ─────────────────

numeric_cols = [
    'raw_co2',
    'avg_co2'
]

for col in numeric_cols:

    if col in df.columns:

        df[col] = pd.to_numeric(
            df[col],
            errors='coerce'
        )

df = df.fillna(0)

# ───────────────── LATEST VALUES ─────────────────

latest = df.iloc[-1]

raw_co2 = round(
    float(latest.get('raw_co2', 0)),
    2
)

avg_co2 = round(
    float(latest.get('avg_co2', 0)),
    2
)

co2_status = latest.get(
    'co2_status',
    'UNKNOWN'
)

warmup_left = latest.get(
    'warmup_left',
    0
)

# ───────────────── STATUS COLOR ─────────────────

if co2_status == 'FRESH':

    status_color = "🟢"

elif co2_status == 'NORMAL':

    status_color = "🟡"

elif co2_status == 'WARNING':

    status_color = "🟠"

else:

    status_color = "🔴"

# ───────────────── METRICS ─────────────────

c1, c2, c3, c4 = st.columns(4)

with c1:

    st.metric(
        "Raw CO₂",
        f"{raw_co2} ppm"
    )

with c2:

    st.metric(
        "Average CO₂",
        f"{avg_co2} ppm"
    )

with c3:

    st.metric(
        "Air Quality",
        f"{status_color} {co2_status}"
    )

with c4:

    st.metric(
        "Warmup Left",
        f"{warmup_left}s"
    )

st.markdown("---")

# ───────────────── LIVE CHART ─────────────────

st.subheader("📈 Live CO₂ Analytics")

chart_df = df.tail(100).reset_index(drop=True)

if not chart_df.empty:

    st.line_chart(
        chart_df[
            ['raw_co2', 'avg_co2']
        ],
        height=400,
        width='stretch'
    )

else:

    st.info("No chart data available.")

st.markdown("---")

# ───────────────── WINDOW ANALYTICS ─────────────────

st.subheader("📊 Window Analytics")

if not window_df.empty:

    st.dataframe(
        window_df.tail(20),
        width='stretch',
        height=250
    )

else:

    st.info(
        "Window analytics not available yet."
    )

st.markdown("---")

# ───────────────── LIVE STREAM TABLE ─────────────────

st.subheader("🗃 Live Sensor Stream")

display_cols = [
    'raw_co2',
    'avg_co2',
    'co2_status',
    'warmup_left',
    'timestamp'
]

available_cols = [
    c for c in display_cols
    if c in df.columns
]

if available_cols:

    st.dataframe(
        df[available_cols]
        .tail(20)
        .iloc[::-1]
        .reset_index(drop=True),
        width='stretch',
        height=350
    )

else:

    st.info("No stream table data available.")

st.markdown("---")

# ───────────────── FOOTER ─────────────────

st.caption(
    f"Last Refresh: "
    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
)

st.info(
    "Manual Refresh: Press R or click browser refresh button."
)