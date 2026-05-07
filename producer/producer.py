import serial
import json
from kafka import KafkaProducer
import time

# ───────────────── CONFIG ─────────────────

SERIAL_PORT = 'COM4'
BAUD_RATE = 9600

KAFKA_HOST = '172.18.141.41:9092'
TOPIC = 'sensor-data'

# ───────────────── SAFE FLOAT ─────────────────

def safe_float(value, default=0.0):

    try:

        value = str(value).strip().lower()

        if value in ['ovf', 'nan', 'inf', '', 'none']:
            return default

        val = float(value)

        # Clamp unrealistic MQ135 spikes
        if val < 0:
            val = 0

        if val > 5000:
            val = 5000

        return round(val, 2)

    except:
        return default

# ───────────────── SERIAL CONNECTION ─────────────────

ser = serial.Serial(
    SERIAL_PORT,
    BAUD_RATE,
    timeout=1
)

# ───────────────── KAFKA PRODUCER ─────────────────

producer = KafkaProducer(
    bootstrap_servers=KAFKA_HOST,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print('=' * 60)
print(' MQ135 Producer Started...')
print('=' * 60)

# ───────────────── MAIN LOOP ─────────────────

while True:

    try:

        if ser.in_waiting > 0:

            line = ser.readline() \
                .decode('utf-8', errors='ignore') \
                .strip()

            # Ignore invalid lines
            if 'RAW_CO2' not in line:
                continue

            parts = {}

            # Parse key:value pairs
            for segment in line.split('|'):

                if ':' in segment:

                    key, _, val = segment.partition(':')

                    parts[key.strip()] = val.strip()

            # ───────────────── EXTRACT VALUES ─────────────────

            warmup_str = parts.get(
                'WARMUP_LEFT',
                '0'
            ).replace('s', '')

            raw_co2 = safe_float(
                parts.get('RAW_CO2', 0)
            )

            avg_co2 = safe_float(
                parts.get('AVG_CO2', 0)
            )

            # Fallback if sensor sends invalid value
            if raw_co2 <= 0:

                raw_co2 = 400
                avg_co2 = 400

            print(
                f'[RAW] '
                f'RAW_CO2={raw_co2} | '
                f'AVG_CO2={avg_co2}'
            )

            # ───────────────── CO2 STATUS ─────────────────

            if raw_co2 < 800:

                co2_status = 'FRESH'

            elif raw_co2 < 1200:

                co2_status = 'NORMAL'

            elif raw_co2 < 2000:

                co2_status = 'WARNING'

            else:

                co2_status = 'DANGER'

            # ───────────────── FINAL PAYLOAD ─────────────────

            data = {

                'raw_co2': raw_co2,
                'avg_co2': avg_co2,

                'co2_status': co2_status,

                'warmup_left': (
                    int(warmup_str)
                    if warmup_str.isdigit()
                    else 0
                ),

                'timestamp': int(time.time())
            }

            # ───────────────── SEND TO KAFKA ─────────────────

            producer.send(
                TOPIC,
                value=data
            )

            producer.flush()

            print(
                f"[SENT] "
                f"co2={data['raw_co2']} ppm "
                f"({data['co2_status']})"
            )

        time.sleep(0.1)

    except KeyboardInterrupt:

        print('\nProducer stopped.')

        ser.close()
        producer.close()

        break

    except Exception as e:

        print(f'[ERROR] {e}')

        time.sleep(0.5)