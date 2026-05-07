# 🌿 Real-Time Air Quality Monitoring System

A distributed IoT streaming pipeline for real-time air quality monitoring using an MQ135 gas sensor, Apache Kafka, Apache Spark Structured Streaming, and a Streamlit dashboard.

---

# 📌 Project Overview

This project captures live CO₂ sensor data from an MQ135 sensor connected to Arduino, streams the data through Apache Kafka, processes it using Apache Spark Structured Streaming, and visualizes analytics in a real-time Streamlit dashboard.

The system demonstrates a complete end-to-end streaming architecture suitable for IoT analytics, smart monitoring systems, and real-time data engineering applications.

---

# 🏗️ Architecture

```text
MQ135 Sensor
      ↓
Arduino UNO
      ↓
Serial Communication
      ↓
Python Kafka Producer
      ↓
Apache Kafka
      ↓
Spark Structured Streaming
      ↓
Parquet Storage
      ↓
Streamlit Dashboard
```

---

# 🚀 Features

* Real-time CO₂ monitoring
* Kafka-based data streaming
* Spark Structured Streaming analytics
* Window-based stream processing
* Live dashboard visualization
* Real-time parquet data storage
* Distributed pipeline architecture
* Air quality status classification
* Low-latency stream analytics

---

# 🛠️ Technologies Used

| Technology   | Purpose                    |
| ------------ | -------------------------- |
| Arduino UNO  | Sensor interfacing         |
| MQ135 Sensor | CO₂ detection              |
| Python       | Producer & analytics logic |
| Apache Kafka | Real-time messaging        |
| Apache Spark | Stream processing          |
| PySpark      | Structured streaming       |
| Streamlit    | Dashboard visualization    |
| Pandas       | Data analysis              |
| Parquet      | Streaming data storage     |
| WSL Ubuntu   | Distributed environment    |

---

# 📂 Project Structure

```text
IOT-Streaming-Pipeline/
│
├── arduino/
│   └── mq135_sensor.ino
│
├── producer/
│   └── producer.py
│
├── spark/
│   ├── spark_consumer.py
│   └── spark_analytics.py
│
├── visualization/
│   └── dashboard.py
│
├── output/
│   ├── consumer/
│   ├── alerts/
│   └── window/
│
└── README.md
```

---

# ⚙️ System Requirements

## Hardware

* Arduino UNO
* MQ135 Air Quality Sensor
* USB Cable
* Windows/Linux System

## Software

* Python 3.10+
* Apache Kafka
* Apache Spark 3.5+
* Java 11+
* Streamlit
* WSL Ubuntu

---

# 📦 Python Dependencies

Install required packages:

```bash
pip install kafka-python pyspark pandas pyarrow fastparquet streamlit pyserial
```

---

# 🔥 Kafka Setup

## Step 1 — Start Kafka

```bash
cd ~/kafka

bin/kafka-server-start.sh config/server.properties
```

---

# ⚡ Running the Project

The pipeline runs across multiple terminals.

---

# 🖥️ Terminal 1 — Kafka Server

```bash
cd ~/kafka

bin/kafka-server-start.sh config/server.properties
```

---

# 🖥️ Terminal 2 — Spark Consumer

```bash
cd "/mnt/c/Users/YOUR_USERNAME/OneDrive - K L University/Desktop/IOT-Streaming-Pipeline/IOT-Streaming-Pipeline"

source venv/bin/activate

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

cd spark

spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
spark_consumer.py
```

---

# 🖥️ Terminal 3 — Spark Analytics

```bash
cd "/mnt/c/Users/YOUR_USERNAME/OneDrive - K L University/Desktop/IOT-Streaming-Pipeline/IOT-Streaming-Pipeline"

source venv/bin/activate

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

cd spark

python spark_analytics.py
```

---

# 🖥️ Terminal 4 — Producer

```powershell
cd "C:\Users\YOUR_USERNAME\OneDrive - K L University\Desktop\IOT-Streaming-Pipeline\IOT-Streaming-Pipeline\producer"

python producer.py
```

---

# 🖥️ Terminal 5 — Dashboard

```powershell
cd "C:\Users\YOUR_USERNAME\OneDrive - K L University\Desktop\IOT-Streaming-Pipeline\IOT-Streaming-Pipeline\visualization"

streamlit run dashboard.py
```

---

# 🌐 Dashboard

Open:

```text
http://localhost:8501
```

The dashboard displays:

* Live CO₂ values
* Average CO₂ analytics
* Air quality status
* Window analytics
* Live stream table
* Real-time charts

---

# 📊 Air Quality Classification

| CO₂ Range (ppm) | Status  |
| --------------- | ------- |
| < 800           | FRESH   |
| 800–1200        | NORMAL  |
| 1200–2000       | WARNING |
| > 2000          | DANGER  |

---

# 🧠 Spark Streaming Analytics

The Spark pipeline performs:

* Real-time Kafka ingestion
* JSON stream parsing
* Structured stream processing
* Window aggregation
* Live parquet generation
* Stream analytics computation

---

# 📈 Example Output

```text
[SENT] co2=420 ppm (FRESH)
[SENT] co2=860 ppm (NORMAL)
[SENT] co2=1500 ppm (WARNING)
```

---

# 🎯 Learning Outcomes

This project demonstrates:

* Distributed systems architecture
* Real-time data streaming
* Stream processing pipelines
* Kafka producer-consumer model
* Spark Structured Streaming
* IoT analytics engineering
* Dashboard visualization
* Sensor data processing

---

# 🔮 Future Enhancements

* Multi-sensor integration
* Cloud deployment
* Docker containerization
* ML-based air quality prediction
* Historical analytics
* Email/SMS alerting
* Mobile application integration
* Grafana monitoring

---

# 👨‍💻 Author

**Sainadh Pragada**  
GitHub: [@sainath1754](https://github.com/sainath1754)

---

# 📄 License

This project is developed for academic and educational purposes.
