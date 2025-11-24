🌦️ Real-Time Weather Anomaly Detection (Streaming + Machine Learning)

A complete end-to-end Streaming Data Analytics (SDA) project that generates real-time weather data, applies SDA filters (Moving Average, EWMA), detects anomalies using Isolation Forest, and visualizes everything in a Streamlit dashboard.
It supports both local mode and Kafka streaming.

📘 Features

✔ Real-time weather data generation (Temperature, Humidity, Wind Speed)
✔ SDA filters → Moving Average, EWMA
✔ Isolation Forest ML-based anomaly detection
✔ Real-time Streamlit dashboard with anomaly alerts
✔ Auto-saves dashboard plots into docs/images/
✔ Works in local mode (file-based streaming)
✔ Works in Kafka mode for real distributed streaming

🏗️ Architecture
[Weather Sensor / Simulated Data]
          ↓
[Producer → Kafka Topic: "weather_stream"]
          ↓
[Consumer]
          ↓
[SDA Filters (Moving Avg, EWMA)]
          ↓
[Isolation Forest ML Model]
          ↓
[Streamlit Dashboard → Real-Time Visualization]

📁 Project Structure
weather_anomaly_detection/
├── producer.py
├── consumer.py
├── filters.py
├── ml_model.py
├── dashboard.py
├── requirements.txt
├── README.md
├── local_stream.jsonl  (auto-created in local mode)
├── processed_stream.csv (auto-created)
└── docs/
    └── images/
        ├── temperature_trend.png
        └── humidity_trend.png

🛠️ Setup Instructions
1️⃣ Create Virtual Environment
cd weather_anomaly_detection
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

▶️ Running in Local Mode (No Kafka Required)

Local mode simulates streaming into a JSON file. The dashboard reads from processed_stream.csv.

Terminal 1 – Producer
cd weather_anomaly_detection
set USE_LOCAL=true
python producer.py

Terminal 2 – Consumer
cd weather_anomaly_detection
set USE_LOCAL=true
set FROM_START=true
python consumer.py

Terminal 3 – Dashboard
cd weather_anomaly_detection
streamlit run dashboard.py


Open the dashboard at:
👉 http://localhost:8501/

⚡ Kafka Mode (For Real-Time Streaming)

You need Kafka running locally or via Docker.

Create Kafka topic:
kafka-topics --create --topic weather_stream --bootstrap-server localhost:9092

Producer:
set USE_LOCAL=false
set KAFKA_BOOTSTRAP=localhost:9092
set KAFKA_TOPIC=weather_stream
python producer.py

Consumer:
set USE_LOCAL=false
set KAFKA_BOOTSTRAP=localhost:9092
set KAFKA_TOPIC=weather_stream
python consumer.py


Dashboard remains the same.

🌍 Environment Variables
Variable	Purpose
USE_LOCAL	true = local file streaming, false = Kafka
FROM_START	Read from beginning of file
KAFKA_BOOTSTRAP	Kafka host (default: localhost:9092)
KAFKA_TOPIC	Kafka topic name
KAFKA_GROUP	Consumer group id
📊 Sample Logs
Producer Output
[Producer] Sent → {"timestamp": "2025-11-09 12:30:02", "temperature": 33.5, "humidity": 58.3, "wind_speed": 2.7}

Consumer Output
[Consumer] 2025-11-09 12:30:06 → Temp: 45.0°C → ⚠️ Anomaly

Dashboard Sample
🌡️ Temperature Trend (Live)
🟢 Normal readings
🔴 Anomaly spikes highlighted

🧠 ML Model

Isolation Forest is trained on simulated normal ranges.

Based on features:

Moving Average

EWMA

Raw values

Tune sensitivity using contamination in ml_model.py.

🛑 Troubleshooting
Dashboard shows no data?

Ensure consumer is running

Delete processed_stream.csv and restart consumer

High false positives?

Increase EWMA smoothing

Decrease IsolationForest contamination

Kafka errors?

Check Zookeeper & Kafka running

Verify KAFKA_BOOTSTRAP address

🧹 Cleanup
del local_stream.jsonl
del processed_stream.csv

📄 License

This project is for academic & demonstration purposes.
