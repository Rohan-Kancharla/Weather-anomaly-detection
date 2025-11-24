import os
import json
import time
from pathlib import Path

from filters import StreamingMovingAverage, StreamingEWMA
from ml_model import AnomalyDetector

"""
Kafka Consumer for Real-Time Weather Anomaly Detection

Behavior:
- Default LOCAL mode tails local_stream.jsonl and processes each JSON line.
- Kafka mode consumes from topic 'weather_stream'.

Processing steps per event:
- Apply Moving Average and EWMA filters per metric (temperature, humidity, wind).
- Combine filtered signals to form feature vector.
- Run Isolation Forest to classify Normal/Anomaly.
- Append results to processed_stream.csv for dashboard consumption.
"""

BASE_DIR = Path(__file__).resolve().parent
LOCAL_STREAM_PATH = BASE_DIR / "local_stream.jsonl"
PROCESSED_CSV_PATH = BASE_DIR / "processed_stream.csv"


def ensure_processed_csv_header():
    if not PROCESSED_CSV_PATH.exists():
        PROCESSED_CSV_PATH.write_text(
            "Timestamp,Temp,Humidity,Wind,Status,Score\n", encoding="utf-8"
        )


def append_processed_row(ts, temp, humidity, wind, status, score):
    with PROCESSED_CSV_PATH.open("a", encoding="utf-8") as f:
        f.write(f"{ts},{temp},{humidity},{wind},{status},{score}\n")


def process_event(event, filters, detector: AnomalyDetector):
    ts = event.get("timestamp")
    temp = float(event.get("temperature", 0.0))
    humidity = float(event.get("humidity", 0.0))
    wind = float(event.get("wind_speed", 0.0))

    # Update filters
    temp_ma = filters["temp_ma"].update(temp)
    temp_ewma = filters["temp_ewma"].update(temp)
    hum_ma = filters["hum_ma"].update(humidity)
    hum_ewma = filters["hum_ewma"].update(humidity)
    wind_ma = filters["wind_ma"].update(wind)
    wind_ewma = filters["wind_ewma"].update(wind)

    # Combine MA and EWMA evenly for a robust signal
    temp_f = 0.5 * temp_ma + 0.5 * temp_ewma
    hum_f = 0.5 * hum_ma + 0.5 * hum_ewma
    wind_f = 0.5 * wind_ma + 0.5 * wind_ewma

    status, score = detector.predict([temp_f, hum_f, wind_f])

    # Log to console
    print(
        f"[Consumer] {ts} → Temp: {round(temp_f, 1)}°C, "
        f"Humidity: {round(hum_f, 1)}%, Wind: {round(wind_f, 1)} m/s → "
        + ("✅ Normal" if status == "Normal" else "⚠️ Anomaly Detected")
    )

    # Persist for dashboard
    append_processed_row(ts, round(temp_f, 2), round(hum_f, 2), round(wind_f, 2), status, round(score, 4))


def run_local(from_start: bool = True):
    # Ensure files
    LOCAL_STREAM_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_STREAM_PATH.touch(exist_ok=True)
    ensure_processed_csv_header()

    print("[Consumer] Local mode enabled. Tailing", LOCAL_STREAM_PATH)

    filters = {
        "temp_ma": StreamingMovingAverage(3),
        "temp_ewma": StreamingEWMA(0.3),
        "hum_ma": StreamingMovingAverage(3),
        "hum_ewma": StreamingEWMA(0.3),
        "wind_ma": StreamingMovingAverage(3),
        "wind_ewma": StreamingEWMA(0.3),
    }
    detector = AnomalyDetector()

    with LOCAL_STREAM_PATH.open("r", encoding="utf-8") as f:
        if not from_start:
            # Seek to end so we only process new events
            f.seek(0, os.SEEK_END)

        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                time.sleep(0.5)
                f.seek(pos)
                continue
            try:
                event = json.loads(line)
                process_event(event, filters, detector)
            except Exception as e:
                print("[Consumer] Error parsing line:", e)


def run_kafka():
    from kafka import KafkaConsumer

    ensure_processed_csv_header()
    filters = {
        "temp_ma": StreamingMovingAverage(3),
        "temp_ewma": StreamingEWMA(0.3),
        "hum_ma": StreamingMovingAverage(3),
        "hum_ewma": StreamingEWMA(0.3),
        "wind_ma": StreamingMovingAverage(3),
        "wind_ewma": StreamingEWMA(0.3),
    }
    detector = AnomalyDetector()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "weather_stream")
    print(f"[Consumer] Kafka mode. Bootstrap={bootstrap}, Topic={topic}")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=os.getenv("KAFKA_GROUP", "weather-anomaly-consumer"),
    )

    try:
        for msg in consumer:
            event = msg.value
            process_event(event, filters, detector)
    except KeyboardInterrupt:
        print("[Consumer] Stopped.")


if __name__ == "__main__":
    use_local = os.getenv("USE_LOCAL", "true").strip().lower() == "true"
    from_start = os.getenv("FROM_START", "true").strip().lower() == "true"
    if use_local:
        run_local(from_start=from_start)
    else:
        run_kafka()