import os
import json
import time
import random
from datetime import datetime
from pathlib import Path

"""
Kafka Producer for Real-Time Weather Anomaly Detection

Behavior:
- By default, runs in LOCAL mode (no Kafka required) and appends JSON lines
  to local_stream.jsonl in the project folder.
- If USE_LOCAL is set to 'false', it will attempt to publish to Kafka topic
  'weather_stream' using kafka-python.

Data fields per event:
- timestamp (YYYY-MM-DD HH:MM:SS)
- temperature (°C)
- humidity (%)
- wind_speed (m/s)

Simulates realistic values with occasional anomalies.
"""

BASE_DIR = Path(__file__).resolve().parent
LOCAL_STREAM_PATH = BASE_DIR / "local_stream.jsonl"


def generate_weather():
    """Generate a single weather data point with occasional anomalies."""
    # Baseline normal conditions
    temp = random.gauss(30.0, 2.0)  # mean 30°C, std 2
    humidity = random.gauss(55.0, 5.0)  # mean 55%, std 5
    wind = random.gauss(3.0, 1.0)  # mean 3 m/s, std 1

    # Occasionally inject anomalies
    anomaly = random.random() < 0.10  # 10% chance
    if anomaly:
        # Temperature spike or drop
        if random.random() < 0.5:
            temp += random.uniform(10.0, 15.0)
        else:
            temp -= random.uniform(8.0, 12.0)
        # Humidity skew
        humidity += random.uniform(-15.0, 15.0)
        # Wind gust
        wind += random.uniform(3.0, 5.0)

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(temp, 2),
        "humidity": round(humidity, 2),
        "wind_speed": round(wind, 2),
    }


def run_local():
    """Append messages to a local JSONL file to simulate streaming."""
    LOCAL_STREAM_PATH.parent.mkdir(parents=True, exist_ok=True)
    # Ensure file exists
    LOCAL_STREAM_PATH.touch(exist_ok=True)

    print("[Producer] Local mode enabled. Writing to", LOCAL_STREAM_PATH)
    with LOCAL_STREAM_PATH.open("a", encoding="utf-8") as f:
        while True:
            msg = generate_weather()
            f.write(json.dumps(msg) + "\n")
            f.flush()
            print(f"[Producer] Sent data → {json.dumps(msg)}")
            time.sleep(2)


def run_kafka():
    """Publish messages to Kafka topic 'weather_stream'."""
    from kafka import KafkaProducer

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "weather_stream")

    print(f"[Producer] Kafka mode. Bootstrap={bootstrap}, Topic={topic}")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=100,
    )

    try:
        while True:
            msg = generate_weather()
            producer.send(topic, msg)
            producer.flush()
            print(f"[Producer] Sent data → {json.dumps(msg)}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("[Producer] Stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    use_local = os.getenv("USE_LOCAL", "true").strip().lower() == "true"
    if use_local:
        run_local()
    else:
        run_kafka()