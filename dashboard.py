import time
from pathlib import Path

import pandas as pd
import streamlit as st
import altair as alt
import matplotlib.pyplot as plt

"""
Streamlit Dashboard for Real-Time Weather Anomaly Detection

Reads processed_stream.csv and visualizes:
- Temperature/Humidity trends (line chart)
- Highlights anomalies in red
- Shows latest few data points
"""

BASE_DIR = Path(__file__).resolve().parent
PROCESSED_CSV_PATH = BASE_DIR / "processed_stream.csv"
IMAGES_DIR = BASE_DIR / "docs" / "images"


def load_data(max_rows: int = 500):
    expected_cols = ["Timestamp", "Temp", "Humidity", "Wind", "Status", "Score"]
    if not PROCESSED_CSV_PATH.exists():
        return pd.DataFrame(columns=expected_cols)

    # Try reading with existing header; fallback to assigning expected headers
    try:
        df = pd.read_csv(PROCESSED_CSV_PATH)
    except Exception:
        df = pd.read_csv(PROCESSED_CSV_PATH, header=None, names=expected_cols)

    # If required columns missing, re-read assuming no header
    if ("Timestamp" not in df.columns) or any(c not in df.columns for c in expected_cols):
        df = pd.read_csv(PROCESSED_CSV_PATH, header=None, names=expected_cols)

    # Remove any stray header rows that might have been appended into the file
    df = df[df["Timestamp"] != "Timestamp"]

    # Convert dtypes robustly
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    for col in ["Temp", "Humidity", "Wind", "Score"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows with invalid core fields
    df = df.dropna(subset=["Timestamp", "Temp", "Humidity", "Wind"])

    # Limit to last N rows for responsiveness
    if len(df) > max_rows:
        df = df.tail(max_rows)
    return df


def resample_10min(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate data into 10-minute intervals for smoother time-based visualization.

    - Numeric fields are averaged per 10-minute bin.
    - Status is set to 'Anomaly' if any record in the bin is anomalous, else 'Normal'.
    """
    if df.empty:
        return df
    df_res = df.copy()
    df_res = df_res.set_index("Timestamp").sort_index()
    agg = {
        "Temp": "mean",
        "Humidity": "mean",
        "Wind": "mean",
        "Score": "mean",
        "Status": lambda s: "Anomaly" if (s == "Anomaly").any() else "Normal",
    }
    df_res = df_res.resample("10T").agg(agg).reset_index()
    return df_res


def temperature_chart(df: pd.DataFrame):
    base = alt.Chart(df).encode(x="Timestamp:T")
    line = base.mark_line(color="steelblue").encode(y=alt.Y("Temp:Q", title="Temperature (°C)"))
    anomalies = base.transform_filter(alt.datum.Status == "Anomaly").mark_point(color="red", size=60).encode(y="Temp:Q")
    return (line + anomalies).properties(height=300)


def humidity_chart(df: pd.DataFrame):
    base = alt.Chart(df).encode(x="Timestamp:T")
    line = base.mark_line(color="#2ca02c").encode(y=alt.Y("Humidity:Q", title="Humidity (%)"))
    anomalies = base.transform_filter(alt.datum.Status == "Anomaly").mark_point(color="red", size=60).encode(y="Humidity:Q")
    return (line + anomalies).properties(height=300)


def save_images(df: pd.DataFrame):
    """Save UI snapshots (temperature and humidity trends) as PNG images."""
    try:
        IMAGES_DIR.mkdir(parents=True, exist_ok=True)

        # Temperature plot
        plt.figure(figsize=(10, 4))
        plt.plot(df["Timestamp"], df["Temp"], color="steelblue", label="Temperature")
        anomalies_mask = df["Status"] == "Anomaly"
        plt.scatter(df.loc[anomalies_mask, "Timestamp"], df.loc[anomalies_mask, "Temp"], color="red", label="Anomaly", zorder=3)
        plt.title("Temperature Trend (Anomalies in Red)")
        plt.xlabel("Time")
        plt.ylabel("Temperature (°C)")
        plt.legend()
        plt.tight_layout()
        plt.savefig(IMAGES_DIR / "temperature_trend.png")
        plt.close()

        # Humidity plot
        plt.figure(figsize=(10, 4))
        plt.plot(df["Timestamp"], df["Humidity"], color="#2ca02c", label="Humidity")
        plt.scatter(df.loc[anomalies_mask, "Timestamp"], df.loc[anomalies_mask, "Humidity"], color="red", label="Anomaly", zorder=3)
        plt.title("Humidity Trend (Anomalies in Red)")
        plt.xlabel("Time")
        plt.ylabel("Humidity (%)")
        plt.legend()
        plt.tight_layout()
        plt.savefig(IMAGES_DIR / "humidity_trend.png")
        plt.close()
    except Exception:
        # Non-fatal: if saving fails, continue without interrupting the dashboard
        pass


def main():
    st.set_page_config(page_title="Real-Time Weather Dashboard", layout="wide")
    st.title("🌡️ Real-Time Weather Dashboard")
    st.write("Blue/Green lines = Normal | Red dots = Anomaly")

    chart_temp = st.empty()
    chart_hum = st.empty()
    stats_placeholder = st.empty()
    table_placeholder = st.empty()

    # Auto-refresh loop (every 5 seconds)
    while True:
        df = load_data(max_rows=500)
        if not df.empty:
            chart_temp.altair_chart(temperature_chart(df), use_container_width=True)
            chart_hum.altair_chart(humidity_chart(df), use_container_width=True)

            # Latest record summary
            latest = df.iloc[-1]
            stats_placeholder.info(
                f"📊 Current Weather: Temp: {latest['Temp']}°C | Humidity: {latest['Humidity']}% "
                f"| Wind: {latest['Wind']} m/s | Status: {'✅ Normal' if latest['Status']=='Normal' else '⚠️ Anomaly'}"
            )

            # Latest few rows
            table_placeholder.dataframe(df.tail(10), use_container_width=True)

            # Save image snapshots for README/docs
            save_images(df)
        else:
            st.warning("Waiting for processed_stream.csv data...")

        # Refresh interval: 5 seconds
        time.sleep(5)


if __name__ == "__main__":
    main()