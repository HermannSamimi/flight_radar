from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def alert_unusual_aircraft():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms=5000
    )

    alerts = []
    count = 0

    for msg in consumer:
        count += 1
        data = msg.value
        if not data:
            continue

        callsign = data.get("callsign", "").strip()
        altitude = data.get("altitude") or 0.0

        # Smoke-test threshold (2 500 m ≈ 8 200 ft)
        if not callsign or altitude > 2500:
            alert = (
                f"✈️ *TEST ALERT*\n"
                f"Country: {data.get('origin_country')}\n"
                f"Callsign: {callsign or 'MISSING'}\n"
                f"Altitude: {altitude} m"
            )
            alerts.append(alert)

    print(f"[DEBUG] processed {count} messages, built {len(alerts)} alerts")

    if alerts:
        text = "\n\n".join(alerts)
        send_telegram_alert(text)


def send_telegram_alert(message):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    data = {
        "chat_id": TG_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        resp = requests.post(url, json=data, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print("Failed to send Telegram alert:", e)


with DAG(
    "aircraft_alerts_telegram",
    default_args={"owner": "airflow", "retries": 0},
    description="Alert on missing callsign or test‐threshold altitude via Telegram",
    schedule_interval="@hourly",
    start_date=datetime(2025, 5, 20),
    catchup=False,
) as dag:

    alert_task = PythonOperator(
        task_id="send_telegram_alerts",
        python_callable=alert_unusual_aircraft,
    )