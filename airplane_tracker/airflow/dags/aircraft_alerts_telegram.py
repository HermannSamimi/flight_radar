import logging
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airplane_tracker.utils.secrets import get_secret
from kafka import KafkaConsumer
import telegram

# Logger
log = logging.getLogger("airflow.alerts")
log.setLevel(logging.INFO)

def alert_unusual_aircraft():
    kaf = get_secret("flight_radar/kafka")
    tg  = get_secret("flight_radar/telegram")

    consumer = KafkaConsumer(
        kaf["topic"],
        bootstrap_servers=kaf["bootstrap_servers"],
        auto_offset_reset="latest",
        consumer_timeout_ms=1000,
        value_deserializer=lambda m: json.loads(m)
    )

    bot = telegram.Bot(token=tg["token"])
    for msg in consumer:
        r = msg.value
        if not r.get("callsign") or r.get("altitude", 0) > 50000:
            text = f"⚠️ Unusual flight: {r}"
            bot.send_message(chat_id=tg["chat_id"], text=text)
            log.info("Alert sent: %s", text)

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # on_failure_callback or email alerts can be added here if desired
}

with DAG(
    dag_id="aircraft_alerts_telegram",
    start_date=datetime(2025, 5, 21),
    schedule="@hourly",      # run once every hour
    catchup=False,
    default_args=default_args,
) as dag:
    PythonOperator(
        task_id="alert_unusual",
        python_callable=alert_unusual_aircraft,
    )