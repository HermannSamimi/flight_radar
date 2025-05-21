from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airplane_tracker.utils.secrets import get_secret
import telegram, json
from kafka import KafkaConsumer

log = logging.getLogger("airflow.alerts")

def alert_unusual_aircraft():
    kaf = get_secret("flight_radar/kafka")
    tg  = get_secret("flight_radar/telegram")
    cons = KafkaConsumer(
        kaf["topic"], bootstrap_servers=kaf["bootstrap_servers"],
        auto_offset_reset="latest", consumer_timeout_ms=1000,
        value_deserializer=lambda m: json.loads(m)
    )
    bot = telegram.Bot(token=tg["token"])
    for msg in cons:
        r = msg.value
        if not r.get("callsign") or r["altitude"]>50000:
            text = f"⚠️ Unusual: {r}"
            bot.send_message(chat_id=tg["chat_id"], text=text)
            log.info("Alert sent: %s", text)

with DAG(
    "aircraft_alerts_telegram",
    start_date=datetime(2025,5,21),
    schedule_interval=None,
    catchup=False
) as dag:
    PythonOperator(
        task_id="alert_unusual",
        python_callable=alert_unusual_aircraft
    )