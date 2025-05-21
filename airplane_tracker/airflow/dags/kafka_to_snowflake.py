import os
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
import snowflake.connector
import requests
from dotenv import load_dotenv

load_dotenv()

# ─── CONFIG ────────────────────────────────────────────────────────────────────
SNOWFLAKE_PARAMS = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").split(",")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ─── NOTIFICATION HELPERS ──────────────────────────────────────────────────────
def send_telegram_notification(message: str):
    """Send a message to Telegram and log the response."""
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
    }
    logger = logging.getLogger("airflow.task")
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("Telegram notification sent: %s", message.replace("\n", " | "))
    except Exception as e:
        logger.error("Failed to send Telegram notification: %s", e)

# ─── MAIN INGEST FUNCTION ──────────────────────────────────────────────────────
def consume_and_store(data_interval_start, data_interval_end, **kwargs):
    logger = logging.getLogger("airflow.task")
    window_start = data_interval_start.strftime("%Y-%m-%d %H:%M:%S")
    window_end   = data_interval_end.strftime("%Y-%m-%d %H:%M:%S")
    logger.info("Starting Kafka → Snowflake ingestion for %s → %s UTC", window_start, window_end)

    # 1) Read from Kafka
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000,
    )

    raw_batch = []
    summary = {}

    for msg in consumer:
        record = msg.value
        raw_batch.append(record)

        country = record.get("origin_country", "UNKNOWN")
        alt = record.get("altitude") or 0.0
        summary.setdefault(country, {"count": 0, "alt_sum": 0.0})
        summary[country]["count"] += 1
        summary[country]["alt_sum"] += alt

    consumer.close()
    logger.info("Consumed %d messages from Kafka", len(raw_batch))

    if not raw_batch:
        logger.warning("No records to process; exiting without notification")
        return

    # 2) Load into Snowflake
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_PARAMS)
        cursor = conn.cursor()

        insert_raw_sql = """
            INSERT INTO PUBLIC.aircraft_raw
              (icao24, callsign, origin_country, longitude, latitude, altitude,
               on_ground, velocity, heading, timestamp)
            VALUES (%(icao24)s, %(callsign)s, %(origin_country)s,
                    %(longitude)s, %(latitude)s, %(altitude)s,
                    %(on_ground)s, %(velocity)s, %(heading)s,
                    TO_TIMESTAMP(%(timestamp)s))
        """
        cursor.executemany(insert_raw_sql, raw_batch)
        logger.info("Inserted %d raw rows", len(raw_batch))

        insert_summary_sql = """
            INSERT INTO PUBLIC.aircraft_summary
              (run_time, origin_country, aircraft_count, avg_altitude)
            VALUES (TO_TIMESTAMP(%(run_time)s), %(origin_country)s,
                    %(aircraft_count)s, %(avg_altitude)s)
        """
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        summary_rows = []
        for country, stats in summary.items():
            summary_rows.append({
                "run_time": now_str,
                "origin_country": country,
                "aircraft_count": stats["count"],
                "avg_altitude": stats["alt_sum"] / stats["count"],
            })
        cursor.executemany(insert_summary_sql, summary_rows)
        logger.info("Inserted %d summary rows", len(summary_rows))

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        logger.exception("Snowflake load failed")
        send_telegram_notification(
            f"❌ *Ingestion failed* for {window_start}→{window_end} UTC:\n```\n{e}\n```"
        )
        raise

    # 3) Success notification
    msg = (
        f"✅ *Data ingestion complete* for {window_start} → {window_end} UTC\n"
        f"• Events sent: *{len(raw_batch)}*\n"
        f"• Countries summarized: *{len(summary)}*"
    )
    send_telegram_notification(msg)
    logger.info("Done.")

# ─── DAG DEFINITION ────────────────────────────────────────────────────────────
with DAG(
    dag_id="kafka_to_snowflake",
    default_args=DEFAULT_ARGS,
    description="Read from Kafka, load to Snowflake, then notify via Telegram",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 5, 20),
    catchup=False,
    tags=["kafka", "snowflake", "telegram"],
) as dag:

    ingest = PythonOperator(
        task_id="consume_and_store",
        python_callable=consume_and_store,
    )

    ingest