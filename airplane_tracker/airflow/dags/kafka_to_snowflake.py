from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
from kafka import KafkaConsumer
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

SNOWFLAKE_PARAMS = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE")
}


def consume_and_load():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms=10000
    )

    conn = snowflake.connector.connect(**SNOWFLAKE_PARAMS)
    cursor = conn.cursor()

    raw_batch = []
    summary = {}

    for msg in consumer:
        d = msg.value
        raw_batch.append(d)

        country = d.get("origin_country")
        alt = d.get("altitude") or 0.0
        if country not in summary:
            summary[country] = {"count": 0, "alt_sum": 0.0}
        summary[country]["count"] += 1
        summary[country]["alt_sum"] += alt

    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Insert raw
    for row in raw_batch:
        cursor.execute("""
            INSERT INTO PUBLIC.aircraft_raw 
            (icao24, callsign, origin_country, longitude, latitude, altitude, on_ground, velocity, heading, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TO_TIMESTAMP(%s))
        """, (
            row["icao24"], row["callsign"], row["origin_country"],
            row["longitude"], row["latitude"], row["altitude"],
            row["on_ground"], row["velocity"], row["heading"], row["timestamp"]
        ))

    # Insert summary
    for country, data in summary.items():
        avg_alt = data["alt_sum"] / data["count"]
        cursor.execute("""
            INSERT INTO PUBLIC.aircraft_summary (run_time, origin_country, aircraft_count, avg_altitude)
            VALUES (TO_TIMESTAMP(%s), %s, %s, %s)
        """, (now, country, data["count"], avg_alt))

    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    "kafka_to_snowflake",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2)
    },
    description="Read from Kafka and load to Snowflake",
    schedule_interval="@hourly",
    start_date=datetime(2025, 5, 20),
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id="consume_kafka_and_store_in_snowflake",
        python_callable=consume_and_load
    )