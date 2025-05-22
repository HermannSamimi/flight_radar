import logging
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
import snowflake.connector

from airplane_tracker.utils.secrets import get_secret

# Logger
log = logging.getLogger("airflow.raw_flights_ingest")
log.setLevel(logging.INFO)

def consume_and_store():
    # Fetch secrets
    kaf = get_secret("flight_radar/kafka")
    sf  = get_secret("flight_radar/snowflake")

    # Set up Kafka consumer
    consumer = KafkaConsumer(
        kaf["topic"],
        bootstrap_servers=kaf["bootstrap_servers"],
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m)
    )

    # Connect to Snowflake
    ctx = snowflake.connector.connect(
        user=sf["user"],
        account=sf["account"],
        password=sf["password"],
        warehouse=sf["warehouse"],
        database=sf["database"],
        schema=sf["schema"]
    )
    cs = ctx.cursor()

    # Rebuild table
    cs.execute("DROP TABLE IF EXISTS raw_flights")
    cs.execute("""
        CREATE TABLE raw_flights (
          ingestion_time TIMESTAMP_LTZ,
          data           VARIANT
        )
    """)
    ctx.commit()

    # Consume everything in Kafka and load
    for msg in consumer:
        rec_json = json.dumps(msg.value)
        cs.execute(
            """
            INSERT INTO raw_flights (ingestion_time, data)
            VALUES (current_timestamp(), parse_json(%(j)s))
            """,
            {"j": rec_json}
        )

    ctx.commit()
    cs.close()
    ctx.close()
    log.info("Finished ingesting Kafka messages into Snowflake.")

# Default arguments for retry behavior
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="raw_flights_ingest",
    start_date=datetime(2025, 5, 21),
    schedule=None,   # or schedule="0 6 * * *" for daily 08:00 EET
    catchup=False,
    default_args=default_args,
) as dag:

    ingest = PythonOperator(
        task_id="consume_and_store",
        python_callable=consume_and_store,
    )