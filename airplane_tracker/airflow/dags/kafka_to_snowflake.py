from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer, TopicPartition
import snowflake.connector, json, logging
from airflow.utils.email import send_email
from airplane_tracker.utils.secrets import get_secret

log = logging.getLogger("airflow.kafka_to_snowflake")

def notify_failure(context):
    send_email(to="ops@example.com",
               subject="DAG Failure: kafka_to_snowflake",
               html_content=str(context))

def consume_and_store():
    kaf = get_secret("flight_radar/kafka")
    sf  = get_secret("flight_radar/snowflake")
    consumer = KafkaConsumer(
        bootstrap_servers=kaf["bootstrap_servers"],
        group_id="airflow_consumers",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m)
    )
    tp = TopicPartition(kaf["topic"], 0)
    consumer.assign([tp])
    last = get_last_offset_from_db()
    consumer.seek(tp, last)

    ctx = snowflake.connector.connect(
        user=sf["user"], account=sf["account"], password=sf["password"],
        warehouse=sf["warehouse"], database=sf["database"], schema=sf["schema"]
    )
    cs = ctx.cursor()
    for msg in consumer:
        rec = msg.value
        cs.execute(
            "MERGE INTO aircraft t USING (SELECT %(icao)s AS icao, %(ts)s AS ts) s "
            "ON t.icao=s.icao AND t.ts=s.ts "
            "WHEN NOT MATCHED THEN INSERT (...) VALUES (...)"
        , rec)
        consumer.commit()
    ctx.commit()
    cs.close()
    ctx.close()

with DAG(
    "kafka_to_snowflake",
    start_date=datetime(2025,5,21),
    schedule_interval=None,
    catchup=False,
    default_args={
      "retries":3,
      "retry_delay":timedelta(minutes=2),
      "on_failure_callback":notify_failure
    }
) as dag:
    PythonOperator(
        task_id="consume_and_store",
        python_callable=consume_and_store
    )