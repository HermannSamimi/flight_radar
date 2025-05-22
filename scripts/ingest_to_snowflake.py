import os
import json
import logging
import snowflake.connector
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("ingest_to_snowflake")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights")
SNOWFLAKE_JSON = os.getenv("FLIGHT_RADAR_SNOWFLAKE")

if not (KAFKA_BOOTSTRAP and SNOWFLAKE_JSON):
    log.error("Missing KAFKA_BOOTSTRAP_SERVERS or FLIGHT_RADAR_SNOWFLAKE")
    exit(1)

snowflake_conf = json.loads(SNOWFLAKE_JSON)
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=10_000,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

records = [msg.value for msg in consumer]
consumer.close()

if not records:
    log.info("No records to ingest.")
    exit(0)

ctx = snowflake.connector.connect(
    user=snowflake_conf["user"],
    password=snowflake_conf["password"],
    account=snowflake_conf["account"],
    role=snowflake_conf.get("role"),
    warehouse=snowflake_conf.get("warehouse"),
    database=snowflake_conf.get("database"),
    schema=snowflake_conf.get("schema"),
)
cs = ctx.cursor()
try:
    cs.execute(f"USE DATABASE {snowflake_conf['database']}")
    cs.execute(f"USE SCHEMA {snowflake_conf['schema']}")
    for rec in records:
        cs.execute(
            """
            INSERT INTO flights
              (icao, callsign, latitude, longitude, altitude, country, timestamp)
            VALUES
              (%(icao)s, %(callsign)s, %(latitude)s,
               %(longitude)s, %(altitude)s, %(country)s, %(timestamp)s)
            """,
            rec,
        )
    ctx.commit()
    log.info("✅ Inserted %d records into Snowflake", len(records))
finally:
    cs.close()
    ctx.close()