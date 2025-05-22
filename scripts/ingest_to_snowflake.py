# scripts/ingest_to_snowflake.py

import os
import json
import logging
from datetime import datetime
import snowflake.connector
from kafka import KafkaConsumer
from dotenv import load_dotenv

# 1. Load env and configure logging
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("ingest_to_snowflake")

# 2. Read connection info
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "flights")
SNOWFLAKE_JSON  = os.getenv("FLIGHT_RADAR_SNOWFLAKE")  # must be JSON-encoded

if not (KAFKA_BOOTSTRAP and SNOWFLAKE_JSON):
    log.error("Missing KAFKA_BOOTSTRAP_SERVERS or FLIGHT_RADAR_SNOWFLAKE in .env")
    exit(1)

snowflake_conf = json.loads(SNOWFLAKE_JSON)

# 3. Consume from Kafka with a timeout
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=10_000,  # <--- wait max 10s then stop
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

records = [msg.value for msg in consumer]
consumer.close()

if not records:
    log.info("No records to ingest.")
    exit(0)

# 4. Connect to Snowflake and insert
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
    for rec in records:
        cs.execute(
            """
            INSERT INTO flights
              (icao, callsign, latitude, longitude, altitude, country, timestamp)
            VALUES
              (%(icao)s,
               %(callsign)s,
               %(latitude)s,
               %(longitude)s,
               %(altitude)s,
               %(country)s,
               %(timestamp)s)
            """,
            rec
        )
    ctx.commit()
    log.info("✅ Inserted %d records into Snowflake", len(records))
finally:
    cs.close()
    ctx.close()