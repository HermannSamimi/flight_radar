import os
import json
import time
import logging
from kafka import KafkaProducer
import redis
from prometheus_client import start_http_server, Counter
from pythonjsonlogger import jsonlogger
from dotenv import load_dotenv

# Load env
load_dotenv()

# Logging
logger = logging.getLogger("flight_radar.producer")
handler = logging.StreamHandler()
handler.setFormatter(jsonlogger.JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Metrics
MSG_COUNT = Counter("aircraft_messages_published", "Total aircraft messages published")

# Kafka
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
TOPIC = os.getenv("KAFKA_TOPIC", "flights")

# Redis
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.Redis.from_url(redis_url)
CHANNEL = "aircraft_updates"

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 10))

def fetch_adsb_data():
    # TODO: implement your ADS-B fetch
    return []

def fetch_and_publish():
    data = fetch_adsb_data()
    if not data:
        logger.warning("No ADS-B data fetched.")
        return

    for rec in data:
        producer.send(TOPIC, rec)
        MSG_COUNT.inc()
    producer.flush()

    redis_client.publish(CHANNEL, json.dumps(data))
    logger.info("Published %d records", len(data))

if __name__ == "__main__":
    start_http_server(8000)
    logger.info("Producer started; polling every %d seconds", POLL_INTERVAL)
    while True:
        fetch_and_publish()
        time.sleep(POLL_INTERVAL)