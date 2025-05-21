import os
import json
import time
import logging
from kafka import KafkaProducer
import redis
from prometheus_client import start_http_server, Counter
from pythonjsonlogger import jsonlogger
from dotenv import load_dotenv

# Load .env vars
load_dotenv()

# — Logging setup
logger = logging.getLogger("flight_radar.producer")
handler = logging.StreamHandler()
handler.setFormatter(jsonlogger.JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# — Metrics
MSG_COUNT = Counter("aircraft_messages_published", "Total aircraft messages published")

# — Kafka client
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
TOPIC = os.getenv("KAFKA_TOPIC", "flights")

# — Redis client
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.Redis.from_url(redis_url)
CHANNEL = "aircraft_updates"

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 10))

def fetch_adsb_data():
    """
    TODO: Implement your ADS-B data fetch logic here.
    Should return a list of dicts, each with keys:
    'icao', 'callsign', 'latitude', 'longitude', 'altitude', 'country', 'timestamp'
    """
    return []

def fetch_and_publish():
    data = fetch_adsb_data()
    if not data:
        logger.warning("No ADS-B data fetched this cycle.")
        return

    for record in data:
        producer.send(TOPIC, record)
        MSG_COUNT.inc()
    producer.flush()
    # Publish batch to UI
    redis_client.publish(CHANNEL, json.dumps(data))
    logger.info("Published %d records to Kafka and Redis", len(data))

if __name__ == "__main__":
    # Start Prometheus metrics endpoint
    start_http_server(8000)
    logger.info("Producer started, polling every %d seconds", POLL_INTERVAL)
    while True:
        fetch_and_publish()
        time.sleep(POLL_INTERVAL)