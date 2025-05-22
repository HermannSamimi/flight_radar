import os
import sys
import time
import json
import logging
import redis
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
from pythonjsonlogger import jsonlogger
from prometheus_client import start_http_server, Counter

load_dotenv()

# Required environment variables
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = os.getenv("KAFKA_TOPIC")
REDIS_URL = os.getenv("REDIS_URL")
CHANNEL = os.getenv("REDIS_CHANNEL")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

missing = [
    k
    for k, v in {
        "API_URL": API_URL,
        "API_KEY": API_KEY,
        "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP,
        "KAFKA_TOPIC": TOPIC,
        "REDIS_URL": REDIS_URL,
        "REDIS_CHANNEL": CHANNEL,
    }.items()
    if not v
]
if missing:
    print(f"Missing env vars: {missing}", file=sys.stderr)
    sys.exit(1)

# Logging & metrics
logger = logging.getLogger("flight_radar.producer")
handler = logging.StreamHandler()
handler.setFormatter(jsonlogger.JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)
MSG_COUNT = Counter("aircraft_messages_published", "Total messages sent")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode(),
)
redis_client = redis.Redis.from_url(REDIS_URL)


def fetch_adsb_data():
    """Fetch flights with live position from AviationStack."""
    try:
        resp = requests.get(
            API_URL,
            params={
                "access_key": API_KEY,
                "limit": 100,
                "flight_status": "active",
            },
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.error("Fetch failed: %s", e)
        return []

    out = []
    for f in resp.json().get("data", []):
        live = f.get("live") or {}
        lat = live.get("latitude")
        lon = live.get("longitude")
        if lat is None or lon is None:
            continue
        out.append(
            {
                "icao": f["flight"].get("icao") or "",
                "callsign": f["flight"].get("iata") or "",
                "latitude": lat,
                "longitude": lon,
                "altitude": live.get("altitude") or 0,
                "country": f["departure"].get("iata") or "",
                "timestamp": live.get("updated") or int(time.time()),
            }
        )
    return out


def fetch_and_publish():
    """Fetches data, sends to Kafka, and publishes to Redis."""
    data = fetch_adsb_data()
    if not data:
        logger.warning("No flight data fetched.")
        return []

    for rec in data:
        producer.send(TOPIC, rec)
        MSG_COUNT.inc()
    producer.flush()

    redis_client.publish(CHANNEL, json.dumps(data))
    logger.info("Published %d records to Kafka and Redis", len(data))
    return data


if __name__ == "__main__":
    start_http_server(8000)
    logger.info("Producer started; polling every %d seconds", POLL_INTERVAL)
    while True:
        fetch_and_publish()
        time.sleep(POLL_INTERVAL)