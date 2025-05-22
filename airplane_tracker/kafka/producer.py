# airplane_tracker/kafka/producer.py

import os
import sys
import time
import json
import logging
import requests

from kafka import KafkaProducer
import redis
from prometheus_client import start_http_server, Counter
from pythonjsonlogger import jsonlogger
from dotenv import load_dotenv

# 1. Load environment variables and configure logging/metrics
load_dotenv()

logger = logging.getLogger("flight_radar.producer")
handler = logging.StreamHandler()
handler.setFormatter(jsonlogger.JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

MSG_COUNT = Counter(
    "aircraft_messages_published",
    "Total aircraft messages published"
)

# 2. Read & validate required env-vars
API_URL         = os.getenv("API_URL")
API_KEY         = os.getenv("AVIATIONSTACK_ACCESS_KEY")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC           = os.getenv("KAFKA_TOPIC")
CHANNEL         = os.getenv("REDIS_CHANNEL")
REDIS_URL       = os.getenv("REDIS_URL")
POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL", "60"))

missing = [
    name for name, val in {
        "API_URL": API_URL,
        "AVIATIONSTACK_ACCESS_KEY": API_KEY,
        "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP,
        "KAFKA_TOPIC": TOPIC,
        "REDIS_CHANNEL": CHANNEL,
        "REDIS_URL": REDIS_URL,
    }.items() if not val
]
if missing:
    logger.error("Missing environment variables: %s", missing)
    sys.exit(1)

# 3. Initialize Kafka & Redis clients
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
redis_client = redis.Redis.from_url(REDIS_URL)

# 4. Fetch from AviationStack
def fetch_adsb_data() -> list[dict]:
    """
    Query the AviationStack API for active flights with live position data.
    Returns a list of simplified dicts.
    """
    try:
        resp = requests.get(
            API_URL,
            params={
                "access_key": API_KEY,
                "limit": 100,
                "flight_status": "active"
            },
            timeout=10
        )
        resp.raise_for_status()
    except Exception as e:
        logger.error("AviationStack fetch failed: %s", e)
        return []

    flights = []
    for f in resp.json().get("data", []):
        live = f.get("live") or {}
        lat = live.get("latitude")
        lon = live.get("longitude")
        if lat is None or lon is None:
            continue

        flights.append({
            "icao":      f.get("flight", {}).get("icao", ""),
            "callsign":  f.get("flight", {}).get("iata", ""),
            "latitude":  lat,
            "longitude": lon,
            "altitude":  live.get("altitude", 0),
            "country":   f.get("departure", {}).get("iata", ""),
            "timestamp": live.get("updated", int(time.time()))
        })

    return flights

# 5. Publish to Kafka & Redis
def fetch_and_publish() -> list[dict]:
    data = fetch_adsb_data()
    if not data:
        logger.warning("No flight data fetched.")
        return []

    for record in data:
        producer.send(TOPIC, record)
        MSG_COUNT.inc()
    producer.flush()

    redis_client.publish(CHANNEL, json.dumps(data))
    logger.info("Published %d records", len(data))

    return data

# 6. CLI entrypoint: start Prometheus metrics and loop
if __name__ == "__main__":
    start_http_server(8000)
    logger.info("Producer started; polling every %d seconds", POLL_INTERVAL)
    while True:
        fetch_and_publish()
        time.sleep(POLL_INTERVAL)