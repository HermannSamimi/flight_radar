import os
import json
import time
import logging
from kafka import KafkaProducer
import redis
import requests
from prometheus_client import start_http_server, Counter
from pythonjsonlogger import jsonlogger
from dotenv import load_dotenv

load_dotenv()

# — Logging & metrics (unchanged) —
logger = logging.getLogger("flight_radar.producer")
handler = logging.StreamHandler()
handler.setFormatter(jsonlogger.JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)
MSG_COUNT = Counter("aircraft_messages_published", "Total aircraft messages published")

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer=lambda v: json.dumps(v).encode()
)
redis_client = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
TOPIC = os.getenv("KAFKA_TOPIC", "flights")
CHANNEL = "aircraft_updates"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 10))

# — AviationStack API endpoint & key —
API_URL = "http://api.aviationstack.com/v1/flights"
API_KEY = os.getenv("AVIATIONSTACK_ACCESS_KEY")

def fetch_adsb_data():
    """
    Fetches flights from AviationStack (limited to 100 req/month).
    Returns only those with live position data.
    """
    try:
        resp = requests.get(API_URL, params={
            "access_key": API_KEY,
            "limit": 100,
            "flight_status": "active"
        }, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        logger.error("AviationStack fetch failed: %s", e)
        return []

    out = []
    for f in resp.json().get("data", []):
        live = f.get("live")
        if not live or live.get("latitude") is None or live.get("longitude") is None:
            continue
        out.append({
            "icao":    f["flight"].get("icao") or "",
            "callsign":f["flight"].get("iata") or "",
            "latitude":live["latitude"],
            "longitude":live["longitude"],
            "altitude":live.get("altitude") or 0,
            "country": f["departure"].get("iata") or "",
            "timestamp": live.get("updated") or int(time.time())
        })
    return out

def fetch_and_publish():
    data = fetch_adsb_data()
    if not data:
        logger.warning("No flight data fetched.")
        return

    for rec in data:
        producer.send(TOPIC, rec)
        MSG_COUNT.inc()
    producer.flush()

    redis_client.publish(CHANNEL, json.dumps(data))
    logger.info("Published %d records to Kafka and Redis", len(data))

if __name__ == "__main__":
    start_http_server(8000)
    logger.info("Producer started; polling every %d seconds", POLL_INTERVAL)
    while True:
        fetch_and_publish()
        time.sleep(POLL_INTERVAL)