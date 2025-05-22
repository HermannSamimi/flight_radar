import os
import json
import time
import logging
import redis
import requests
from dotenv import load_dotenv

load_dotenv()
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
REDIS_URL = os.getenv("REDIS_URL")
REDIS_CHANNEL = os.getenv("REDIS_CHANNEL")

log = logging.getLogger("peek_flights")
logging.basicConfig(level=logging.INFO)


def fetch_adsb_data():
    """Fetches and filters live flight data from AviationStack."""
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
        log.error("Fetch failed: %s", e)
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
                "timestamp": live.get("updated") or time.time(),
            }
        )
    return out


def publish_to_redis(data):
    """Publishes a list of flight records to Redis."""
    client = redis.Redis.from_url(REDIS_URL)
    client.publish(REDIS_CHANNEL, json.dumps(data))


if __name__ == "__main__":
    flights = fetch_adsb_data()
    log.info("Fetched %d flights", len(flights))
    publish_to_redis(flights)
    log.info("Published to Redis")