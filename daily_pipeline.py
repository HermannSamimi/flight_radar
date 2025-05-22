#!/usr/bin/env python3
import os
import json
import time
import logging
import requests
import snowflake.connector
from telegram import Bot
from dotenv import load_dotenv

# — Setup & config —
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("daily_pipeline")

API_URL        = os.environ["API_URL"]
API_KEY        = os.environ["API_KEY"]
SNOWFLAKE_CONF = json.loads(os.environ["FLIGHT_RADAR_SNOWFLAKE"])
TELEGRAM_CONF  = json.loads(os.environ["FLIGHT_RADAR_TELEGRAM"])
TELEGRAM_TOKEN = TELEGRAM_CONF["token"]
CHAT_ID        = TELEGRAM_CONF["chat_id"]

# — Fetch flights —
try:
    resp = requests.get(
        API_URL,
        params={"access_key": API_KEY, "limit": 100, "flight_status": "active"},
        timeout=10,
    )
    resp.raise_for_status()
    flights = [
        {
            "icao":     f["flight"].get("icao") or "",
            "callsign": f["flight"].get("iata") or "",
            "latitude": f["live"]["latitude"],
            "longitude":f["live"]["longitude"],
            "altitude": f["live"].get("altitude", 0),
            "country":  f["departure"].get("iata") or "",
            "timestamp": f["live"].get("updated") or int(time.time()),
        }
        for f in resp.json().get("data", [])
        if f.get("live", {}).get("latitude") is not None
           and f.get("live", {}).get("longitude") is not None
    ]
    log.info("Fetched %d live flights", len(flights))
except Exception as e:
    log.error("Failed to fetch flights: %s", e)
    flights = []

if not flights:
    log.info("No flights to ingest; exiting.")
    exit(0)

# — Insert into Snowflake —
try:
    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_CONF["user"],
        password=SNOWFLAKE_CONF["password"],
        account=SNOWFLAKE_CONF["account"],
        role=SNOWFLAKE_CONF.get("role"),
        warehouse=SNOWFLAKE_CONF.get("warehouse"),
        database=SNOWFLAKE_CONF.get("database"),
        schema=SNOWFLAKE_CONF.get("schema"),
    )
    cs = ctx.cursor()
    cs.execute(f"USE DATABASE {SNOWFLAKE_CONF['database']}")
    cs.execute(f"USE SCHEMA {SNOWFLAKE_CONF['schema']}")

    insert_sql = """
        INSERT INTO flights
          (icao, callsign, latitude, longitude, altitude, country, timestamp)
        VALUES
          (%(icao)s, %(callsign)s, %(latitude)s,
           %(longitude)s, %(altitude)s, %(country)s,
           %(timestamp)s)
    """
    for rec in flights:
        cs.execute(insert_sql, rec)
    ctx.commit()
    inserted = len(flights)
    log.info("✅ Inserted %d records into Snowflake", inserted)
finally:
    cs.close()
    ctx.close()

# — Send Telegram notification —
try:
    bot = Bot(token=TELEGRAM_TOKEN)
    msg = f"📥 Today’s ingest: {inserted} flights inserted into Snowflake."
    bot.send_message(chat_id=CHAT_ID, text=msg)
    log.info("✅ Sent Telegram notification")
except Exception as e:
    log.error("Failed to send Telegram message: %s", e)
    exit(1)