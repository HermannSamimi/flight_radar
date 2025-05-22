import os
import json
import logging
from kafka import KafkaConsumer
from telegram import Bot
from telegram.constants import ParseMode
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("send_alerts")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights")
TELEGRAM_JSON = os.getenv("FLIGHT_RADAR_TELEGRAM")
THRESHOLD_FT = int(os.getenv("ALERT_ALTITUDE_FT", "40000"))

if not (KAFKA_BOOTSTRAP and TELEGRAM_JSON):
    log.error("Missing KAFKA_BOOTSTRAP_SERVERS or FLIGHT_RADAR_TELEGRAM")
    exit(1)

telegram_conf = json.loads(TELEGRAM_JSON)
bot = Bot(token=telegram_conf["token"])
chat_id = telegram_conf["chat_id"]

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="latest",
    consumer_timeout_ms=5_000,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

sent = 0
for msg in consumer:
    rec = msg.value
    if rec.get("altitude", 0) > THRESHOLD_FT:
        text = (
            f"🚨 *High Altitude Alert*\n"
            f"• Callsign: `{rec['callsign']}`\n"
            f"• Altitude: {rec['altitude']} ft\n"
            f"• Location: ({rec['latitude']:.3f}, {rec['longitude']:.3f})"
        )
        bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.MARKDOWN)
        sent += 1

consumer.close()
log.info("✅ Sent %d alerts", sent)