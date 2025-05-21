import os
import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_SERVERS = "localhost:9092"
TOPIC = os.getenv("KAFKA_TOPIC")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS.split(","),   # ensures it's a list
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_aircraft_data():
    url = "https://opensky-network.org/api/states/all"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error fetching OpenSky data: {e}")
    return None

if __name__ == "__main__":
    while True:
        print("Fetching aircraft data...")
        data = fetch_aircraft_data()
        if data and "states" in data:
            print(f"Fetched {len(data['states'])} aircrafts")
            batch = []  # Prepare list for Streamlit

            for state in data["states"][:100]:  # limit for demo
                try:
                    aircraft = {
                        "icao24": state[0],
                        "callsign": state[1].strip() if state[1] else "",
                        "origin_country": state[2],
                        "longitude": state[5],
                        "latitude": state[6],
                        "altitude": state[7],
                        "on_ground": state[8],
                        "velocity": state[9],
                        "heading": state[10],
                        "timestamp": data["time"]
                    }
                    batch.append(aircraft)
                    producer.send(TOPIC, value=aircraft)
                    print("→ Sent:", aircraft)
                except Exception as e:
                    print(f"Parse error: {e}")

            # Save batch to local file for Streamlit
            with open("/Users/hermann/Documents/personal-training/flight_radar/airplane_tracker/data/latest_aircraft.json", "w") as f:
                json.dump(batch, f)

        else:
            print("No valid data received")
        time.sleep(120)