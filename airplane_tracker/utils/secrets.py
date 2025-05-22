import os, json
from dotenv import load_dotenv

# Load your .env at import time
load_dotenv()

def get_secret(name):
    env_var = name.replace("/", "_").upper()  # e.g. FLIGHT_RADAR_KAFKA
    val = os.getenv(env_var)
    if val is None:
        raise RuntimeError(f"Missing secret env var: {env_var}")
    return json.loads(val)