import streamlit as st
import redis
from airplane_tracker.kafka.consumer import listen_and_publish


def get_redis_client():
    """Create and return a Redis client using the REDIS_URL secret."""
    url = st.secrets.get("REDIS_URL")
    if not url:
        st.error("🔒 Missing REDIS_URL in Streamlit secrets. Please configure it in .streamlit/secrets.toml.")
        st.stop()
    return redis.Redis.from_url(url)


def listen_and_render():
    """Listen for flight messages and render them in Streamlit, also publishing to Redis."""
    # Configure Streamlit page
    st.set_page_config(page_title="Live Flight Map", layout="wide")
    st.title("✈️ Live Flight Map")

    # Get Redis client
    rc = get_redis_client()

    # Container for map updates
    map_container = st.empty()

    for msg in listen_and_publish():
        # Publish to Redis channel
        rc.publish("flights", msg)
        # Parse message (assumed JSON with latitude/longitude)
        try:
            data = msg if isinstance(msg, dict) else st.json(msg)
        except Exception:
            data = {}

        # Display on map
        if data.get("latitude") and data.get("longitude"):
            map_container.map([{
                "lat": data["latitude"],
                "lon": data["longitude"]
            }])
        else:
            st.write(f"Received: {msg}")


if __name__ == "__main__":
    listen_and_render()
