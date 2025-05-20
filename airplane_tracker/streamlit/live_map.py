import os
import json
import pandas as pd
import pydeck as pdk
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ✅ Page layout
st.set_page_config(layout="wide")
st.title("✈️ Real-Time Aircraft Map")

# 🔁 Auto-refresh every 10 seconds
st_autorefresh(interval=10_000, limit=None, key="aircraft_refresh")

# 📁 Data file path
DATA_PATH = "data/latest_aircraft.json"

# 📦 Load JSON as DataFrame
def load_data():
    try:
        with open(DATA_PATH, "r") as f:
            records = json.load(f)
        return pd.DataFrame(records)
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return pd.DataFrame()

# ⬇ Load the data
df = load_data()

# 🗺️ Show map if data is available
if not df.empty:
    st.markdown(f"**Showing {len(df)} aircrafts**")
    st.dataframe(df[['icao24', 'callsign', 'origin_country', 'latitude', 'longitude', 'altitude']].head(10))

    # 🌍 Display aircraft on the map
    st.pydeck_chart(pdk.Deck(
        initial_view_state=pdk.ViewState(
            latitude=df['latitude'].mean() if 'latitude' in df else 50,
            longitude=df['longitude'].mean() if 'longitude' in df else 10,
            zoom=4,
            pitch=40,
        ),
        layers=[
            pdk.Layer(
                "ScatterplotLayer",
                data=df,
                get_position='[longitude, latitude]',
                get_color='[200, 30, 0, 160]',
                get_radius=10000,
                pickable=True,
                auto_highlight=True,
            )
        ],
        tooltip={"text": "Callsign: {callsign}\nCountry: {origin_country}\nAltitude: {altitude}"}
    ))
else:
    st.warning("No aircraft data to show.")