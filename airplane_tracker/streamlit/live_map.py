import os
import json
import pandas as pd
import pydeck as pdk
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ✅ MUST be first Streamlit command
st.set_page_config(layout="wide")
st.title("✈️ Real-Time Aircraft Map")

# 🔁 Auto-refresh every 10 seconds
st_autorefresh(interval=10_000, limit=None, key="aircraft_refresh")

DATA_PATH = "data/latest_aircraft.json"

def load_data():
    try:
        with open(DATA_PATH, "r") as f:
            records = json.load(f)
        return pd.DataFrame(records)
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return pd.DataFrame()

df = load_data()

if not df.empty:
    st.markdown(f"**Showing {len(df)} aircrafts**")
    st.dataframe(df.head(10))

    st.pydeck_chart(pdk.Deck(
        initial_view_state=pdk.ViewState(
            latitude=50,
            longitude=10,
            zoom=4,
            pitch=40,
        ),
        layers=[
            pdk.Layer(
                'ScatterplotLayer',
                data=df,
                get_position='[longitude, latitude]',
                get_color='[200, 30, 0, 160]',
                get_radius=10000,
                pickable=True,
                auto_highlight=True,
            )
        ],
    ))
else:
    st.warning("No aircraft data to show.")