import streamlit as st
import pandas as pd
import redis
import json
import pydeck as pdk

st.set_page_config(page_title="Live Flight Radar", layout="wide")
st.title("✈️ Live Flight Map")

rc = redis.Redis.from_url(st.secrets["REDIS_URL"])
map_placeholder = st.empty()

def listen_and_render():
    pub = rc.pubsub()
    pub.subscribe("aircraft_updates")
    for msg in pub.listen():
        if msg['type']!= 'message': continue
        data = json.loads(msg['data'])
        df = pd.DataFrame(data)
        layer = pdk.Layer(
            "ScatterplotLayer", df,
            get_position=["longitude","latitude"],
            get_radius=20000,
            pickable=True,
            auto_highlight=True
        )
        deck = pdk.Deck(
            initial_view_state={"latitude":0,"longitude":0,"zoom":1},
            layers=[layer],
            tooltip={"html":"<b>Callsign:</b> {callsign}<br><b>Alt:</b> {altitude} ft"}
        )
        map_placeholder.pydeck_chart(deck)

if __name__ == "__main__":
    listen_and_render()