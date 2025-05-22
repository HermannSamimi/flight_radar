import json
import streamlit as st
import pydeck as pdk
import redis

st.set_page_config(page_title="Live Flight Map", layout="wide")
st.title("✈️ Live Flight Radar")


@st.cache_resource
def get_redis_client():
    return redis.Redis.from_url(st.secrets["REDIS_URL"])


client = get_redis_client()
latest = client.get("aircraft_updates")
if not latest:
    st.write("Waiting for data…")
else:
    data = json.loads(latest)
    df = pdk.data_utils.pd.DataFrame(data)

    st.pydeck_chart(
        pdk.Deck(
            initial_view_state=pdk.ViewState(
                latitude=df["latitude"].mean(),
                longitude=df["longitude"].mean(),
                zoom=4,
                pitch=50,
            ),
            layers=[
                pdk.Layer(
                    "ScatterplotLayer",
                    data=df,
                    get_position=["longitude", "latitude"],
                    get_fill_color=[255, 0, 0, 160],
                    get_radius=20000,
                )
            ],
        )
    ) #i