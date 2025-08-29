import streamlit as st
import folium
from folium.plugins import Fullscreen
from streamlit_folium import st_folium
import paho.mqtt.client as mqtt
import json
import pandas as pd
import logging
import os
from pathlib import Path
import base64

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MQTT Configuration
BROKER = "localhost"
PORT = 1883
TOPIC = "nsw/fuel/prices"
CSV_FILE = "fuelprice.csv"

# Path to static directory
static_dir = Path(__file__).parent / "static"

# Brand icon file mapping (adjust as needed)
brand_icons = {
    "7-Eleven": "7-Eleven.png",
    "BP": "BP.png",
    "Shell": "Shell.png",
    "Caltex": "Caltex.jpg",
    "EG Ampol": "EG.png",
    "Ampol": "Ampol.png",
    "Metro Fuel": "Metro.jpeg",
    "United": "United.jpeg",
    "Coles Express": "ColesExpress.png",
    "Reddy Express": "ReddyExpress.png",
    "Ultra Petroleum": "UltraPetroleum.jpeg"
}

# Helper to encode image to base64
def get_base64_icon(path):
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except Exception as e:
        logger.warning(f"Missing or unreadable icon: {path}")
        return None

# Global shared variables
fuel_data_global = pd.DataFrame(columns=[
    'brand_id', 'station_id', 'station_code', 'brand', 'name',
    'address', 'suburb', 'state', 'postal_code',
    'latitude', 'longitude', 'is_adblue_available',
    'fuel_type', 'price', 'last_updated'
])
map_data_global = {}

# Load initial data into globals from CSV
if os.path.exists(CSV_FILE):
    try:
        fuel_data_global = pd.read_csv(CSV_FILE)
        fuel_data_global['last_updated'] = pd.to_datetime(
            fuel_data_global['last_updated'], errors='coerce'
        )
        for _, row in fuel_data_global.iterrows():
            if pd.isna(row['fuel_type']) or pd.isna(row['price']):
                continue
            station_code = row['station_code']
            if station_code not in map_data_global:
                map_data_global[station_code] = {
                    'brand': row['brand'],
                    'name': row['name'],
                    'address': f"{row['address']}, {row['suburb']}, {row['state']} {row['postal_code']}",
                    'latitude': row['latitude'],
                    'longitude': row['longitude'],
                    'fuel_prices': {}
                }
            map_data_global[station_code]['fuel_prices'][row['fuel_type']] = {
                'price': row['price'],
                'last_updated': row['last_updated']
            }
        logger.info("Loaded initial data from fuelprice.csv")
    except Exception as e:
        logger.error(f"Error loading fuelprice.csv: {e}")

# MQTT Callbacks
def on_connect(client, userdata, connect_flags, reason_code, properties):
    logger.info(f"Connected to MQTT broker with result code {reason_code}")
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    global fuel_data_global, map_data_global
    try:
        message = json.loads(msg.payload.decode())
        if message.get('last_updated'):
            message['last_updated'] = pd.to_datetime(
                message['last_updated'], format='%d/%m/%Y %H:%M:%S', errors='coerce'
            )
        new_row = pd.DataFrame([message])
        fuel_data_global = pd.concat([fuel_data_global, new_row], ignore_index=True)

        station_code = message['station_code']
        fuel_type = message['fuel_type']
        if station_code not in map_data_global:
            map_data_global[station_code] = {
                'brand': message['brand'],
                'name': message['name'],
                'address': f"{message['address']}, {message['suburb']}, {message['state']} {message['postal_code']}",
                'latitude': message['latitude'],
                'longitude': message['longitude'],
                'fuel_prices': {}
            }
        map_data_global[station_code]['fuel_prices'][fuel_type] = {
            'price': message['price'],
            'last_updated': message['last_updated']
        }

        logger.info(f"Processed MQTT message for station {station_code}, fuel type {fuel_type}")
    except Exception as e:
        logger.error(f"Error processing MQTT message: {e}")

# Initialize MQTT client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
try:
    client.connect(BROKER, PORT, 60)
    client.loop_start()
except Exception as e:
    st.error(f"Error connecting to MQTT broker: {e}")
    st.stop()

# UI setup
st.set_page_config(layout="wide")
st.title("NSW Fuel Price Dashboard")
st.write("Real-time fuel prices across NSW service stations")

# Always sync global data to session state
st.session_state.fuel_data = fuel_data_global.copy()
st.session_state.map_data = map_data_global.copy()

# Filters
all_fuel_types = set()
for station_data in st.session_state.map_data.values():
    all_fuel_types.update(station_data['fuel_prices'].keys())
fuel_types = sorted(list(all_fuel_types))
selected_fuel = st.selectbox("Select Fuel Type", fuel_types)

brands = sorted(set([data['brand'] for data in st.session_state.map_data.values()]))
selected_brands = st.multiselect("Select Brand(s)", options=brands, default=brands[:1])

# Map setup
m = folium.Map(location=[-33.8688, 151.2093], zoom_start=8)
Fullscreen(position='topright').add_to(m)

def get_popup_content(station_data):
    popup_text = f"""
    <b>{station_data['name']}</b><br>
    <b>Brand:</b> {station_data['brand']}<br>
    <b>Address:</b> {station_data['address']}<br>
    <b>Fuel Prices:</b><br>
    """
    for fuel_type, info in station_data['fuel_prices'].items():
        last_updated = info['last_updated'].strftime('%d/%m/%Y %H:%M:%S') if pd.notna(info['last_updated']) else 'Unknown'
        popup_text += f"{fuel_type}: {info['price']} cents (Updated: {last_updated})<br>"
    return popup_text

# Add markers with brand icons and price above icon
for station_code, data in st.session_state.map_data.items():
    if pd.notna(data['latitude']) and pd.notna(data['longitude']):
        if selected_fuel not in data['fuel_prices']:
            continue
        if data['brand'] not in selected_brands:
            continue

        price = data['fuel_prices'][selected_fuel]['price']
        popup_content = get_popup_content(data)

        icon_path = brand_icons.get(data['brand'])
        img_html = ""
        if icon_path:
            full_icon_path = static_dir / icon_path
            base64_img = get_base64_icon(full_icon_path)
            if base64_img:
                img_html = f'<img src="data:image/png;base64,{base64_img}" width="30" height="30">'

        folium.Marker(
            location=[data['latitude'], data['longitude']],
            icon=folium.DivIcon(html=f'''
                <div style="text-align:center; line-height:1;">
                    <div style="background-color:#003366; color:white; font-size:12px; font-weight:bold;
                                padding:2px 5px; border-radius:4px; margin-bottom:2px; display:inline-block;">
                        {price if price else 'N/A'}
                    </div>
                    {img_html}
                </div>
            '''),
            popup=folium.Popup(popup_content, max_width=300),
            tooltip=f"{data['brand']} - {selected_fuel}: {price} cents"
        ).add_to(m)


# Style to prevent elongation
st.markdown(
    """
    <style>
    .map-container .folium-map {
        width: 100% !important;
        height: 85vh !important;
    }
    .main > div:first-child {
        padding-top: 1rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Show map
with st.container():
    st.markdown('<div class="map-container">', unsafe_allow_html=True)
    st_folium(m, use_container_width=True, height=700, key=f"map_{st.session_state.get('update_trigger', 0)}")
    st.markdown('</div>', unsafe_allow_html=True)

# Optional raw data
if st.checkbox("Show Raw Data"):
    st.write(st.session_state.fuel_data)
