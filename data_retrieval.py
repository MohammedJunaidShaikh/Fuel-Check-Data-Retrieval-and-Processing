import requests
import base64
import pandas as pd
import uuid
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
import json
import time
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MQTT Configuration
BROKER = "localhost"
PORT = 1883
TOPIC = "nsw/fuel/prices"

# API Configuration
BASE_URL = "https://api.onegov.nsw.gov.au"
TOKEN_URL = f"{BASE_URL}/oauth/client_credential/accesstoken"
INITIAL_FUEL_URL = f"{BASE_URL}/FuelPriceCheck/v1/fuel/prices"
UPDATE_FUEL_URL = f"{BASE_URL}/FuelPriceCheck/v1/fuel/prices/new"
API_KEY = "jcX7a71NC5QoatwlDGCkEAxPHASGFb8h"
API_SECRET = "ZBbazTtrp96XXlLk"
OUTPUT_FILE = "fuelprice.csv"

# Global MQTT client
client = None

# MQTT Callbacks
def on_connect(client, userdata, connect_flags, reason_code, properties):
    logger.info(f"Connected to MQTT broker with result code {reason_code}")
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    logger.info(f"Received message on topic {msg.topic}: {msg.payload.decode()}")

def initialize_mqtt_client():
    """Initialize and connect MQTT client."""
    global client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(BROKER, PORT, 60)
        client.loop_start()
        logger.info("MQTT client initialized and connected")
    except Exception as e:
        logger.error(f"Error connecting to MQTT broker: {e}")
        exit(1)

def fetch_access_token():
    """Fetch OAuth access token using Basic Auth."""
    credentials = f"{API_KEY}:{API_SECRET}"
    basic_auth_header = f"Basic {base64.b64encode(credentials.encode()).decode()}"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": basic_auth_header
    }
    params = {"grant_type": "client_credentials"}
    
    try:
        response = requests.get(TOKEN_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        return response.json().get("access_token")
    except requests.RequestException as e:
        logger.error(f"Error fetching access token: {e}")
        raise

def fetch_fuel_data(access_token, url):
    """Fetch fuel data from the specified API endpoint."""
    transaction_id = str(uuid.uuid4())
    request_timestamp = datetime.now(timezone.utc).strftime("%d/%m/%Y %I:%M:%S %p")
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": f"Bearer {access_token}",
        "apikey": API_KEY,
        "transactionid": transaction_id,
        "requesttimestamp": request_timestamp
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching fuel data from {url}: {e}")
        return {"stations": [], "prices": []}

def process_fuel_data(data):
    """Process raw API data into a DataFrame."""
    # Create stations DataFrame
    stations_data = []
    for station in data.get('stations', []):
        location = station.get('location', {})
        stations_data.append({
            'brand_id': station.get('brandid', ''),
            'station_id': station.get('stationid', ''),
            'station_code': station.get('code'),
            'brand': station.get('brand', 'Unknown'),
            'name': station.get('name', 'Unknown'),
            'address': station.get('address', 'Unknown'),
            'latitude': location.get('latitude') if isinstance(location, dict) else None,
            'longitude': location.get('longitude') if isinstance(location, dict) else None,
            'is_adblue_available': station.get('isAdBlueAvailable', False)
        })
    stations_df = pd.DataFrame(stations_data)

    # Create prices DataFrame
    prices_data = []
    for price in data.get('prices', []):
        prices_data.append({
            'station_code': price.get('stationcode'),
            'fuel_type': price.get('fueltype'),
            'price': price.get('price'),
            'last_updated': price.get('lastupdated')
        })
    prices_df = pd.DataFrame(prices_data)

    # Clean stations DataFrame
    stations_df.fillna({'brand': 'Unknown', 'name': 'Unknown', 'address': 'Unknown'}, inplace=True)
    stations_df['latitude'] = pd.to_numeric(stations_df['latitude'], errors='coerce')
    stations_df['longitude'] = pd.to_numeric(stations_df['longitude'], errors='coerce')
    stations_df = stations_df[stations_df['station_code'].notna()]

    # Clean prices DataFrame
    prices_df.fillna({'price': 0.0}, inplace=True)
    prices_df['price'] = pd.to_numeric(prices_df['price'], errors='coerce')
    prices_df['last_updated'] = pd.to_datetime(prices_df['last_updated'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    prices_df = prices_df[prices_df['station_code'].notna()]

    # Merge DataFrames
    new_df = pd.merge(stations_df, prices_df, on='station_code', how='left')

    # Split address into components
    address_split = new_df['address'].str.split(', ', expand=True)
    new_df['suburb'] = None
    new_df['state'] = None
    new_df['postal_code'] = None
    if address_split.shape[1] > 1:
        location_split = address_split[1].str.rsplit(' ', n=2, expand=True)
        new_df['suburb'] = location_split[0] if location_split.shape[1] > 0 else None
        new_df['state'] = location_split[1] if location_split.shape[1] > 1 else None
        new_df['postal_code'] = location_split[2] if location_split.shape[1] > 2 else None
        new_df['address'] = address_split[0]

    # Reorder columns
    new_df = new_df[[
        'brand_id', 'station_id', 'station_code', 'brand', 'name',
        'address', 'suburb', 'state', 'postal_code',
        'latitude', 'longitude', 'is_adblue_available',
        'fuel_type', 'price', 'last_updated'
    ]]

    return new_df

def update_fuel_csv_and_publish(new_df, is_initial=False):
    """Update CSV file and publish records to MQTT."""
    try:
        # Load existing CSV or create empty DataFrame
        try:
            existing_df = pd.read_csv(OUTPUT_FILE)
            existing_df['last_updated'] = pd.to_datetime(existing_df['last_updated'], errors='coerce')
        except FileNotFoundError:
            logger.warning(f"{OUTPUT_FILE} not found. Creating new DataFrame.")
            existing_df = pd.DataFrame(columns=[
                'brand_id', 'station_id', 'station_code', 'brand', 'name',
                'address', 'suburb', 'state', 'postal_code',
                'latitude', 'longitude', 'is_adblue_available',
                'fuel_type', 'price', 'last_updated'
            ])

        # For initial data, replace existing CSV; for updates, append and deduplicate
        if is_initial:
            combined_df = new_df
        else:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.sort_values(['station_code', 'fuel_type', 'last_updated'])
            combined_df = combined_df.groupby(['station_code', 'fuel_type']).last().reset_index()

        # Save to CSV
        combined_df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"Updated data saved to {OUTPUT_FILE}")

        # Publish new or updated records to MQTT
        publish_df = new_df if is_initial else combined_df[combined_df['last_updated'] == new_df['last_updated'].max()]
        for _, row in publish_df.iterrows():
            if pd.isna(row['fuel_type']) or pd.isna(row['price']):
                continue  # Skip rows with missing fuel type or price
            message = row.to_dict()
            if pd.notna(message['last_updated']):
                message['last_updated'] = message['last_updated'].strftime('%d/%m/%Y %H:%M:%S')
            readable_message = (
                f"Station {message['station_code']} ({message['name']}), "
                f"Brand: {message['brand']}, Fuel Type: {message['fuel_type']}, "
                f"Price: {message['price']} cents, Last Updated: {message['last_updated']}, "
                f"Address: {message['address']}, {message['suburb']}, {message['state']} {message['postal_code']}"
            )
            client.publish(TOPIC, json.dumps(message))
            logger.info(f"Published: {readable_message}")
            time.sleep(0.1)  # 0.1-second delay between messages

    except Exception as e:
        logger.error(f"Error updating CSV or publishing to MQTT: {e}")

def main():
    """Main execution loop."""
    initialize_mqtt_client()
    access_token = None
    token_expiry = 0

    try:
        # Fetch initial data
        current_time = time.time()
        if not os.path.exists(OUTPUT_FILE):
            if current_time >= token_expiry:
                access_token = fetch_access_token()
                token_expiry = current_time + 3600  # Assume token valid for 1 hour
            initial_data = fetch_fuel_data(access_token, INITIAL_FUEL_URL)
            if initial_data.get('stations', []) or initial_data.get('prices', []):
                initial_df = process_fuel_data(initial_data)
                update_fuel_csv_and_publish(initial_df, is_initial=True)
                logger.info("Initial data processed and published")

        # Continuous update loop
        while True:
            # Refresh token if expired
            current_time = time.time()
            if current_time >= token_expiry:
                access_token = fetch_access_token()
                token_expiry = current_time + 3600

            # Fetch and process updated fuel data
            update_data = fetch_fuel_data(access_token, UPDATE_FUEL_URL)
            if not update_data.get('stations', []) and not update_data.get('prices', []):
                logger.info("No new data received, continuing to next fetch")
                # time personally.sleep(60)
                time.sleep(60)
                continue

            update_df = process_fuel_data(update_data)
            if not update_df.empty:
                update_fuel_csv_and_publish(update_df, is_initial=False)

            # Wait 60 seconds before next fetch
            time.sleep(60)

    except KeyboardInterrupt:
        logger.info("Stopping MQTT client...")
        client.loop_stop()
        client.disconnect()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if client:
            client.loop_stop()
            client.disconnect()
            logger.info("MQTT client disconnected")

if __name__ == "__main__":
    main()