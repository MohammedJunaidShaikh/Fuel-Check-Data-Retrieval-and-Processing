# FuelCheck Data Retrieval and Processing  

## 📌 Project Overview  
This project focuses on retrieving, processing, and visualizing real-time fuel price data from the New South Wales (NSW) Government’s FuelCheck API.  
It provides hands-on experience in data engineering workflows, including:  
- Data retrieval from APIs  
- Data integration and cleaning  
- Data publishing via MQTT  
- Real-time subscription and visualization with a dynamic dashboard  

The system runs continuously, simulating an unbounded data stream for real-world data engineering use cases.  

---

## 🚀 Features and Workflow  

### 1. Data Retrieval  
- Fetches live fuel pricing data across NSW service stations using the FuelCheck API (v1 endpoints).  
- Optimized API calls to minimize redundant requests.  

### 2. Data Integration & Storage  
- Consolidates retrieved data into a single dataset.  
- Cleans and preprocesses data:  
  - Handles missing values  
  - Ensures correct data types  
  - Filters out irrelevant/inconsistent records  
- Stores the consolidated dataset into a `.csv` file.  

### 3. Data Publishing via MQTT  
- Publishes each fuel price record as an MQTT message.  
- Adds a `0.1s` delay between each publish to simulate streaming.  

### 4. Data Subscribing & Visualization  
- Dashboard application subscribes to MQTT messages.  
- Dynamic map visualization (Streamlit + Folium) shows:  
  - Station locations with markers  
  - Station brand and default fuel price (selectable via dropdown)  
  - Pop-up with station name, address, prices, and update timestamp  

### 5. Continuous Execution  
- Tasks 1–3 (retrieval, integration, publishing) run continuously.  
- A `60s` delay is applied between each API retrieval cycle.  

---

## 📂 Project Structure  


---

## 🛠️ Tech Stack  

- **Python**  
- **Requests** – API calls  
- **Pandas** – data cleaning & integration  
- **paho-mqtt** – MQTT publishing & subscribing  
- **Streamlit** + **Folium** – dashboard & map visualization  

---

## ⚙️ Setup Instructions  

1. Clone the repository:  
   ```bash
   git clone https://github.com/MohammedJunaidShaikh/Fuel-Check-Data-Retrieval-and-Processing.git
   cd Fuel-Check-Data-Retrieval-and-Processing

  name: fuelcheck-env
channels:
  - defaults
  - conda-forge

dependencies:
  - python=3.10
  - pip
  - pip:
      - requests
      - pandas
      - paho-mqtt
      - streamlit
      - folium

## Snapshots of Project
<img width="1088" height="718" alt="image" src="https://github.com/user-attachments/assets/a3820103-88b1-44e7-9091-4c65caa30128" />
<img width="990" height="783" alt="image" src="https://github.com/user-attachments/assets/b62fd372-b9f9-4cc6-899b-0a4c557b4a41" />

