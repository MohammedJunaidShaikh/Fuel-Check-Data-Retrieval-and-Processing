FuelCheck Data Retrieval and Processing
📌 Project Overview

This project focuses on retrieving, processing, and visualizing real-time fuel price data from the New South Wales (NSW) Government’s FuelCheck API. The aim is to gain hands-on experience in data engineering workflows, including:

Data retrieval from APIs

Data integration and cleaning

Data publishing via MQTT

Real-time subscription and visualization with a dynamic dashboard

The system is designed to run continuously, simulating an unbounded data stream for real-world data engineering use cases.

🚀 Features and Workflow
1. Data Retrieval

Fetch live fuel pricing data across NSW service stations using the FuelCheck API (v1 endpoints).

Implemented API call optimization to minimize redundant requests.

2. Data Integration & Storage

Consolidates retrieved data into a single dataset.

Cleans and preprocesses the data by:

Handling missing values

Ensuring correct data types

Filtering out irrelevant/inconsistent records

Stores the final dataset into a .csv file for further use.

3. Data Publishing via MQTT

Publishes each fuel price record as an MQTT message.

Introduces a 0.1s delay between each publish to simulate streaming.

4. Data Subscribing & Visualization

A dashboard application subscribes to the MQTT stream.

Dynamic map visualization (built using Streamlit + Folium) displays:

Service station locations with markers

Station brand and default fuel price (selectable via dropdown)

Detailed pop-up with station name, address, updated prices, and timestamp

5. Continuous Execution

Tasks 1–3 (retrieval, integration, publishing) run continuously.

A 60s delay is applied between each new round of API retrieval, in addition to the publishing delay.

📂 Project Structure
ASGN2/
│── data_retrieval.py        # Handles API calls, integration, cleaning, and CSV storage
│── visualization.py         # Dashboard subscribing to MQTT and rendering live map
│── requirements.txt         # Dependencies
│── fuelprice.csv            # Consolidated dataset (output)
│── COMP5339_Assignment2.pdf # Project report

🛠️ Tech Stack

Python

Requests (API calls)

Pandas (data cleaning & integration)

paho-mqtt (MQTT publishing & subscribing)

Streamlit + Folium (dashboard & map visualization)

⚙️ Setup Instructions

Clone this repository:

git clone https://github.com/MohammedJunaidShaikh/Fuel-Check-Data-Retrieval-and-Processing.git
cd Fuel-Check-Data-Retrieval-and-Processing


Create and activate a virtual environment (optional but recommended):

python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows


Install dependencies:

pip install -r requirements.txt


Run the data retrieval & publishing service:

python data_retrieval.py


In another terminal, run the dashboard:

streamlit run visualization.py

📊 Deliverables

Python Programs:

data_retrieval.py (Tasks 1–3)

visualization.py (Task 4)

requirements.txt: All required packages.

Project Report: Summarizes workflow, insights, challenges, and recommendations.
