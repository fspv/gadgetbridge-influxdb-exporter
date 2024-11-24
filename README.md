# Automated Data Export & Visualization for Xiaomi Mi Band Users

This script allows you to seamlessly download and process your Xiaomi Mi Band data from [Gadget Bridge](https://gadgetbridge.org/). It retrieves your exported files from Google Drive, parses the data, and stores it in an InfluxDB database, enabling you to easily visualize and analyze your data with tools like Grafana.

**Why Use This Script?**

- **Preserve Your Data**: By default, Gadget Bridge can lose data when you update your phone, as it may rotate older records. This script ensures that all of your tracking data is safely stored in InfluxDB, preventing any loss.
  
- **Unlock Full Data Control**: Once the data is in InfluxDB, you have the freedom to query, manipulate, and visualize it however you want—without being limited by the Gadget Bridge app's interface. Customize your dashboards, generate detailed reports, or integrate with other services.

Whether you're tracking your fitness progress or exploring new ways to interact with your data, this script opens up new possibilities for Xiaomi Mi Band users looking for deeper insights.

## Build
```sh
virtualenv .venv
source .venv/bin/activate
pip install poetry
poetry install
```

## Run

Create a service account via Google Cloud Console and download the credentials.json file.

Set the following environment variables to run the script:

```python
GOOGLE_DRIVE_FILE_ID="your_file_id_here"
SERVICE_ACCOUNT_PATH="/service_account.json"
OTLP_ENDPOINT="http://otel-collector:4317"
INFLUXDB_URL="http://influxdb:8086"
INFLUXDB_ORG=default_org
INFLUXDB_BUCKET=default_bucket
INFLUXDB_TOKEN=default_token
DAEMON=true
```

Run the script

```sh
python main.py
```
