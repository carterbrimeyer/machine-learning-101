import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv()

# Format the date as a string in "YYYY-MM-DD" format
yesterday_date = datetime.now() - timedelta(days=2)
formatted_date = yesterday_date.strftime("%Y-%m-%d")

url = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny?filter=record_date:eq:' + formatted_date

try:
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON data from the response
        json_data = response.json()["data"][0]
    else:
        # If the request was not successful, print an error message
        print(f"Request failed with status code {response.status_code}")
except requests.exceptions.RequestException as e:
    print(f"Request error: {e}")

uri = os.environ.get('CONNECTION_STRING')

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
db = client[os.environ.get('DATABASE_NAME')]
collection = db[os.environ.get('ACTUAL_COLLECTION')]]

print(json_data)
collection.insert_one(json_data)

print("COMPLETED")

client.close()
