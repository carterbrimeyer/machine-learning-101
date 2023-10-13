from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA

load_dotenv()

def get_tomorrows_estimate(data, column):

    df = pd.DataFrame(data)

    # Ensure 'record_date' is in datetime format
    df['record_date'] = pd.to_datetime(df['record_date'])

    # Set 'record_date' as the DataFrame index
    df.set_index('record_date', inplace=True)

    df[column] = pd.to_numeric(df[column], errors='coerce')  # 'coerce' handles non-numeric values

    # Create a continuous date index with daily frequency
    start_date = df.index.min()
    end_date = df.index.max()
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    # Merge your original data with the continuous date index
    df = df.reindex(date_range)

    # Forward fill missing values (replace NaNs with the last available value)
    df[column].ffill(inplace=True)

    # Extract the 'tot_pub_debt_out_amt' column as the time series data
    time_series_data = df[column]

    train_size = int(len(time_series_data) * 0.75)
    train_data, test_data = time_series_data[train_size:], time_series_data[:train_size]

    # Define the order of the ARIMA model (p, d, q)
    p, d, q = 0, 1, 1
    # Create and fit the ARIMA model
    model = ARIMA(train_data, order=(p, d, q))
    model_fit = model.fit()

    # Make predictions
    forecast = model_fit.forecast(steps=1) # Forecast one step ahead (tomorrow)
    # Return the estimated value for 'tot_pub_debt_out_amt'
    return forecast

# Example usage of the function
uri = os.environ.get('CONNECTION_STRING')

client = MongoClient(uri, server_api=ServerApi('1'))
db = client[os.environ.get('DATABASE_NAME')]
collection = db[os.environ.get('ACTUAL_COLLECTION')]

# Define the sort criteria
sort_key = [("record_date", 1)]  # 1 for ascending order, -1 for descending

# Query and sort the documents
result = list(collection.find().sort(sort_key))

debt_held_amt_estimate = get_tomorrows_estimate(result, 'debt_held_public_amt')

intragov_hold_amt_estimate = get_tomorrows_estimate(result, 'intragov_hold_amt')

tot_pub_debt_out_amt_estimate = get_tomorrows_estimate(result, 'tot_pub_debt_out_amt')

tomorrow_date = tot_pub_debt_out_amt_estimate.index[0].strftime("%Y-%m-%d")

# Prepare the JSON data
json_data = {
    "record_date": tomorrow_date,
    "debt_held_public_amt": round(debt_held_amt_estimate.values[0], 2),
    "intragov_hold_amt": round(intragov_hold_amt_estimate.values[0], 2),
    "tot_pub_debt_out_amt": round(tot_pub_debt_out_amt_estimate.values[0], 2),
    }

collection = db[os.environ.get('PREDICTION_COLLECTION')]
#collection.insert_one(json_data)

print(json_data)

client.close()
