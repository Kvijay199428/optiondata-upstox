import requests
from datetime import datetime

# Define the list of exchanges to check for
exchange_list = ['NSE', 'BSE', 'MCX']

# Get today's date in "YYYY-MM-DD" format
date_today = datetime.today().strftime('%Y-%m-%d')

# Insert the date in the URL
url = f'https://api.upstox.com/v2/market/timings/{date_today}'
headers = {'Accept': 'application/json'}

# Make the GET request
response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()

    # Debugging: Print the raw data received from the API
    print("Raw data received from API:", data)

    # Check if the response status is successful
    if data['status'] == 'success':
        # Check if 'data' exists in the response
        if 'data' in data:
            # Extract the exchange timings from the response
            for timing in data['data']:
                exchange = timing.get('exchange')

                # Debugging: Print each exchange from the response
                print(f"Exchange found in response: {exchange}")

                # Only process data for exchanges in the exchange_list
                if exchange in exchange_list:
                    # Convert Unix timestamps to readable format
                    start_time = datetime.utcfromtimestamp(timing['start_time'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    end_time = datetime.utcfromtimestamp(timing['end_time'] / 1000).strftime('%Y-%m-%d %H:%M:%S')

                    # Print the formatted data
                    print(f"Exchange: {exchange}")
                    print(f"Start Time: {start_time}")
                    print(f"End Time: {end_time}")
                else:
                    print(f"Exchange '{exchange}' not in the list. Skipping.")
        else:
            print("No 'data' field in the response.")
    else:
        print("Failed to retrieve valid data from API.")
else:
    print("Failed to retrieve data. Status code:", response.status_code)
