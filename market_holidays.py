import requests
import json
from datetime import datetime

# # Assume the first script is named 'holidays.py'

# # Import the fetch_holidays function from the first script
# from market_holidays import fetch_holidays, holidays

# # Fetch holidays data
# fetch_holidays()

# # Access the holidays variable
# for holiday in holidays:
#     print(f"Date: {holiday['date']}")
#     print(f"Description: {holiday['description']}")
#     print(f"Holiday Type: {holiday['holiday_type']}\n")

# Global variable to store holidays
holidays = []

def fetch_holidays():
    global holidays  # Declare the global variable
    url = 'https://api.upstox.com/v2/market/holidays'
    headers = {'Accept': 'application/json'}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        
        # Check if there are holidays in the response
        if 'data' in data:
            holidays = []  # Reset the global holidays list
            for holiday in data['data']:
                holiday_date = holiday['date']
                description = holiday['description']
                holiday_type = holiday['holiday_type']
                
                # Convert the date string to a more readable format
                formatted_date = datetime.strptime(holiday_date, '%Y-%m-%d').strftime('%B %d, %Y')
                
                # Append the holiday details to the global list
                holidays.append({
                    'date': formatted_date,
                    'description': description,
                    'holiday_type': holiday_type
                })

                print(f"Date: {formatted_date}")
                print(f"Description: {description}")
                print(f"Holiday Type: {holiday_type}\n")
        else:
            print("No holiday data found.")
    else:
        print("Failed to retrieve data. Status code:", response.status_code)

# Call the function to fetch holidays
fetch_holidays()
