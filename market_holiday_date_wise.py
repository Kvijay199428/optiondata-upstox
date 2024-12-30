import requests
from datetime import datetime

# from market_holiday_date_wise import market_holiday_date_wise

# # Call the function
# market_holiday_date_wise()


# Define the function to get market holiday details by date
def market_holiday_date_wise(date_today=None):
    # Use today's date if no date is provided
    if date_today is None:
        date_today = datetime.now().strftime('%Y-%m-%d')
    
    url = f'https://api.upstox.com/v2/market/holidays/{date_today}'
    headers = {'Accept': 'application/json'}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        
        # Print the status
        print(f"Status: {data['status']}\n")
        
        # Check if there are holidays in the response
        if 'data' in data and data['data']:
            print("Holidays:")
            for holiday in data['data']:
                holiday_date = holiday['date']
                description = holiday['description']
                holiday_type = holiday['holiday_type']
                closed_exchanges = ", ".join(holiday.get('closed_exchanges', []))
                open_exchanges = ", ".join([f"{ex['exchange']} (Start: {ex['start_time']}, End: {ex['end_time']})" for ex in holiday.get('open_exchanges', [])])
                
                # Convert the date string to a more readable format
                formatted_date = datetime.strptime(holiday_date, '%Y-%m-%d').strftime('%B %d, %Y')
                
                print(f"Date: {formatted_date}")
                print(f"Description: {description}")
                print(f"Holiday Type: {holiday_type}")
                print(f"Closed Exchanges: {closed_exchanges}")
                print(f"Open Exchanges: {open_exchanges}\n")
        else:
            print("No holiday for today.")
    else:
        print("Failed to retrieve data. Status code:", response.status_code)

# If needed, you can call the function directly
if __name__ == "__main__":
    market_holiday_date_wise()
