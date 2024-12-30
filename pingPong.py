import os
import sys
import time
import upstox_client
from upstox_client.rest import ApiException

# Function to assign market data to variables
def assign_market_data(api_response):
    market_data = api_response.data

    # Define the last prices for the required symbols as global variables
    global nifty50ltp, niftynxt50ltp, niftyfinserviceltp, niftymidselectltp, niftybankltp, sensexltp, bankexltp

    nifty50ltp = getattr(market_data.get('NSE_INDEX:Nifty 50', {}), 'last_price', 'N/A')
    niftynxt50ltp = getattr(market_data.get('NSE_INDEX:Nifty Next 50', {}), 'last_price', 'N/A')
    niftyfinserviceltp = getattr(market_data.get('NSE_INDEX:Nifty Fin Service', {}), 'last_price', 'N/A')
    niftymidselectltp = getattr(market_data.get('NSE_INDEX:NIFTY MID SELECT', {}), 'last_price', 'N/A')
    niftybankltp = getattr(market_data.get('NSE_INDEX:Nifty Bank', {}), 'last_price', 'N/A')
    sensexltp = getattr(market_data.get('BSE_INDEX:SENSEX', {}), 'last_price', 'N/A')
    bankexltp = getattr(market_data.get('BSE_INDEX:BANKEX', {}), 'last_price', 'N/A')

    # Print the assigned values for confirmation
    print(f"Nifty 50 LTP: {nifty50ltp}")
    print(f"Nifty Next 50 LTP: {niftynxt50ltp}")
    print(f"Nifty Fin Service LTP: {niftyfinserviceltp}")
    print(f"Nifty Mid Select LTP: {niftymidselectltp}")
    print(f"Nifty Bank LTP: {niftybankltp}")
    print(f"Sensex LTP: {sensexltp}")
    print(f"Bankex LTP: {bankexltp}")

# Function to ensure paths are correctly located in executable
def resource_path(relative_path):
    """Get the absolute path to the resource, works for dev and for PyInstaller."""
    try:
        # PyInstaller creates a temp folder and stores the path in _MEIPASS
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)

# Read the access token from the file
with open("api/token/accessToken_OC.txt", "r") as file:
    access_token = file.read().strip()

# Set up Upstox client configuration
configuration = upstox_client.Configuration()
configuration.access_token = access_token
api_version = '2.0'

# Define the symbols to fetch LTP (Last Traded Price)
symbol = 'NSE_INDEX|Nifty 50,NSE_INDEX|Nifty Bank,NSE_INDEX|Nifty Fin Service,NSE_INDEX|NIFTY MID SELECT,NSE_INDEX|Nifty Next 50,BSE_INDEX|SENSEX,BSE_INDEX|BANKEX'

# Create an instance of the MarketQuoteApi
api_instance = upstox_client.MarketQuoteApi(upstox_client.ApiClient(configuration))

# Function to fetch market data and update every second
def fetch_market_data():
    while True:
        start_time = time.time()
        
        try:
            # Fetch the latest market data
            api_response = api_instance.ltp(symbol, api_version)
            
            # Assign and print the last prices
            assign_market_data(api_response)
        
        except ApiException as e:
            print(f"Exception when calling MarketQuoteApi->ltp: {e}\n")
        
        # Calculate how long the API call and processing took
        elapsed_time = time.time() - start_time
        
        # If the operation took less than 1 second, wait for the remaining time
        if elapsed_time < 0.5:
            time.sleep(0.5 - elapsed_time)
        else:
            print(f"Warning: Fetching and processing took longer than 1 second ({elapsed_time:.2f}s)")

# Start the automatic fetching of market data
fetch_market_data()