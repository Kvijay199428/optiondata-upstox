import json
import upstox_client
from upstox_client.rest import ApiException
import datetime

# Function to load access token from file
def get_access_token():
    # with open('api/token/accessToken_order.txt', 'r') as file:
    with open('api/token/accessToken_oc.txt', 'r') as file:
        access_token = file.read().strip()
    return access_token

# Function to place an order using Upstox API
def place_order(order_type, product_type, price, trigger_price, instrument, validity, is_amo, quantity):
    configuration = upstox_client.Configuration()
    configuration.access_token = get_access_token()

    # API instance
    api_instance = upstox_client.OrderApi(upstox_client.ApiClient(configuration))

    # Create order request body based on order type and parameters
    body = upstox_client.PlaceOrderRequest(
        quantity,  # Quantity input from the user
        product_type,  # "D" for Delivery, "I" for Intraday
        validity,  # e.g., "DAY" or "IOC"
        price,  # price for LIMIT orders (0.0 for MARKET and SL-M orders)
        "string",  # placeholder for order tag
        instrument,  # instrument id, e.g., "NSE_EQ|INE528G01035"
        order_type,  # e.g., "MARKET", "LIMIT", "SL", "SL-M"
        "BUY",  # Order side, "BUY" or "SELL"
        0,  # Disclosed quantity (0 means no disclosure)
        trigger_price,  # Trigger price for stop-loss orders
        is_amo  # Whether this is an after-market order (True/False)
    )
    api_version = '2.0'
    
    # Try placing the order
    try:
        api_response = api_instance.place_order(body, api_version)
        print(api_response)
    except ApiException as e:
        print(f"Exception when calling OrderApi->place_order: {e}\n")

# Load complete instrument data from JSON
def load_instruments():
    try:
        with open("api/instrument/complete.json", "r") as file:
            instruments = json.load(file)
        return instruments
    except FileNotFoundError:
        print("Instrument file not found. Ensure the path is correct.")
        return []

# Function to auto-populate suggestions based on entered trading symbol
def search_instrument(trading_symbol, instruments):
    matches = [instr['trading_symbol'] for instr in instruments if instr['trading_symbol'].lower().startswith(trading_symbol.lower())]
    if matches:
        print(f"Suggestions: {', '.join(matches[:5])}")  # Show up to 5 suggestions
    for instrument in instruments:
        if instrument.get("trading_symbol").lower() == trading_symbol.lower():
            return instrument["instrument_key"]
    return None

# Function to validate and input quantity
def input_quantity():
    while True:
        qty_str = input("Enter Quantity (default is 1): ").lstrip('0') or "1"
        try:
            quantity = int(qty_str)
            if quantity < 1:
                print("Quantity cannot be less than 1. Please enter again.")
                continue
            return quantity
        except ValueError:
            print("Invalid quantity format. Please enter again.")

# Function to select order type
def input_order_type():
    while True:
        order_type_input = input("Select Order Type ('0' for MARKET, '00' for LIMIT, '000' for SL): ")
        if order_type_input == "0":
            return "MARKET"
        elif order_type_input == "00":
            return "LIMIT"
        elif order_type_input == "000":
            return "SL"
        else:
            print("Invalid input. Please enter again.")

# Function to select product type
def input_product_type():
    while True:
        product_type_input = input("Select Product Type ('1' for INTRADAY, '2' for DELIVERY): ")
        if product_type_input == "1":
            return "I"  # INTRADAY
        elif product_type_input == "2":
            return "D"  # DELIVERY
        else:
            print("Invalid input. Please enter again.")

# Function to select order validity
def input_validity():
    while True:
        validity_input = input("Select Validity ('4' for DAY, '5' for IOC): ")
        if validity_input == "4":
            return "DAY"
        elif validity_input == "5":
            return "IOC"
        else:
            print("Invalid input. Please enter again.")

# Function to check AMO based on time (market timings: 9:15 AM - 3:30 PM IST)
def check_amo_time():
    current_time = datetime.datetime.now().time()
    market_start = datetime.time(9, 15)
    market_end = datetime.time(15, 30)
    
    if current_time < market_start or current_time > market_end:
        print("Outside market hours. AMO order will be placed automatically.")
        return True
    else:
        return False

# Function to handle AMO input for market orders
def input_amo(order_type):
    if order_type == "MARKET":
        return check_amo_time()  # Automatically set AMO based on time
    return False

# Main function to execute the order input process
def execute_order():
    instruments = load_instruments()
    
    # Step 1: Search instrument by trading symbol with auto-population
    while True:
        trading_symbol = input("Enter Trading Symbol: ")
        search_instrument(trading_symbol, instruments)
        instrument_key = search_instrument(trading_symbol, instruments)
        if instrument_key:
            print(f"Instrument found: {instrument_key}")
            break
        else:
            print("Instrument not found. Please enter again.")

    # Step 2: Input Quantity
    quantity = input_quantity()
    
    # Step 3: Input Order Type
    order_type = input_order_type()
    
    # Step 4: Input Product Type
    product_type = input_product_type()
    
    # Step 5: Input Validity
    validity = input_validity()
    
    # Step 6: Check or Input AMO (if order type is MARKET)
    is_amo = input_amo(order_type)

    # Step 7: Set price and trigger price based on order type
    if order_type == "LIMIT":
        price = float(input("Enter the Limit Price: "))
        trigger_price = 0.0
    elif order_type == "SL":
        price = float(input("Enter the Stop-Loss Price: "))
        trigger_price = float(input("Enter the Trigger Price: "))
    else:  # For MARKET orders
        price = 0.0
        trigger_price = 0.0

    # Step 8: Place the order using the Upstox API
    place_order(
        order_type=order_type,
        product_type=product_type,
        price=price,
        trigger_price=trigger_price,
        instrument=instrument_key,
        validity=validity,
        is_amo=is_amo,
        quantity=quantity
    )

# Call the main function to run the order placement process
if __name__ == "__main__":
    execute_order()
