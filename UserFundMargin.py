import upstox_client
from upstox_client.rest import ApiException

def read_access_token(file_path: str) -> str:
    """Reads access token from a file."""
    try:
        with open(file_path, 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        raise FileNotFoundError(f"Access token file '{file_path}' not found.")

def configure_client(token: str) -> upstox_client.Configuration:
    """Configures Upstox client with the provided access token."""
    configuration = upstox_client.Configuration()
    configuration.access_token = token
    return configuration

def print_margin_details(margin_data, asset_type: str) -> None:
    """Prints margin details for a given asset type with enhanced formatting."""
    margin_descriptions = {
        'used_margin': 'Positive values denote the amount blocked in an open order or position. Negative values denote the amount being released.',
        'payin_amount': 'Instant payin amount reflected here.',
        'span_margin': 'Amount blocked on futures and options towards SPAN.',
        'adhoc_margin': 'Payin amount credited through a manual process.',
        'notional_cash': 'Amount maintained for withdrawal.',
        'available_margin': 'Total margin available for trading.',
        'exposure_margin': 'Amount blocked on futures and options towards Exposure.'
    }

    print(f"\n{'='*40}\n{asset_type.capitalize()} Margin Details:")
    print(f"{'='*40}")
    for key, description in margin_descriptions.items():
        value = getattr(margin_data, key, 0.0)  # Use getattr to access the attributes safely
        print(f"  {key.replace('_', ' ').capitalize():<20}: {value:>10.2f} - {description}")
    print(f"{'='*40}")

def fetch_user_fund_margin(api_instance, api_version: str) -> None:
    """Fetches and prints the user fund and margin details with improved error handling."""
    try:
        # Get User Fund and Margin
        api_response = api_instance.get_user_fund_margin(api_version)
        print(api_response)  # Print raw response for debugging
        
        # Check the status from the response object
        if api_response.status == 'success':
            # Print margin data for equity and commodity
            if api_response.data:
                if 'equity' in api_response.data:
                    print_margin_details(api_response.data['equity'], "equity")
                else:
                    print("Equity margin data not available.")
                    
                if 'commodity' in api_response.data:
                    print_margin_details(api_response.data['commodity'], "commodity")
                else:
                    print("Commodity margin data not available.")
            else:
                print("No margin data available.")
        else:
            print("Failed to fetch margin details. Status: {api_response.status}")
    except ApiException as e:
        print(f"Exception when calling UserApi->get_user_fund_margin: {e}")

if __name__ == "__main__":
    # Display service downtime information
    print("NOTE: The Funds service is down for maintenance daily from 12:00 AM to 5:30 AM IST.")
    
    # Path to the file containing the access token
    token_file = 'accessToken.txt'
    
    # Read the access token
    access_token = read_access_token(token_file)
    
    # Configure the client
    configuration = configure_client(access_token)
    api_version = '2.0'
    
    # Create an instance of UserApi
    api_instance = upstox_client.UserApi(upstox_client.ApiClient(configuration))

    # Fetch and print the user fund and margin details
    fetch_user_fund_margin(api_instance, api_version)
