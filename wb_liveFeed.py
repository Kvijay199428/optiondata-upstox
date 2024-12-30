# Import necessary modules
import asyncio
import json
import ssl
import upstox_client
import websockets
from google.protobuf.json_format import MessageToDict
from threading import Thread
import MarketDataFeed_pb2 as pb
from time import sleep

# Global variable for storing data
data_dict = {}

# Read access token from file
TOKEN_DIR = 'api/token/accessToken_oc.txt'

def read_access_token(token_dir):
    """Read access token from the given file."""
    with open(token_dir, "r") as file:
        return file.read()

# Fetch access token
access_token = read_access_token(TOKEN_DIR)

def get_market_data_feed_authorize(api_version, configuration):
    """Get authorization for market data feed."""
    try:
        api_instance = upstox_client.WebsocketApi(
            upstox_client.ApiClient(configuration))
        return api_instance.get_market_data_feed_authorize(api_version)
    except Exception as e:
        print(f"Error during authorization: {e}")
        return None

def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response

async def connect_websocket(uri, ssl_context):
    """Establish and return a WebSocket connection."""
    try:
        websocket = await websockets.connect(uri, ssl=ssl_context)
        print('Connection established')
        return websocket
    except Exception as e:
        print(f"Failed to establish connection: {e}")
        return None

async def subscribe_to_instruments(websocket, instruments):
    """Subscribe to market data for given instruments."""
    try:
        # Data to be sent over the WebSocket
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "full",  # Last traded price continuous mode ltpc, full, ohlc
                "instrumentKeys": instruments
            }
        }
        binary_data = json.dumps(data).encode('utf-8')
        await websocket.send(binary_data)
        print(f"Subscribed to instruments: {instruments}")
    except Exception as e:
        print(f"Error while subscribing: {e}")

def extract_relevant_data(feed):
    """Extract and return relevant market data."""
    try:
        feeds = feed.get('feeds', {})
        for instrument_key, details in feeds.items():
            # Extracting relevant fields
            market_data = details.get('ff', {}).get('marketFF', {})
            ltp = market_data.get('ltpc', {}).get('ltp', None)
            ohlc_list = market_data.get('marketOHLC', {}).get('ohlc', [])
            oi = market_data.get('eFeedDetails', {}).get('oi', None)
            change_oi = market_data.get('eFeedDetails', {}).get('changeOi', None)
            
            # Looking for the 'I1' interval data
            interval_data = next((ohlc for ohlc in ohlc_list if ohlc['interval'] == 'I1'), None)

            if interval_data:
                extracted_data = {
                    'ltp': ltp,
                    'interval': 'I1',
                    'open': interval_data['open'],
                    'high': interval_data['high'],
                    'low': interval_data['low'],
                    'close': interval_data['close'],
                    'volume': interval_data['volume'],
                    'oi': oi,
                    'changeOi': change_oi
                }
                return extracted_data

    except Exception as e:
        print(f"Error while extracting data: {e}")
    return {}

async def fetch_market_data():
    """Fetch market data using WebSocket and print it."""
    global data_dict

    # Create default SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Configure OAuth2 access token for authorization
    configuration = upstox_client.Configuration()
    api_version = '2.0'
    configuration.access_token = access_token

    # Get market data feed authorization
    response = get_market_data_feed_authorize(api_version, configuration)
    if not response or not response.data or not response.data.authorized_redirect_uri:
        print("Authorization failed, exiting.")
        return

    # Establish WebSocket connection
    websocket = await connect_websocket(response.data.authorized_redirect_uri, ssl_context)
    if not websocket:
        return

    await asyncio.sleep(1)  # Wait for 1 second before subscribing

    # List of instruments to subscribe to
    instruments = [
        "NSE_INDEX|Nifty 50",
        "NSE_INDEX|Nifty Bank",
        "NSE_INDEX|NIFTY MID SELECT",
        "NSE_INDEX|Nifty Next 50",
        "NSE_INDEX|Nifty Fin Service",
        "BSE_INDEX|SENSEX",
        "BSE_INDEX|BANKEX",
    ]

    # Subscribe to market data for selected instruments
    await subscribe_to_instruments(websocket, instruments)

    # Continuously receive and decode data from WebSocket
    try:
        while True:
            message = await websocket.recv()
            decoded_data = decode_protobuf(message)
            data_dict = MessageToDict(decoded_data)  # Convert to dict
            
            # Extract and print relevant data
            relevant_data = extract_relevant_data(data_dict)
            if relevant_data:
                print(json.dumps(relevant_data, indent=2))
    except Exception as e:
        print(f"Error while receiving data: {e}")
    finally:
        await websocket.close()

def run_websocket():
    """Run WebSocket in a new event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(fetch_market_data())

# Start the WebSocket connection in a separate thread
def start_websocket_thread():
    """Start WebSocket in a separate thread."""
    websocket_thread = Thread(target=run_websocket)
    websocket_thread.daemon = True
    websocket_thread.start()

if __name__ == "__main__":
    # Start WebSocket connection in a background thread
    start_websocket_thread()
    
    # Wait for data and display in a loop
    try:
        while True:
            sleep(1)
            if data_dict:
                print(f"Latest Market Data: {data_dict}")
            else:
                print("Waiting for market data...")
    except KeyboardInterrupt:
        print("Exiting the application.")


# instruments = [
#         # "NSE_INDEX|Nifty 50",
#         # "NSE_INDEX|Nifty Bank",
#         # "NSE_INDEX|NIFTY MID SELECT",
#         # "NSE_INDEX|Nifty Next 50",
#         # "NSE_INDEX|Nifty Fin Service",
#         # "BSE_INDEX|SENSEX",
#         # "BSE_INDEX|BANKEX",
#         "MCX_FO|436980",
#         "MCX_FO|438640",
#         "MCX_FO|432293"
#     ]