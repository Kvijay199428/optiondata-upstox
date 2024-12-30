# Import necessary modules
import asyncio
import json
import ssl
import upstox_client
import websockets
from google.protobuf.json_format import MessageToDict
from upstox_client import feeder
import MarketDataFeed_pb2 as pb

def get_market_data_feed_authorize(api_version, configuration):
    """Get authorization for market data feed."""
    api_instance = upstox_client.WebsocketApi(
        upstox_client.ApiClient(configuration))
    try:
        api_response = api_instance.get_market_data_feed_authorize(api_version)
        return api_response
    except upstox_client.rest.ApiException as e:
        print(f"API Exception: {e}")
        raise

def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response

def read_access_token(file_path):
    """Read access token from a file."""
    try:
        with open(file_path, 'r') as file:
            token = file.read().strip()
        return token
    except IOError as e:
        print(f"Error reading token file: {e}")
        raise

def parse_market_data(data_dict, last_data):
    """Parse and print market data only if there's a change."""
    feeds = data_dict.get('feeds', {})

    for instrument_key, instrument_data in feeds.items():
        # Check if data has changed
        if last_data.get(instrument_key) != instrument_data:
            print(f"\nInstrument: {instrument_key}")
            oc = instrument_data.get('oc', {})
            if oc:
                # Print detailed data
                print(f"Data for instrument {instrument_key}: {json.dumps(oc, indent=2)}")

                # Last Traded Price and Change
                ltpc = oc.get('ltpc', {})
                if ltpc:
                    print(f"  Last Traded Price: {ltpc.get('cp')}")
                    print(f"  Last Traded Time: {ltpc.get('ltt')}")

                # Bid/Ask Quote
                bidAskQuote = oc.get('bidAskQuote', {})
                if bidAskQuote:
                    print(f"  Bid Quantity: {bidAskQuote.get('bidq')}")
                    print(f"  Bid Price: {bidAskQuote.get('bp')}")
                    print(f"  Ask Quantity: {bidAskQuote.get('askq')}")
                    print(f"  Ask Price: {bidAskQuote.get('ap')}")

                # Option Greeks
                optionGreeks = oc.get('optionGreeks', {})
                if optionGreeks:
                    print(f"  Delta: {optionGreeks.get('delta')}")
                    print(f"  Gamma: {optionGreeks.get('gamma')}")
                    print(f"  Vega: {optionGreeks.get('vega')}")
                    print(f"  Theta: {optionGreeks.get('theta')}")
                    print(f"  Rho: {optionGreeks.get('rho')}")
                    print(f"  Implied Volatility: {optionGreeks.get('iv')}")
                    print(f"  Underlier Price: {optionGreeks.get('up')}")

                # EFeed Details
                eFeedDetails = oc.get('eFeedDetails', {})
                if eFeedDetails:
                    print(f"  Current Price: {eFeedDetails.get('cp')}")
                    print(f"  Total Bid Quantity: {eFeedDetails.get('tbq')}")
                    print(f"  Total Ask Quantity: {eFeedDetails.get('tsq')}")
                    print(f"  Lowest Cost: {eFeedDetails.get('lc')}")
                    print(f"  Upper Cost: {eFeedDetails.get('uc')}")

            # Update the last received data
            last_data[instrument_key] = instrument_data
        else:
            print(f"Instrument {instrument_key} data unchanged.")  # Indicate no change

async def fetch_market_data(update_interval_ms=60):
    """Fetch market data using WebSocket and print it."""

    # Create default SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Configure OAuth2 access token for authorization
    configuration = upstox_client.Configuration()

    api_version = '2.0'
    accessToken_dir = 'api/token/accessToken_oc.txt'
    access_token = read_access_token(accessToken_dir)
    configuration.access_token = access_token

    print(f"Using access token: {access_token[:10]}...{accessToken_dir}")  # Print a partial token for debugging

    # Get market data feed authorization
    try:
        response = get_market_data_feed_authorize(api_version, configuration)
    except Exception as e:
        print(f"Failed to authorize: {e}")
        return

    # Connect to the WebSocket with SSL context
    try:
        async with websockets.connect(response.data.authorized_redirect_uri, ssl=ssl_context) as websocket:
            print('Connection established')

            await asyncio.sleep(1)  # Wait for 1 second

            # Data to be sent over the WebSocket
            data = {
                "guid": "someguid",
                "method": "sub",
                "data": {
                    "mode": "full",
                    "instrumentKeys": ["MCX_FO|430106", "MCX_FO|430268"]
                }
            }

            # Convert data to binary and send over WebSocket
            binary_data = json.dumps(data).encode('utf-8')
            await websocket.send(binary_data)

            # Store last received data for comparison
            last_data = {}

            # Continuously receive and decode data from WebSocket
            while True:
                message = await websocket.recv()

                # Print the raw message to verify data reception
                print(f"Raw WebSocket message received: {message}")

                try:
                    decoded_data = decode_protobuf(message)
                    # Convert the decoded data to a dictionary
                    data_dict = MessageToDict(decoded_data)

                    # Parse and print market data (only if changed)
                    parse_market_data(data_dict, last_data)
                except Exception as e:
                    print(f"Error parsing or decoding message: {e}")

                # Sleep for the specified interval (convert milliseconds to seconds)
                await asyncio.sleep(update_interval_ms / 1000)
    except Exception as e:
        print(f"WebSocket connection failed: {e}")

# Execute the function to fetch market data
# Set the update interval to 25 milliseconds (or 60 ms as needed)
asyncio.run(fetch_market_data(update_interval_ms=25))
