# Import necessary modules
import asyncio
import json
import ssl
import upstox_client
from datetime import datetime, timedelta
import websockets
from google.protobuf.json_format import MessageToDict
from threading import Thread
import MarketDataFeed_pb2 as pb
from time import sleep

filename = "api/token/accessToken_oc.txt"
with open(filename, "r") as file:
    access_token = file.read()

def get_market_data_feed_authorize(api_version, configuration):
    """Get authorization for market data feed."""
    api_instance = upstox_client.WebsocketApi(upstox_client.ApiClient(configuration))
    api_response = api_instance.get_market_data_feed_authorize(api_version)
    return api_response

def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response

# Convert "ts" timestamp to readable date
def convert_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

def process_response_data(data_dict):
    """Process and print the response data in a readable format."""
    if 'feeds' in data_dict and 'MCX_FO|437034' in data_dict['feeds']:
        feed_data = data_dict['feeds']['MCX_FO|437034']['ff']['marketFF']

        # Extract necessary fields
        last_traded_price = feed_data['ltpc']['ltp']
        last_traded_time = feed_data['ltpc']['ltt']
        last_traded_quantity = feed_data['ltpc']['ltq']
        close_price = feed_data['ltpc']['cp']

        # Bid/Ask quotes
        bid_ask_quotes = feed_data['marketLevel']['bidAskQuote']

        # Option Greeks
        option_greeks = feed_data['optionGreeks']
        implied_volatility = option_greeks['iv']
        delta = option_greeks['delta']
        theta = option_greeks['theta']
        gamma = option_greeks['gamma']
        vega = option_greeks['vega']
        rho = option_greeks['rho']

        # OHLC (Open-High-Low-Close) data
        ohlc_data = feed_data['marketOHLC']['ohlc']

        # Print out the details
        print(f"Last Traded Price: {last_traded_price}")
        print(f"Last Traded Time: {last_traded_time}")
        print(f"Last Traded Quantity: {last_traded_quantity}")
        print(f"Close Price: {close_price}\n")

        print("Bid/Ask Quotes:")
        for i, quote in enumerate(bid_ask_quotes, 1):
            print(f"Level {i}: Bid Quantity: {quote['bq']}, Bid Price: {quote['bp']}, Ask Quantity: {quote['aq']}, Ask Price: {quote['ap']}")

        print("\nOption Greeks:")
        print(f"Implied Volatility: {implied_volatility}")
        print(f"Delta: {delta}, Theta: {theta}, Gamma: {gamma}, Vega: {vega}, Rho: {rho}")

        print("\nOHLC Data:")
        for ohlc in ohlc_data:
            readable_timestamp = convert_timestamp(int(ohlc['ts']))  # Convert timestamp
            print(f"Interval: {ohlc['interval']}, Open: {ohlc['open']}, High: {ohlc['high']}, Low: {ohlc['low']}, Close: {ohlc['close']}, Volume: {ohlc.get('volume', 'N/A')}, Timestamp: {readable_timestamp}")

async def fetch_market_data():
    global data_dict
    """Fetch market data using WebSocket and print it."""

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

    # Connect to the WebSocket with SSL context
    async with websockets.connect(response.data.authorized_redirect_uri, ssl=ssl_context) as websocket:
        print('Connection established')

        await asyncio.sleep(0.003)  # Wait for 1 second

        # Data to be sent over the WebSocket
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "full",
                "instrumentKeys": ["MCX_FO|437034"]
            }
        }

        # Convert data to binary and send over WebSocket
        binary_data = json.dumps(data).encode('utf-8')
        await websocket.send(binary_data)

        # Continuously receive and decode data from WebSocket
        while True:
            message = await websocket.recv()
            decoded_data = decode_protobuf(message)

            # Convert the decoded data to a dictionary
            data_dict = MessageToDict(decoded_data)

            # Process the data and print it
            process_response_data(data_dict)

def run_websocket():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(fetch_market_data())

# Start the WebSocket connection in a separate thread
websocket_thread = Thread(target=run_websocket)
websocket_thread.start()

sleep(0)  # Give some time to receive initial data
while True:
    sleep(1)
    # print(data_dict)  # Print the data in raw format or remove this line if not needed


# response geting from websocket

# {
#   "type": "live_feed",
#   "feeds": {
#     "MCX_FO|437034": {
#       "ff": {
#         "marketFF": {
#           "ltpc": {
#             "ltp": 304.0,  // Last Traded Price
#             "ltt": "1726065522000",  // Last Traded Time
#             "ltq": "100",  // Last Traded Quantity
#             "cp": 264.9  // Closing Price
#           },
#           "marketLevel": {
#             "bidAskQuote": [
#               { "bq": 2, "bp": 295.1, "bno": 0, "aq": 2, "ap": 306.6 },
#               { "bq": 1, "bp": 292.0, "bno": 0, "aq": 2, "ap": 306.7 },
#               { "bq": 1, "bp": 287.8, "bno": 0, "aq": 1, "ap": 309.8 },
#               { "bq": 1, "bp": 286.8, "bno": 0, "aq": 1, "ap": 319.8 },
#               { "bq": 2, "bp": 283.8, "bno": 0, "aq": 0, "ap": 0.0 }
#             ]
#           },
#           "optionGreeks": {
#             "op": 304.0,  // Option Price
#             "up": 5530.0,  // Underlying Price
#             "iv": 0.3831,  // Implied Volatility
#             "delta": -0.5176,  // Delta
#             "theta": -3.6758,  // Theta
#             "gamma": 0.0006,  // Gamma
#             "vega": 6.9363,  // Vega
#             "rho": -3.1358  // Rho
#           },
#           "marketOHLC": {
#             "ohlc": [
#               { "interval": "1d", "open": 241.7, "high": 305.9, "low": 233.0, "close": 304.0, "volume": 8, "ts": "1725993000000" },
#               { "interval": "I1", "open": 305.9, "high": 305.9, "low": 305.9, "close": 305.9, "ts": "1726065420000" },
#               { "interval": "I1", "open": 304.0, "high": 304.0, "low": 304.0, "close": 304.0, "volume": 1, "ts": "1726065480000" },
#               { "interval": "I30", "open": 268.1, "high": 268.1, "low": 268.1, "close": 268.1, "volume": 1, "ts": "1726063200000" },
#               { "interval": "I30", "open": 288.7, "high": 305.9, "low": 233.0, "close": 304.0, "volume": 3, "ts": "1726065000000" }
#             ]
#           },
#           "eFeedDetails": {
#             "atp": 264.2,  // Average Traded Price
#             "cp": 264.9,  // Closing Price
#             "vtt": "8",  // Total Traded Volume
#             "oi": 4.0,  // Open Interest
#             "changeOi": 0.0,  // Change in Open Interest
#             "tbq": 9.0,  // Total Buy Quantity
#             "tsq": 6.0,  // Total Sell Quantity
#             "close": 0.0,  // Close Price
#             "lc": 23.9,  // Lower Circuit Limit
#             "uc": 836.1,  // Upper Circuit Limit
#             "yh": 305.9,  // Yesterday's High
#             "yl": 233.0,  // Yesterday's Low
#             "fp": 0.0,  // Futures Price
#             "fv": 0,  // Futures Volume
#             "mbpBuy": "0",  // Market Buy Price
#             "mbpSell": "0",  // Market Sell Price
#             "tv": "0",  // Total Volume
#             "sp": 5521.0,  // Strike Price
#             "poi": 0.0  // Previous Open Interest
#           }
#         }
#       }
#     }
#   }
# }
