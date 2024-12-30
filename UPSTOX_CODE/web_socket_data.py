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

filename = filename = "api/token/accessToken_OC.txt"

with open(filename,"r") as file:
    access_token = file.read()

def get_market_data_feed_authorize(api_version, configuration):
    """Get authorization for market data feed."""
    api_instance = upstox_client.WebsocketApi(
        upstox_client.ApiClient(configuration))
    api_response = api_instance.get_market_data_feed_authorize(api_version)
    return api_response


def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


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
    response = get_market_data_feed_authorize(
        api_version, configuration)

    # Connect to the WebSocket with SSL context
    async with websockets.connect(response.data.authorized_redirect_uri, ssl=ssl_context) as websocket:
        print('Connection established')

        await asyncio.sleep(1)  # Wait for 1 second

        # # Data to be sent over the WebSocket
        # data = {
        #     "guid": "someguid",
        #     "method": "sub",
        #     "data": {
        #         "mode": "ltpc",
        #         "instrumentKeys": ["NSE_INDEX|Nifty 50", "NSE_INDEX|Nifty Bank", "NSE_INDEX|NIFTY MID SELECT", "NSE_INDEX|Nifty Next 50", "NSE_INDEX|Nifty Fin Service", "BSE_INDEX|SENSEX", "BSE_INDEX|BANKEX"]
        #     }
        # }

        # Data to be sent over the WebSocket
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "full",
                "instrumentKeys": ["MCX_FO|434271", "MCX_FO|438576", "MCX_FO|430106", "MCX_FO|435823", "MCX_FO|436953", "MCX_FO|432293"]
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

            # Print the dictionary representation
            # print(json.dumps(data_dict))


# Execute the function to fetch market data
# asyncio.run(fetch_market_data())
def run_websocket():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(fetch_market_data())

# Start the WebSocket connection in a separate thread
websocket_thread = Thread(target=run_websocket)
websocket_thread.start()
sleep(5)
while True:
    sleep(1)
    print(data_dict)
    # ltp = data_dict['feeds']['NSE_INDEX|Nifty 50']['ff']['indexFF']['ltpc']['ltp']
    # ltp = data_dict['feeds']['NSE_INDEX|Nifty Bank']['ff']['indexFF']['ltpc']['ltp']
    # ltp = data_dict['feeds']['NSE_INDEX|NIFTY MID SELECT']['ff']['indexFF']['ltpc']['ltp']
    # ltp = data_dict['feeds']['NSE_INDEX|Nifty Next 50']['ff']['indexFF']['ltpc']['ltp']
    # ltp = data_dict['feeds']['NSE_INDEX|Nifty Fin Service']['ff']['indexFF']['ltpc']['ltp']
    # ltp = data_dict['feeds']['BSE_INDEX|SENSEX']['ff']['indexFF']['ltpc']['ltp']
    # ltp = data_dict['feeds']['BSE_INDEX|BANKEX']['ff']['indexFF']['ltpc']['ltp']
    # ltp_50 = data_dict['feeds']['NSE_INDEX|Nifty 50']['ff']['indexFF']['ltpc']['ltp']
    # print(f"Last Price {ltp} : {ltp_50}")