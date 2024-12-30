# Import necessary modules
import asyncio
import json
import ssl,sys
import upstox_client
import websockets
from google.protobuf.json_format import MessageToDict
from threading import Thread
import MarketDataFeed_pb2 as pb
from time import sleep
import requests as rq
import pandas as pd
filename =f"accessToken.txt"
with open(filename,"r") as file:
    access_token = file.read()

df = pd.read_csv("https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz")

def filter_df(df,lot_size):
    df = df[(df['exchange'] == 'NSE_FO') &(df['instrument_type'] == 'OPTIDX') &(df['lot_size'] == lot_size)]
    df = df[df['expiry'] ==  min(df['expiry'].unique())]
    return df

def find_option(near_val,optionChain):
    call_symbol = {}
    put_symbol = {}
    trade_symbol = {}
    for i in range(5):
        try:
            ltp_data = get_quotes(optionChain)['data']
        except:
            sleep(0.5)
            ltp_data = get_quotes(optionChain)['data']
            pass
        for k, v in ltp_data.items():
            # Call Symbol
            if float(v['last_price']) <= near_val and k[-2:] == 'CE':
                call_symbol.update({v['instrument_token']: float(v['last_price'])})
            # Put Symbol
            if float(v['last_price']) <= near_val and k[-2:] == 'PE':
                put_symbol.update({v['instrument_token']: float(v['last_price'])})
        if call_symbol and put_symbol:
            ce_val = min(list(call_symbol.values()), key=lambda x: abs(x-near_val))
            pe_val = min(list(put_symbol.values()), key=lambda x: abs(x-near_val))
            for a, b in call_symbol.items():
                if b == ce_val:
                    trade_symbol.update({a: b})
            for c, d in put_symbol.items():
                if d == pe_val:
                    trade_symbol.update({c: d})
        if trade_symbol:
            return trade_symbol 
        else:
            sleep(1)
    return 'Symbol Not found'

def get_quotes(instrument):
    ltp_data = {}
    url = 'https://api-v2.upstox.com/market-quote/ltp'
    headers = {
        'accept': 'application/json',
        'Api-Version': '2.0','Authorization': f'Bearer {access_token}'}
    params = {'symbol': instrument,'interval':'1d'}
    res = rq.get(url, headers=headers, params=params).json()
    return res

def palce_order(symbol,qty,direction):
    url = "https://api.upstox.com/v2/order/place"
    headers = {
    'accept': 'application/json',
    'Api-Version': '2.0',
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {access_token}'}
    payload={
      "quantity": qty,
      "product": "D",
      "validity": "DAY",
      "price": 0,
      "tag": "string",
      "instrument_token": symbol,
      "order_type": "MARKET",
      "transaction_type": direction,
      "disclosed_quantity": 0,
      "trigger_price": 0,
      "is_amo": False}
    data = json.dumps(payload)
    response = rq.post(url, headers=headers, data=data).json()
    return response['data']['order_id']

def get_order_history(oid):
    url = "https://api.upstox.com/v2/order/details"
    payload={'order_id':oid}
    headers = {
        'accept': 'application/json',
        'Api-Version': '2.0',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'}
    response = rq.get( url, headers=headers,params=payload)
    return response.json()['data']

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

        # Data to be sent over the WebSocket
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "ltpc",
                "instrumentKeys": symbol
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

def run_websocket():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(fetch_market_data())


# Input Parameters *****
buy_percent = 10
qty = 15
premium_range = 200
sl = 0
tsl = 2
# **************************


BNDF = filter_df(df,15)
optionChain = list(BNDF['instrument_key'])
trade_symbol = find_option(premium_range,optionChain)
symbol = list(trade_symbol)

websocket_thread = Thread(target=run_websocket)
websocket_thread.start()
sleep(5)


trade = None
while True:
    sleep(1)
    for name in symbol:
        ltp = data_dict['feeds'][name]['ltpc']['ltp']
        print(name,ltp)

        if ltp >= (trade_symbol[name]*buy_percent)/100 and trade == None:
            oid=palce_order(name,qty,'BUY')
            orderHistory = get_order_history(oid)
            if orderHistory['status'] == 'complete':
                avgPrc = orderHistory['average_price']
                sl = avgPrc-sl
                tsl = avgPrc+tsl
                option = name
                trade = 1
                print(f"Buy Trade In {name} Price : {avgPrc}")
            if orderHistory['status'] != 'complete':
                print("Something went wrong")
                sys.exit()


        if trade == 1 and ltp >= tsl and option == name:
            tsl+=2
            sl+=2
            print(f"Buy sl trailed {option} Sl :{sl}")

        if trade == 1 and ltp <= sl and option == name:
            oid=palce_order(option,qty,'SELL')
            print(f"Buy {option} Exit {ltp}")
            sys.exit()


