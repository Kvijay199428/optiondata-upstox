# API.py

# CLIENT
user = '7EAHBJ'

# API EXPIRY 06/09/2034         *** apikey is known as CLIENT_ID and apisecret is known for CLIENT_SECRET
apikey_his = '096449e9-13fb-4089-982c-82748d231d1a'
apisecret_his = 'wkq01jsw2c'

apikey_order = '04362a8f-f775-4a9e-a18b-67308f0794fb'
apisecret_order = '02nrln6f4k'

apikey_oc = '747e91fb-6532-49c4-af80-2c8388be77e5'
apisecret_oc = 'ssj2ri0wfp'

apikey_latency = '17bb10d9-3f61-4cd8-8aa4-0b0ee82448f8'
apisecret_latency = '0tx9e6nvop'

# TOTP Secret
totp_secret = 'CMEO6DFNYM54PHF7OKL4PN2DVCIQQEZY'

# MOBILE No
mobile_no = '9599130381'

# PIN 6 digit login PIN
pin = '280894'

# redirect URI
redirect_url = 'https://127.0.0.1:50000/'

# auth uri
auth_uri = 'https://api.upstox.com/index/oauth/token?client_id=your_client_id&redirect_uri=your_redirect_uri&response_type=code&scope=read'

# TPIN - Your TPIN (Telephone PIN) allows you to authenticate yourself over the phone for faster service
TPIN = '2846'

# instrument json
nse = 'https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz'
bse = 'https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz'
mcx = 'https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz'
complete = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz'
suspended = 'https://assets.upstox.com/market-quote/instruments/exchange/suspended-instrument.json.gz'

# database  PostgreSqL
dbname = 'historical'
user = 'admin'
port = '5432'
password = 'admin'
host = 'localhost'

# sample json object

# EQ for equity
#     {
#   "segment": "NSE_EQ",
#   "name": "JOCIL LIMITED",
#   "exchange": "NSE",
#   "isin": "INE839G01010",
#   "instrument_type": "EQ",
#   "instrument_key": "NSE_EQ|INE839G01010",
#   "lot_size": 1,
#   "freeze_quantity": 100000.0,
#   "exchange_token": "16927",
#   "tick_size": 5.0,
#   "trading_symbol": "JOCIL",
#   "short_name": "JOCIL",
#   "security_type": "NORMAL"
# }

# Futures
#     {
#   "weekly": false,
#   "segment": "NSE_FO",
#   "name": "071NSETEST",
#   "exchange": "NSE",
#   "expiry": 2111423399000,
#   "instrument_type": "FUT",
#   "underlying_symbol": "071NSETEST",
#   "instrument_key": "NSE_FO|36702",
#   "lot_size": 50,
#   "freeze_quantity": 100000.0,
#   "exchange_token": "36702",
#   "minimum_lot": 50,
#   "underlying_key": "NSE_EQ|DUMMYSAN011",
#   "tick_size": 5.0,
#   "underlying_type": "EQUITY",
#   "trading_symbol": "071NSETEST FUT 27 NOV 36",
#   "strike_price": 0.0
# }

# # Options
#     {
#   "weekly": false,
#   "segment": "NSE_FO",
#   "name": "VODAFONE IDEA LIMITED",
#   "exchange": "NSE",
#   "expiry": 1706207399000,
#   "instrument_type": "CE",
#   "underlying_symbol": "IDEA",
#   "instrument_key": "NSE_FO|36708",
#   "lot_size": 80000,
#   "freeze_quantity": 1600000.0,
#   "exchange_token": "36708",
#   "minimum_lot": 80000,
#   "underlying_key": "NSE_EQ|INE669E01016",
#   "tick_size": 5.0,
#   "underlying_type": "EQUITY",
#   "trading_symbol": "IDEA 22 CE 25 JAN 24",
#   "strike_price": 22.0
# }

# # Index
#     {
#   "segment": "BSE_INDEX",
#   "name": "AUTO",
#   "exchange": "BSE",
#   "instrument_type": "INDEX",
#   "instrument_key": "BSE_INDEX|AUTO",
#   "exchange_token": "13",
#   "trading_symbol": "AUTO"
# }