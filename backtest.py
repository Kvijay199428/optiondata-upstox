import requests
import psycopg2
from psycopg2 import sql
import logging
from logging.handlers import RotatingFileHandler
from configparser import ConfigParser
import os
from datetime import datetime, time as dt_time 
import time as t
import schedule
import pytz
import pandas as pd  # Import Pandas

# Configure logging with rotating file handler
log_file = "api/logs/optionChain.log"
handler = RotatingFileHandler(log_file, maxBytes=5000000, backupCount=5)  # 5 MB max per log file, keep 5 backups
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

logger.info("Starting real-time data insertion script.")

def get_current_timestamp():
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S") + f".{now.microsecond // 1000:03d}"

# Fetch database config
def configDB(filename="api/ini/OptionChain.ini", section="postgresql"):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        logging.error(f"Section {section} not found in {filename} file.")
        raise Exception(f'Section {section} not found in {filename} file.')
    return db

# Check and create database if not exists
def check_and_create_db(db_config):
    logging.debug("Checking if the database exists.")
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            port=db_config.get('port', 5432),
            database='postgres'
        )
        logging.info("Database connected successfully.")
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [db_config['database']])
        exists = cur.fetchone()
        if not exists:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_config['database'])))
            logging.info(f"Database {db_config['database']} created successfully.")
        else:
            logging.info(f"Database {db_config['database']} already exists.")
        cur.close()
    except Exception as e:
        logging.error(f"Error checking or creating database: {e}")
    finally:
        if conn is not None:
            conn.close()


# Helper function to sanitize table names
def sanitize_table_name(expiry_date, instrument_key):
    # Replace invalid characters like spaces and special symbols with underscores
    sanitized_instrument_key = instrument_key.replace(' ', '_').replace('|', '_').lower()
    sanitized_expiry_date = expiry_date.replace('-', '_')
    return f"{sanitized_instrument_key}_{sanitized_expiry_date}"

# Database insertion function
def insert_data_into_db(db_config, table_name, data):
    logging.debug(f"Inserting data into table {table_name}.")
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Create the table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TIMESTAMP PRIMARY KEY,
            expiry DATE,
            strike_price NUMERIC,
            underlying_spot_price NUMERIC,
            call_ltp NUMERIC,
            call_close_price NUMERIC,
            call_volume INT,
            call_oi INT,
            call_bid_price NUMERIC,
            call_bid_qty INT,
            call_ask_price NUMERIC,
            call_ask_qty INT,
            call_vega NUMERIC,
            call_theta NUMERIC,
            call_gamma NUMERIC,
            call_delta NUMERIC,
            call_iv NUMERIC,
            put_ltp NUMERIC,
            put_close_price NUMERIC,
            put_volume INT,
            put_oi INT,
            put_bid_price NUMERIC,
            put_bid_qty INT,
            put_ask_price NUMERIC,
            put_ask_qty INT,
            put_vega NUMERIC,
            put_theta NUMERIC,
            put_gamma NUMERIC,
            put_delta NUMERIC,
            put_iv NUMERIC,
            pcr NUMERIC,
            underlying_key TEXT
        );
        """
        cur.execute(create_table_query)
        logging.info("Table created or verified successfully.")

        # Insert data
        insert_query = f"""
        INSERT INTO {table_name} (timestamp, expiry, strike_price, underlying_spot_price, call_ltp, call_close_price, call_volume, call_oi,
                                  call_bid_price, call_bid_qty, call_ask_price, call_ask_qty, call_vega, call_theta, call_gamma, call_delta, call_iv, 
                                  put_ltp, put_close_price, put_volume, put_oi, put_bid_price, put_bid_qty, put_ask_price, put_ask_qty, 
                                  put_vega, put_theta, put_gamma, put_delta, put_iv, pcr, underlying_key)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        # Extract values
        values = (
            get_current_timestamp(),
            data.get('expiry'),
            data.get('strike_price'),
            data.get('underlying_spot_price'),
            data['call_options']['market_data'].get('ltp'),
            data['call_options']['market_data'].get('close_price'),
            data['call_options']['market_data'].get('volume'),
            data['call_options']['market_data'].get('oi'),
            data['call_options']['market_data'].get('bid_price'),
            data['call_options']['market_data'].get('bid_qty'),
            data['call_options']['market_data'].get('ask_price'),
            data['call_options']['market_data'].get('ask_qty'),
            data['call_options']['option_greeks'].get('vega'),
            data['call_options']['option_greeks'].get('theta'),
            data['call_options']['option_greeks'].get('gamma'),
            data['call_options']['option_greeks'].get('delta'),
            data['call_options']['option_greeks'].get('iv'),
            data['put_options']['market_data'].get('ltp'),
            data['put_options']['market_data'].get('close_price'),
            data['put_options']['market_data'].get('volume'),
            data['put_options']['market_data'].get('oi'),
            data['put_options']['market_data'].get('bid_price'),
            data['put_options']['market_data'].get('bid_qty'),
            data['put_options']['market_data'].get('ask_price'),
            data['put_options']['market_data'].get('ask_qty'),
            data['put_options']['option_greeks'].get('vega'),
            data['put_options']['option_greeks'].get('theta'),
            data['put_options']['option_greeks'].get('gamma'),
            data['put_options']['option_greeks'].get('delta'),
            data['put_options']['option_greeks'].get('iv'),
            data.get('pcr'),
            data.get('underlying_key')
        )

        # Create indexes
        cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS {} 
            ON {} (underlying_key)
        """).format(
            sql.Identifier(f"idx_{table_name}_underlying_key"),
            sql.Identifier(table_name)
        ))
        cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS {} 
            ON {} (strike_price)
        """).format(
            sql.Identifier(f"idx_{table_name}_strike_price"),
            sql.Identifier(table_name)
        ))
        cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS {} 
            ON {} (expiry)
        """).format(
            sql.Identifier(f"idx_{table_name}_expiry"),
            sql.Identifier(table_name)
        ))

        # Log SQL query and values
        logging.debug(f"SQL Query: {insert_query}")
        logging.debug(f"Values: {values}")

        # Execute insertion
        cur.execute(insert_query, values)

        conn.commit()
        logging.info("Data inserted successfully.")
        cur.close()

    except Exception as error:
        logging.error(f"Error while inserting data into {table_name}: {error}")
    finally:
        if conn is not None:
            conn.close()

# Fetch and process data from API
class OptionChainFetcher:
    def fetch_data(self):
        # Read the access token from file
        try:
            with open('api/token/accessToken_OC.txt', 'r') as file:
                access_token = file.read().strip()
        except FileNotFoundError:
            logging.error("Access token file not found.")
            return {}

        url = 'https://api.upstox.com/v2/option/chain'
        params = {
            'instrument_key': 'BSE_INDEX|SENSEX',
            'expiry_date': '2024-09-13'
        }
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        # Sanitize and generate the table name
        table_name = sanitize_table_name(params['instrument_key'], params['expiry_date'])

        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            logging.debug(f"Request URL: {response.url}")
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Request failed: {e}")
            return {}

    def start_fetching(self):
        logging.info("Starting data fetching...")
        data = self.fetch_data()

        # Check for successful status and presence of 'data' key
        if data.get('status') == 'success' and 'data' in data:
            logging.debug(f"Data fetched successfully: {data}")

            # Loop through the items in the 'data' key directly
            for item in data['data']:
                expiry = item.get('expiry')
                underlying_spot_price = item.get('underlying_spot_price')
                pcr = item.get('pcr')
                underlying_key = item.get('underlying_key')
                strike_price = item.get('strike_price')
                call_options = item.get('call_options')
                put_options = item.get('put_options')

                # Generate a valid table name using the expiry date and instrument key
                table_name = sanitize_table_name(expiry, underlying_key)

                record = {
                    'expiry': expiry,
                    'strike_price': strike_price,
                    'underlying_spot_price': underlying_spot_price,
                    'pcr': pcr,
                    'underlying_key': underlying_key,
                    'call_options': call_options,
                    'put_options': put_options
                }
                # Insert the data into the dynamically generated table
                insert_data_into_db(db_config, table_name, record)

        else:
            logging.error(f"API error or unexpected response format: {data}")

# Schedule job
def is_market_open():
    now = datetime.now(pytz.timezone('Asia/Kolkata')).time()
    return dt_time(0, 15) <= now <= dt_time(23, 30)

def fetch_and_insert_data():
    if is_market_open():
        logging.info("Market is open. Fetching data.")
        fetcher.start_fetching()
    else:
        logging.info("Market is closed. Stopping script.")
        exit()

if __name__ == '__main__':
    # Database setup
    db_config = configDB()
    check_and_create_db(db_config)

    fetcher = OptionChainFetcher()

    # Schedule data fetching during market hours every 3 milliseconds
    schedule.every(0.003).seconds.do(fetch_and_insert_data)

    while True:
        schedule.run_pending()
        t.sleep(1)


# expected response
# {
#   "status": "success",
#   "data": [
    # {
    #   "strike_price": 22400.0,
    #   "underlying_spot_price": 24852.15,
    #   "call_options": {
    #     "instrument_key": "NSE_FO|67667",
    #     "market_data": {
    #       "ltp": 0.0,
    #       "volume": 0,
    #       "oi": 0.0,
    #       "close_price": 1929.7,
    #       "bid_price": 2301.3,
    #       "bid_qty": 1775,
    #       "ask_price": 2617.55,
    #       "ask_qty": 1000,
    #       "prev_oi": 0.0
    #     },
    #     "option_greeks": {
    #       "vega": 0.0,
    #       "theta": 0.0,
    #       "gamma": 0.0,
    #       "delta": 1.0,
    #       "iv": 0.0
    #      }
    #   },
    #     "call_options": {
    #     "instrument_key": "NSE_FO|67667",
    #     "market_data": {
    #       "ltp": 0.0,
    #       "volume": 0,
    #       "oi": 0.0,
    #       "close_price": 1929.7,
    #       "bid_price": 2301.3,
    #       "bid_qty": 1775,
    #       "ask_price": 2617.55,
    #       "ask_qty": 1000,
    #       "prev_oi": 0.0
    #     },
    #     "option_greeks": {
    #       "vega": 0.0,
    #       "theta": 0.0,
    #       "gamma": 0.0,
    #       "delta": 1.0,
    #       "iv": 0.0
    #     }
#         }
#     }
# }

# # json api response structure
# {
#   "status": "success",
#   "data": [
#     {
#       "expiry": "2024-09-19",
#       "strike_price": 22400.0,
#       "underlying_key": "NSE_INDEX|Nifty 50",
#       "underlying_spot_price": 24852.15,
#       "call_options": {
#         "instrument_key": "NSE_FO|67667",
#         "market_data": {
#           "ltp": 0.0,
#           "volume": 0,
#           "oi": 0.0,
#           "close_price": 1929.7,
#           "bid_price": 2301.3,
#           "bid_qty": 1775,
#           "ask_price": 2617.55,
#           "ask_qty": 1000,
#           "prev_oi": 0.0
#         },
#         "option_greeks": {
#           "vega": 0.0,
#           "theta": 0.0,
#           "gamma": 0.0,
#           "delta": 1.0,
#           "iv": 0.0
#         }
#       },
#       "put_options": {
#         "instrument_key": "NSE_FO|67668",
#         "market_data": {
#           "ltp": 6.0,
#           "volume": 157150,
#           "oi": 81975.0,
#           "close_price": 4.05,
#           "bid_price": 5.05,
#           "bid_qty": 900,
#           "ask_price": 9.05,
#           "ask_qty": 1800,
#           "prev_oi": 15475.0
#         },
#         "option_greeks": {
#           "vega": 1.6772,
#           "theta": -1.6576,
#           "gamma": 0.0,
#           "delta": -0.014,
#           "iv": 25.7
#         }
#       }
#     }
#   ]
# }


        # params = {
        #     'instrument_key': 'NSE_INDEX|Nifty Bank',
        #     'expiry_date': '2024-09-19'
        # }

        # params = {
        #     'instrument_key': 'NSE_INDEX|Nifty Fin Service',
        #     'expiry_date': '2024-09-19'
        # }

        # params = {
        #     'instrument_key': 'NSE_INDEX|NIFTY MID SELECT',
        #     'expiry_date': '2024-09-19'
        # }

        # params = {
        #     'instrument_key': 'NSE_INDEX|Nifty Next 50',
        #     'expiry_date': '2024-09-19'
        # }

        # params = {
        #     'instrument_key': 'BSE_INDEX|SENSEX',
        #     'expiry_date': '2024-09-19'
        # }

        # params = {
        #     'instrument_key': 'BSE_INDEX|BANKEX',
        #     'expiry_date': '2024-09-19'
        # }