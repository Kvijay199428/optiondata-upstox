import sys
import requests
import psycopg2
from psycopg2 import sql
import logging
from logging.handlers import RotatingFileHandler
from configparser import ConfigParser
import os
from datetime import datetime, timedelta, time as dt_time 
import time as t
import schedule
import pytz
import holidays
from holiday.market_holiday_date_wise import market_holiday_date_wise
import calendar
from pathlib import Path
# Call the function
market_holiday_date_wise()

# Function to ensure paths are correctly located in executable
def resource_path(relative_path):
    """Get the absolute path to the resource, works for dev and for PyInstaller."""
    try:
        # PyInstaller creates a temp folder and stores the path in _MEIPASS
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)

# Ensure the directory exists
log_directory = "api/logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configure logging with rotating file handler
log_file = "api/logs/BankNifty.log"
handler = RotatingFileHandler(log_file, maxBytes=5000000, backupCount=5)  # 5 MB max per log file, keep 5 backups
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

logger.info("Starting real-time data insertion script.")

# # Function to get the last wednesday of a given month
# def get_last_wednesday(year, month):
#     last_day = datetime(year, month, 1) + timedelta(days=31)
#     last_day -= timedelta(days=last_day.day)
#     last_wednesday = last_day - timedelta(days=(last_day.weekday() - 2) % 7)

#     india_holidays = holidays.India(years=year)
#     if last_wednesday.weekday() > 4 or last_wednesday in india_holidays:
#         logging.info(f"{last_wednesday.date()} is a weekend or holiday.")
#         last_wednesday -= timedelta(days=1)

#     logging.info(f"Last wednesday expiry date is {last_wednesday.date()}.")
#     return last_wednesday.date()

# def get_next_wednesday(year, month, today=None):
#     if today is None:
#         today = datetime.today()

#     # Call the market_holiday_date_wise function and process its response
#     holiday_response = market_holiday_date_wise()

#     holidays_list = []
    
#     # Ensure holiday_response is not None before accessing its contents
#     if holiday_response is None:
#         logging.error("Error: Holiday response is None. Skipping holiday check.")
#     elif holiday_response.get('status') == 'success' and holiday_response.get('data'):
#         # If there are holidays, extract them
#         holidays_list = [(holiday['date'], holiday['description']) for holiday in holiday_response['data']]
#         for holiday in holidays_list:
#             logging.info(f"Holiday on {holiday[0]}: {holiday[1]}")
#     else:
#         logging.warning("Holiday response status is not 'success' or no data returned.")

#     # Check if today is a weekend
#     if today.weekday() == 5:  # Saturday
#         logging.info("Today is a weekend holiday (Saturday).")
#     elif today.weekday() == 6:  # Sunday
#         logging.info("Today is a weekend holiday (Sunday).")
#     else:  # Weekday (Monday to Friday)
#         # Check if today is a holiday
#         if today.date().strftime('%Y-%m-%d') in [h[0] for h in holidays_list]:
#             logging.info(f"{today.date()} is a holiday. Adjusting the date.")
#             today += timedelta(days=1)  # Move to the next day if today is a holiday

#     # Calculate the days until the next Wednesday
#     days_ahead = (2 - today.weekday() + 7) % 7  # 2 represents Wednesday
#     if days_ahead == 0:  # If today is Wednesday, move to next week
#         days_ahead = 7
    
#     next_wednesday = today + timedelta(days=days_ahead)

#     india_holidays = holidays.India(years=year)
    
#     # If the next Wednesday is a holiday or a weekend, get the previous trading day
#     while next_wednesday.weekday() > 4 or next_wednesday in india_holidays:
#         logging.info(f"{next_wednesday.date()} is a weekend or holiday. Finding the previous trading day.")
#         next_wednesday -= timedelta(days=1)

#     next_wednesday_date = next_wednesday.date()  # Extract the final date
#     logging.info(f"Next Wednesday expiry date is {next_wednesday_date}.")
    
#     return next_wednesday_date
def get_last_wednesday_of_month(year, month, today=None):
    if today is None:
        today = datetime.today()

    # Create a datetime for the last day of the specified month
    last_day = datetime(year, month, calendar.monthrange(year, month)[1])
    
    # Find the last Wednesday working backwards from the last day of the month
    while last_day.weekday() != 2:  # 2 represents Wednesday
        last_day -= timedelta(days=1)
    
    # Call the market_holiday_date_wise function and process its response
    holiday_response = market_holiday_date_wise()

    holidays_list = []
    
    # Ensure holiday_response is not None before accessing its contents
    if holiday_response is None:
        logging.error("Error: Holiday response is None. Skipping holiday check.")
    elif holiday_response.get('status') == 'success' and holiday_response.get('data'):
        # If there are holidays, extract them
        holidays_list = [(holiday['date'], holiday['description']) for holiday in holiday_response['data']]
        for holiday in holidays_list:
            logging.info(f"Holiday on {holiday[0]}: {holiday[1]}")
    else:
        logging.warning("Holiday response status is not 'success' or no data returned.")

    india_holidays = holidays.India(years=year)
    
    # If the last Wednesday is a holiday or a weekend, get the previous trading day
    while last_day.weekday() > 4 or last_day in india_holidays or last_day.date().strftime('%Y-%m-%d') in [h[0] for h in holidays_list]:
        logging.info(f"{last_day.date()} is a weekend or holiday. Finding the previous trading day.")
        last_day -= timedelta(days=1)

    last_wednesday_date = last_day.date()  # Extract the final date
    logging.info(f"Last Wednesday of the month is {last_wednesday_date}.")
    
    return last_wednesday_date

def get_current_timestamp():
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S") + f".{now.microsecond // 1000:03d}"

# Fetch database config
def configDB(filename="api/ini/optiondata.ini", section="postgresql"):
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
    logging.debug(f"Expiry Date is {expiry_date}.")
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
    # Get the current file's directory and construct relative path
        current_dir = Path(__file__).parent
        token_path = current_dir / 'api' / 'token' / 'accessToken_oc.txt'
    
        try:
            with open(token_path, 'r') as file:
                access_token = file.read().strip()
        except FileNotFoundError:
            logging.error("Access token file not found.")
            return {}

        # Get the current year and month
        now = datetime.now()
        # expiry_date = get_last_wednesday(now.year, now.month)
        # expiry_date = get_next_wednesday(now.year, now.month)
        expiry_date = get_last_wednesday_of_month(now.year, now.month)

        # Prepare the API request parameters
        params = {
            'instrument_key': 'NSE_INDEX|Nifty Bank',
            'expiry_date': expiry_date  # Use the dynamically generated expiry date
        }
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        # Sanitize and generate the table name
        table_name = sanitize_table_name(str(expiry_date), params['instrument_key'])

        try:
            response = requests.get('https://api.upstox.com/v2/option/chain', params=params, headers=headers)
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
    return dt_time(9, 14) <= now <= dt_time(15, 30)

def fetch_and_insert_data():
    if is_market_open():
        logging.info("Market is open. Fetching data.")
        fetcher.start_fetching()
    else:
        logging.info("Market is closed. Stopping script.")
        exit()

def convert_milliseconds_to_time(milliseconds):
    """Convert milliseconds timestamp to datetime object in IST."""
    try:
        seconds = milliseconds / 1000
        dt = datetime.fromtimestamp(seconds, pytz.timezone('Asia/Kolkata'))
        logging.info(f"Converted milliseconds {milliseconds} to datetime: {dt}")
        return dt
    except Exception as e:
        logging.error(f"Error converting milliseconds {milliseconds}: {e}")
        return None

def parse_exchange_timings(holiday_data):
    """Parse exchange timings from holiday data."""
    try:
        for exchange_info in holiday_data.get('open_exchanges', []):
            if isinstance(exchange_info, str):
                # Parse the string format "NSE (Start: 1735703100000, End: 1735725600000)"
                parts = exchange_info.split('(')
                exchange_name = parts[0].strip()
                if exchange_name == 'NSE':
                    timing_parts = parts[1].strip(')').split(',')
                    start_ms = int(timing_parts[0].split(':')[1].strip())
                    end_ms = int(timing_parts[1].split(':')[1].strip())
                    return {
                        'name': 'NSE',
                        'start': start_ms,
                        'end': end_ms
                    }
    except Exception as e:
        logging.error(f"Error parsing exchange timings: {e}")
    return None

def is_market_open():
    """
    Check if the market is open based on regular hours and holiday special timings.
    Returns: bool
    """
    # Get current time in IST
    now = datetime.now(pytz.timezone('Asia/Kolkata'))
    current_date = now.strftime('%Y-%m-%d')
    
    logging.info(f"Checking market status for date: {current_date}, time: {now.strftime('%H:%M:%S')}")
    
    # Check holiday data
    holiday_response = market_holiday_date_wise()
    
    if holiday_response and holiday_response.get('status') == 'success':
        holidays = holiday_response.get('data', [])
        
        for holiday in holidays:
            logging.info(f"Checking holiday: {holiday}")
            
            if holiday['date'] == current_date:
                logging.info("Today is a holiday with special timing")
                
                # Parse exchange timings
                exchange_info = parse_exchange_timings(holiday)
                
                if exchange_info:
                    start_time = convert_milliseconds_to_time(exchange_info['start'])
                    end_time = convert_milliseconds_to_time(exchange_info['end'])
                    
                    if start_time and end_time:
                        is_open = start_time <= now <= end_time
                        logging.info(f"Special timing check - Start: {start_time}, End: {end_time}, Current: {now}, Is Open: {is_open}")
                        return is_open
    
    # Regular market hours check
    regular_market_time = now.time()
    is_regular_open = dt_time(9, 14) <= regular_market_time <= dt_time(15, 30)
    logging.info(f"Regular market hours check - Is Open: {is_regular_open}")
    return is_regular_open

def fetch_and_insert_data():
    """Modified fetch and insert function with detailed logging."""
    try:
        market_open = is_market_open()
        logging.info(f"Market open status: {market_open}")
        
        if market_open:
            logging.info("Market is open. Starting data fetch...")
            fetcher.start_fetching()
        else:
            current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            logging.info(f"Market is closed at {current_time}. Waiting for next check.")
    except Exception as e:
        logging.error(f"Error in fetch_and_insert_data: {e}")

if __name__ == '__main__':
    # Set up logging with more detailed format
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('market_timing.log')
        ]
    )
    
    # Database setup
    db_config = configDB()
    check_and_create_db(db_config)

    fetcher = OptionChainFetcher()

    # Schedule data fetching during market hours every second
    schedule.every(1).seconds.do(fetch_and_insert_data)

    logging.info("Starting market data fetching service...")
    
    while True:
        try:
            schedule.run_pending()
            t.sleep(1)  # Changed from 0 to 1 second to reduce CPU usage
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
