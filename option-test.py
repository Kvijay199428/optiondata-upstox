import requests
import psycopg2
from psycopg2 import sql
import logging
from concurrent_log_handler import ConcurrentRotatingFileHandler  # Changed handler
from configparser import ConfigParser
import os
from datetime import datetime, time as dt_time
import time as t
import schedule
import pytz
from tqdm import tqdm
import atexit  # For graceful shutdown

# === Configuration Constants ===

# Define a new log directory outside of OneDrive to prevent access conflicts
LOG_DIRECTORY = "api/logs/"
os.makedirs(LOG_DIRECTORY, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIRECTORY, "optionChain.log")

# === Logging Configuration ===

# Configure logging with ConcurrentRotatingFileHandler for Windows compatibility
handler = ConcurrentRotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5)  # 5 MB max per log file, keep 5 backups
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

# Register a shutdown handler to close log handlers gracefully
def close_log_handlers():
    handlers = logging.root.handlers[:]
    for hdlr in handlers:
        hdlr.close()
        logging.root.removeHandler(hdlr)

atexit.register(close_log_handlers)

logger.info("Starting real-time data insertion script.")

# === Helper Functions ===

def get_current_timestamp():
    now = datetime.now(pytz.timezone('Asia/Kolkata'))  # Make timezone-aware
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
            'expiry_date': '2024-09-20'
        }
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        # Sanitize and generate the table name
        table_name = sanitize_table_name(params['expiry_date'], params['instrument_key'])  # Corrected parameters order

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
def is_market_open(current_time):
    # Market hours: 09:15 to 15:30 Asia/Kolkata
    market_start = dt_time(9, 15)
    market_end = dt_time(15, 30)
    return market_start <= current_time.time() <= market_end

def fetch_and_insert_data():
    current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    if is_market_open(current_time):
        logging.info("Market is open. Fetching data.")
        fetcher.start_fetching()
    else:
        logging.info("Market is closed. Preparing to stop script.")
        # Trigger graceful shutdown by raising a KeyboardInterrupt
        raise KeyboardInterrupt("Market closed")

if __name__ == '__main__':
    # Database setup
    db_config = configDB()
    check_and_create_db(db_config)

    fetcher = OptionChainFetcher()

    # Define timezone
    tz = pytz.timezone('Asia/Kolkata')

    # Define market start and end times
    now = datetime.now(tz)
    today = now.date()
    market_start_time = tz.localize(datetime.combine(today, dt_time(9, 15)))
    market_end_time = tz.localize(datetime.combine(today, dt_time(15, 30)))

    # Handle cases based on current time
    if now < market_start_time:
        logging.info("Market not open yet. Waiting until market opens at 09:15.")
        time_to_wait = (market_start_time - now).total_seconds()
        hours, remainder = divmod(int(time_to_wait), 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"Waiting for market to open in {hours}h {minutes}m {seconds}s.")
        t.sleep(time_to_wait)
        now = datetime.now(tz)

    if now >= market_end_time:
        logging.info("Market already closed. Exiting.")
        print("OptionChain Data Fetching Completed")
        exit()

    # Calculate total duration in seconds for the progress bar (09:15 to 15:30)
    total_duration = (market_end_time - market_start_time).total_seconds()

    # Calculate elapsed time since market opened
    elapsed_duration = (now - market_start_time).total_seconds()
    if elapsed_duration < 0:
        elapsed_duration = 0
    elif elapsed_duration > total_duration:
        elapsed_duration = total_duration

    # Initialize tqdm progress bar
    progress_bar = tqdm(
        total=total_duration,
        initial=elapsed_duration,
        bar_format='{l_bar}{bar}| {percentage:.2f}% ',
        desc='OptionChain Progress',
        unit='s',
        dynamic_ncols=True
    )

    # Schedule data fetching during market hours every second
    schedule.every(1).seconds.do(fetch_and_insert_data)

    try:
        while True:
            schedule.run_pending()

            # Get current time
            current_time = datetime.now(tz)

            if current_time >= market_end_time:
                # Complete the progress bar
                progress_bar.n = total_duration
                progress_bar.refresh()
                progress_bar.close()
                logging.info("OptionChain Data Fetching Completed")
                print("OptionChain Data Fetching Completed")
                break

            # Calculate new elapsed duration
            new_elapsed = (current_time - market_start_time).total_seconds()

            if new_elapsed > progress_bar.n:
                # Update progress bar with the difference
                delta = new_elapsed - progress_bar.n
                progress_bar.update(delta)

            # Sleep for 1 second before next update
            t.sleep(1)

    except KeyboardInterrupt as e:
        # Handle graceful shutdown on interrupt or market closure
        progress_bar.close()
        logging.info(f"Script interrupted: {e}")
        if str(e) == "Market closed":
            print("OptionChain Data Fetching Completed")
        else:
            print("OptionChain Data Fetching Interrupted by User.")
