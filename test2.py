import requests
import psycopg2
from psycopg2 import sql
import logging
from logging.handlers import RotatingFileHandler
from configparser import ConfigParser
import os
from datetime import datetime, time as dt_time 
import time as t
import calendar
from datetime import datetime, timedelta
import schedule
import pytz

# Configure logging with rotating file handler
log_file = "api/logs/test.log"
handler = RotatingFileHandler(log_file, maxBytes=5000000, backupCount=5)  # 5 MB max per log file, keep 5 backups
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

logger.info("Starting real-time data insertion script.")

# Automatically get the current year and month
current_date = datetime.now()
year = current_date.year
month = current_date.month

def get_current_timestamp():
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S") + f".{now.microsecond // 1000:03d}"

# Fetch database config
def configDB(filename="api/ini/test.ini", section="postgresql"):
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

# Function to generate common trading holidays based on the year
def generate_trading_holidays(year):
    holidays = [
        f'{year}-01-26',  # Republic Day
        f'{year}-08-15',  # Independence Day
        f'{year}-10-02',  # Gandhi Jayanti
        f'{year}-12-25',  # Christmas
    ]
    
    # Add variable holidays (example - you may need to update these yearly)
    holidays.extend([
        f'{year}-03-29',  # Good Friday (example - date changes yearly)
        f'{year}-04-11',  # Eid-Ul-Fitr (example - date changes yearly)
        f'{year}-11-12',  # Diwali-Balipratipada (example - date changes yearly)
    ])

    return holidays

# Generate trading holidays for the current year
trading_holidays = generate_trading_holidays(year)

# Helper function to check if a date is a trading holiday
def is_holiday(date_str):
    return date_str in trading_holidays

# Function to get the last specific weekday of a month
def get_last_weekday(year, month, weekday):
    cal = calendar.Calendar()
    days = [day for day in cal.itermonthdays2(year, month) if day[1] == weekday and day[0] != 0]
    return days[-1][0]  # Last occurrence of the weekday

# Function to adjust for holidays
def adjust_for_holiday(year, month, day):
    expiry_date = datetime(year, month, day)
    expiry_str = expiry_date.strftime('%Y-%m-%d')
    
    # Adjust the date if it falls on a holiday
    while is_holiday(expiry_str):
        expiry_date -= timedelta(days=1)
        expiry_str = expiry_date.strftime('%Y-%m-%d')
    
    return expiry_str

# Function to calculate expiry date based on instrument key
def calculate_expiry_date(instrument_key, year, month):
    if instrument_key == 'NSE_INDEX|Nifty 50' or instrument_key == 'BSE_INDEX|SENSEX':
        day = get_last_weekday(year, month, calendar.THURSDAY)
    elif instrument_key == 'NSE_INDEX|Nifty Bank':
        day = get_last_weekday(year, month, calendar.WEDNESDAY)
    elif instrument_key == 'NSE_INDEX|Nifty Fin Service':
        day = get_last_weekday(year, month, calendar.TUESDAY)
    elif instrument_key == 'NSE_INDEX|NIFTY MID SELECT':
        day = get_last_weekday(year, month, calendar.MONDAY)
    elif instrument_key == 'NSE_INDEX|Nifty Next 50' or instrument_key == 'BSE_INDEX|BANKEX':
        day = get_last_weekday(year, month, calendar.FRIDAY)
    else:
        raise ValueError(f"Unknown instrument key: {instrument_key}")
    
    # Adjust for holidays
    return adjust_for_holiday(year, month, day)

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
        
        # Generate params with calculated expiry dates
        params_list = [
            {
                'instrument_key': 'BSE_INDEX|SENSEX',
                'expiry_date': calculate_expiry_date('BSE_INDEX|SENSEX', year, month)
            },
            {
                'instrument_key': 'NSE_INDEX|Nifty Next 50',
                'expiry_date': calculate_expiry_date('NSE_INDEX|Nifty Next 50', year, month)
            },
            {
                'instrument_key': 'NSE_INDEX|NIFTY MID SELECT',
                'expiry_date': calculate_expiry_date('NSE_INDEX|NIFTY MID SELECT', year, month)
            },
            {
                'instrument_key': 'NSE_INDEX|Nifty 50',
                'expiry_date': calculate_expiry_date('NSE_INDEX|Nifty 50', year, month)
            },
            {
                'instrument_key': 'NSE_INDEX|Nifty Fin Service',
                'expiry_date': calculate_expiry_date('NSE_INDEX|Nifty Fin Service', year, month)
            },
            {
                'instrument_key': 'NSE_INDEX|Nifty Bank',
                'expiry_date': calculate_expiry_date('NSE_INDEX|Nifty Bank', year, month)
            },
            {
                'instrument_key': 'BSE_INDEX|BANKEX',
                'expiry_date': calculate_expiry_date('BSE_INDEX|BANKEX', year, month)
            }
        ]

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        all_data = []
        for params in params_list:
            try:
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                logging.debug(f"Request URL: {response.url}")
                data = response.json()
                if data.get('status') == 'success' and 'data' in data:
                    all_data.extend(data['data'])
                else:
                    logging.error(f"API error or unexpected response format for {params['instrument_key']}: {data}")
            except requests.RequestException as e:
                logging.error(f"Request failed for {params['instrument_key']}: {e}")

        return all_data

    def start_fetching(self):
        logging.info("Starting data fetching...")
        data = self.fetch_data()

        if data:
            logging.debug(f"Data fetched successfully: {data}")
            db_config = configDB()

            for item in data:
                expiry = item.get('expiry')
                underlying_key = item.get('underlying_key')
                
                # Generate a valid table name using the expiry date and instrument key
                table_name = sanitize_table_name(expiry, underlying_key)

                insert_data_into_db(db_config, table_name, item)
        else:
            logging.error("No data fetched from API.")

# Schedule job
def is_market_open():
    now = datetime.now(pytz.timezone('Asia/Kolkata')).time()
    return dt_time(9, 15) <= now <= dt_time(15, 30)

def fetch_and_insert_data():
    if is_market_open():
        logging.info("Market is open. Fetching data.")
        fetcher.start_fetching()
    else:
        logging.info("Market is closed. Skipping data fetch.")

if __name__ == '__main__':
    # Database setup
    db_config = configDB()
    check_and_create_db(db_config)

    fetcher = OptionChainFetcher()

    # Schedule data fetching during market hours every 1 second
    schedule.every(1).seconds.do(fetch_and_insert_data)

    while True:
        schedule.run_pending()
        t.sleep(1)