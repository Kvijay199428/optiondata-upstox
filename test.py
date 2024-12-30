import os
import json
import psycopg2
from psycopg2 import sql, extras
from configparser import ConfigParser
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
from tqdm import tqdm
import upstox_client
from upstox_client.rest import ApiException

# Configure logging with rotating file handler
log_file = "api/logs/test.log"
handler = RotatingFileHandler(log_file, maxBytes=5000000, backupCount=5)  # 5 MB max per log file, keep 5 backups
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info("Starting real-time data insertion script.")

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
        logging.info("Database connected successfully.")  # Success message
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [db_config['database']])
        exists = cur.fetchone()
        if not exists:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_config['database'])))
        cur.close()
    finally:
        if conn is not None:
            conn.close()

def create_table(conn, table_name, instrument):
    cur = conn.cursor()

    # Extract instrument_key from the instrument dictionary
    instrument_key = instrument.get('instrument_key', '')

    # Sanitize the instrument_key to replace special characters for index name
    sanitized_instrument_key = instrument_key.replace('|', '_')

    try:
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                timestamp TIMESTAMP PRIMARY KEY,
                open_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                close_price FLOAT,
                volume INTEGER,
                open_interest INTEGER,
                instrument_type TEXT,
                trading_symbol TEXT,
                instrument_key TEXT,
                table_name TEXT
            )
        """).format(sql.Identifier(table_name)))

        # Create first index based on instrument_key
        cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS {} 
            ON {} (instrument_key)
        """).format(
            sql.Identifier(f"idx_{sanitized_instrument_key}_instrument_key"),
            sql.Identifier(table_name)
        ))

        # Create second index based on table_name
        cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS {} 
            ON {} (table_name)
        """).format(
            sql.Identifier(f"idx_{table_name}_table_name"),
            sql.Identifier(table_name)
        ))

        # Create third index based on trading_symbol
        cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS {} 
            ON {} (trading_symbol)
        """).format(
            sql.Identifier(f"idx_{table_name}_trading_symbol"),
            sql.Identifier(table_name)
        ))

        conn.commit()
    finally:
        cur.close()


# Batch insert data
def batch_insert_data(conn, table_name, data, instrument):
    cur = conn.cursor()
    try:
        insert_query = sql.SQL('''
            INSERT INTO {} (timestamp, open_price, high_price, low_price, close_price, volume, open_interest, 
                            instrument_type, 
                            trading_symbol, instrument_key, table_name)
            VALUES %s
            ON CONFLICT (timestamp) DO NOTHING;
        ''').format(sql.Identifier(table_name))

        # Prepare data for insertion
        extras.execute_values(
            cur, insert_query, 
            [(row[0], row[1], row[2], row[3], row[4], row[5], row[6], 
              instrument['instrument_type'], 
              instrument['trading_symbol'], 
              instrument['instrument_key'], 
              table_name) for row in data],
            template=None, page_size=100
        )
        # Get the number of rows inserted
        row_count = cur.rowcount
        conn.commit()
        # Log the number of rows inserted
        logging.info(f"Inserted {row_count} rows into table '{table_name}' for instrument_key '{instrument['instrument_key']}'.")
    finally:
        cur.close()

# Fetch historical data
def fetch_historical_data(instrument_key, interval, from_date, to_date):
    api_version = '2.0'
    api_instance = upstox_client.HistoryApi()
    try:
        from_date_str = from_date.strftime('%Y-%m-%d')
        to_date_str = to_date.strftime('%Y-%m-%d')

        api_response = api_instance.get_historical_candle_data1(instrument_key, interval, to_date_str, from_date_str, api_version)
        response_json = api_response if isinstance(api_response, dict) else api_response.to_dict()

        if response_json.get('status') == 'success':
            candles = response_json.get('data', {}).get('candles', [])
            if candles:
                data_to_insert = [
                    (candle[0], candle[1], candle[2], candle[3], candle[4], candle[5], candle[6])
                    for candle in candles if len(candle) == 7
                ]
                return data_to_insert
        return []
    except ApiException as e:
        logging.error(f"API Exception for {instrument_key}: {e}")
        return []
    except Exception as e:
        logging.error(f"An error occurred for {instrument_key}: {e}")
        return []

# Convert expiry timestamp to readable date
def convert_expiry(expiry_timestamp):
    return datetime.fromtimestamp(expiry_timestamp / 1000).strftime('%Y-%m-%d')

# Calculate from date based on the interval
def calculate_from_date(interval, today):
    if interval == '1minute':
        # Define the range for fetching historical data
        max_days = 30
        start_date = today - timedelta(days=max_days)
        end_date = today

        data_frames = []
        step_count = 0
        total_steps = (end_date - start_date).days // max_days + 1

        while start_date < end_date:
            fdate = start_date.strftime("%Y-%m-%d 09:00")
            tdate = (start_date + timedelta(days=max_days)).strftime("%Y-%m-%d 16:00")

            # Log the date range for the current step
            logging.info(f"Fetching data from {fdate} to {tdate}...")

            # You can perform data fetching here
            # For now, just simulate with placeholders
            # Example: data_frames.append(fetch_data(fdate, tdate))

            # Update the start date for the next step
            start_date += timedelta(days=max_days)

            step_count += 1

        # Return the last start_date (end date in this case) for further processing
        return end_date
    
    elif interval == '30minute':
        return today - timedelta(days=365)
    elif interval == 'day':
        return today - timedelta(days=365 * 5)
    elif interval == 'week':
        return today - timedelta(weeks=10 * 52)
    elif interval == 'month':
        return today - timedelta(weeks=20 * 52)
    return today

# Main function
def main():
    db_params = configDB()
    check_and_create_db(db_params)
    
    conn = psycopg2.connect(**db_params)
    
    json_dir = 'api/instrument/'
    files = [f for f in os.listdir(json_dir) if f.endswith('.json')]
    
    if not files:
        print("No JSON files found.")
        return

    print("Available JSON files:")
    for idx, file in enumerate(files):
        print(f"{idx + 1}: {file}")
    
    file_choice = input("Enter the number corresponding to your choice select 'nse.json': ")
    try:
        file_idx = int(file_choice) - 1
        if 0 <= file_idx < len(files):
            selected_file = files[file_idx]
            file_path = os.path.join(json_dir, selected_file)
            logging.debug(f"Selected JSON file: {selected_file}.")
        else:
            print("Invalid file selection.")
            return
    except ValueError:
        print("Invalid input.")
        return

    try:
        with open(file_path, 'r') as f:
            instruments_data = json.load(f)
    except json.JSONDecodeError:
        print("Error decoding JSON.")
        return

    criteria = {"segment": "NSE_EQ", "instrument_type": "EQ"}
    filtered_instruments = [instr for instr in instruments_data 
                            if instr.get("segment") == criteria["segment"] and 
                            instr.get("instrument_type") == criteria["instrument_type"]]
    
    if not filtered_instruments:
        print("No instruments match the criteria.")
        return

    for instrument in filtered_instruments:
        table_name = instrument['trading_symbol']
        logging.debug(f"Processing instrument: {table_name}.")
        create_table(conn, table_name, instrument)
        
        today = datetime.today()
        from_date = calculate_from_date('1minute', today)
        to_date = today
        
        historical_data = fetch_historical_data(instrument['instrument_key'], '1minute', from_date, to_date)
        if historical_data:
            batch_insert_data(conn, table_name, historical_data, instrument)
        else:
            logging.error(f"No data returned for {instrument['instrument_key']}.")

    conn.close()

if __name__ == "__main__":
    main()
