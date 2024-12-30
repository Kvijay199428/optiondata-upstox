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

# Ensure log directory exists
# Using script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, 'api', 'logs', 'historic_nse.log')
os.makedirs(os.path.dirname(log_file), exist_ok=True)
handler = RotatingFileHandler(log_file, maxBytes=5000000, backupCount=5)  # 5 MB max per log file, keep 5 backups
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info("Starting real-time data insertion script.")

def configDB(filename="api/ini/NSE.ini", section="postgresql"):
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construct the full path to the configuration file
    full_path = os.path.join(script_dir, filename)
    
    parser = ConfigParser()
    parser.read(full_path)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        logging.error(f"Section {section} not found in {full_path} file.")
        raise Exception(f'Section {section} not found in {full_path} file.')
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

def create_table(conn, table_name, instrument_key):
    cur = conn.cursor()

    # Sanitize the instrument_key to replace special characters for index name
    sanitized_instrument_key = instrument_key.replace('|', '_')

    try:
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                time TIMESTAMPTZ PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
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

        # Create fourth index based on instrument_type
        cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS {} 
            ON {} (instrument_type)
        """).format(
            sql.Identifier(f"idx_{table_name}_instrument_type"),
            sql.Identifier(table_name)
        ))

        conn.commit()
    finally:
        cur.close()

def batch_insert_data(conn, table_name, data, instrument):
    cur = conn.cursor()
    try:
        insert_query = sql.SQL('''
            INSERT INTO {} (time, open, high, low, close, volume, open_interest, 
                            instrument_type, 
                            trading_symbol, instrument_key, table_name)
            VALUES %s
            ON CONFLICT (time) DO NOTHING;
        ''').format(sql.Identifier(table_name))

        # Prepare data for insertion
        extras.execute_values(
            cur, insert_query, 
            [(
                datetime.fromisoformat(row[0]), 
                row[1], row[2], row[3], row[4], row[5], row[6], 
                instrument['instrument_type'], 
                instrument['trading_symbol'], 
                instrument['instrument_key'], 
                table_name
            ) for row in data],
            template=None, page_size=100
        )
        # Get the number of rows inserted
        row_count = cur.rowcount
        conn.commit()
        # Log the number of rows inserted
        logging.info(f"Inserted {row_count} rows into table '{table_name}' for instrument_key '{instrument['instrument_key']}'.")
    except Exception as e:
        logging.error(f"Error inserting data for {instrument['instrument_key']}: {e}")
        conn.rollback()
        # Print the first few rows of data to help diagnose the issue
        print(f"First few rows of data for {instrument['trading_symbol']}:")
        print(data[:5])
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
                            if instr.get("segment") == criteria["segment"] 
                            and instr.get("instrument_type") == criteria["instrument_type"]]

    logging.debug(f"Filtered {len(filtered_instruments)} instruments matching the criteria: {criteria}.")
    
    if not filtered_instruments:
        logging.warning("No instruments match the criteria.")
        print("No instruments match the criteria.")
        return
    
    intervals = {
        '1': '1minute',
        '2': '30minute',
        '3': 'day',
        '4': 'week',
        '5': 'month'
    }
    
    print("Select interval:")
    print("1: 1 Minute")
    print("2: 30 Minute")
    print("3: Daily")
    print("4: Weekly")
    print("5: Monthly")
    
    interval_choice = input("Enter the number corresponding to your choice: ")
    interval = intervals.get(interval_choice)
    
    if not interval:
        print("Invalid interval selection.")
        return

    today = datetime.now()
    from_date = calculate_from_date(interval, today)

    total_instruments = len(filtered_instruments)
    progress_bar = tqdm(total=total_instruments, desc="Processing Instruments", unit="instrument")

    for instrument in filtered_instruments:
        table_name = instrument['trading_symbol'].replace(" ", "_")
        data = fetch_historical_data(instrument['instrument_key'], interval, from_date, today)

        if data:
            create_table(conn, table_name, instrument['instrument_key'])
            batch_insert_data(conn, table_name, data, instrument)
        progress_bar.update(1)

    progress_bar.close()
    conn.close()

# Calculate from date based on the interval
def calculate_from_date(interval, today):
    if interval == '1minute':
        return today - timedelta(days=30)
    elif interval == '30minute':
        return today - timedelta(days=365)
    elif interval == 'day':
        return today - timedelta(days=365 * 5)
    elif interval == 'week':
        return today - timedelta(weeks=10 * 52)
    elif interval == 'month':
        return today - timedelta(weeks=20 * 52)
    return today

if __name__ == "__main__":
    main()