import logging
from logging.handlers import RotatingFileHandler
import psycopg2
import pandas as pd
import talib
from configparser import ConfigParser

# Configure logging with rotating file handler
log_file = "api/logs/test.log"
handler = RotatingFileHandler(log_file, maxBytes=5000000, backupCount=5)  # 5 MB max per log file, keep 5 backups
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info("Starting database connection script.")

# Fetch database config
def configDB(filename="api/ini/NSE.ini", section="postgresql"):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        logger.error(f"Section {section} not found in {filename} file.")
        raise Exception(f'Section {section} not found in {filename} file.')
    return db

# Connect to the database and fetch data from the selected table
def connect(table_name, timeframe='1minute', limit=5):
    conn = None
    try:
        # Convert table_name to uppercase for case-insensitive queries
        table_name = table_name.upper()

        # Load connection parameters
        params = configDB()
        
        # Connect to PostgreSQL
        logger.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # Create a cursor object
        cur = conn.cursor()

        # Query to fetch limited data from the specified table
        cur.execute(f'''
            SELECT * FROM "{table_name}" 
            WHERE timestamp >= NOW() - INTERVAL '30 days'
            LIMIT {limit};
        ''')

        # Fetch column names
        colnames = [desc[0] for desc in cur.description]
        
        # Fetch data from the query
        rows = cur.fetchall()

        # Convert the fetched data into a pandas DataFrame
        df = pd.DataFrame(rows, columns=colnames)
        
        # Close the communication with the PostgreSQL database
        cur.close()
        return df

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error: {error}")
        print(f"Error: {error}")
        return None
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')

# Get list of table names
def get_table_names():
    conn = None
    try:
        params = configDB()
        logger.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        tables = cur.fetchall()
        cur.close()
        return [table[0] for table in tables]
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error: {error}")
        print(f"Error: {error}")
        return []
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')

# List all table names and provide input for table selection
def select_table_name():
    table_names = get_table_names()
    if not table_names:
        print("No tables found in the database.")
        return None
    
    print("Available tables:")
    for i, name in enumerate(table_names, start=1):
        print(f"{i}. {name}")
    
    choice = int(input("Select table number: "))
    if 1 <= choice <= len(table_names):
        return table_names[choice - 1]
    else:
        print("Invalid choice.")
        return None

# Prompt for timeframe selection
def select_timeframe():
    timeframes = ['1minute', '3minute', '5minute', '15minute']
    print("Available timeframes:")
    for i, tf in enumerate(timeframes, start=1):
        print(f"{i}. {tf}")
    
    choice = int(input("Select timeframe number: "))
    if 1 <= choice <= len(timeframes):
        return timeframes[choice - 1]
    else:
        print("Invalid choice.")
        return '1minute'  # Default value if the choice is invalid

# Merge data based on the selected timeframe
def merge_data_by_timeframe(df, timeframe):
    if timeframe == '1minute':
        # No merging needed for 1-minute intervals
        return df

    # Define the resampling rule based on the timeframe
    resample_rule = {
        '3minute': '3min',
        '5minute': '5min',
        '15minute': '15min'
    }.get(timeframe, '1min')

    # Ensure the 'timestamp' column is in datetime format and set it as index
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    
    # Resample and aggregate data
    df_resampled = df.resample(resample_rule).agg({
        'open_price': 'first',
        'high_price': 'max',
        'low_price': 'min',
        'close_price': 'last',
        'volume': 'sum',
        'open_interest': 'sum'
    }).dropna()

    df_resampled.reset_index(inplace=True)
    return df_resampled

# Calculate multiple SMAs using TA-Lib
def calculate_sma(df, periods=[20, 14, 9]):
    if 'close_price' in df.columns:
        close = df['close_price'].values
        for period in periods:
            df[f'SMA_{period}'] = talib.SMA(close, timeperiod=period)
    return df

def identify_crossovers(df):
    # Ensure the required SMA columns are in the DataFrame
    if 'SMA_20' not in df.columns or 'SMA_14' not in df.columns or 'SMA_9' not in df.columns:
        print("SMA columns not found in the DataFrame.")
        return None
    
    # Create boolean conditions for crossovers
    crossovers_9_14 = ((df['SMA_9'] > df['SMA_14']) & (df['SMA_9'].shift(1) <= df['SMA_14'].shift(1))) | \
                      ((df['SMA_9'] < df['SMA_14']) & (df['SMA_9'].shift(1) >= df['SMA_14'].shift(1)))
    
    crossovers_14_20 = ((df['SMA_14'] > df['SMA_20']) & (df['SMA_14'].shift(1) <= df['SMA_20'].shift(1))) | \
                       ((df['SMA_14'] < df['SMA_20']) & (df['SMA_14'].shift(1) >= df['SMA_20'].shift(1)))

    crossovers_9_20 = ((df['SMA_9'] > df['SMA_20']) & (df['SMA_9'].shift(1) <= df['SMA_20'].shift(1))) | \
                      ((df['SMA_9'] < df['SMA_20']) & (df['SMA_9'].shift(1) >= df['SMA_20'].shift(1)))

    # Sum up all crossover events
    total_crossovers_9_14 = crossovers_9_14.sum()
    total_crossovers_14_20 = crossovers_14_20.sum()
    total_crossovers_9_20 = crossovers_9_20.sum()

    print(f"Total SMA 9/14 Crossovers: {total_crossovers_9_14}")
    print(f"Total SMA 14/20 Crossovers: {total_crossovers_14_20}")
    print(f"Total SMA 9/20 Crossovers: {total_crossovers_9_20}")

    return total_crossovers_9_14, total_crossovers_14_20, total_crossovers_9_20


def main():
    # Example inputs
    table_name = 'GTPL'  # Replace with your table name
    timeframe = '5minute'  # Replace with your selected timeframe
    
    # Fetch data from the database
    df = connect(table_name, timeframe)
    
    if df is not None:
        # Calculate SMAs
        df = calculate_sma(df, periods=[20, 14, 9])
        
        # Identify crossovers
        crossovers = identify_crossovers(df)
        
        # Optionally: Save or process the DataFrame as needed
        print(df.head())

if __name__ == "__main__":
    main()
