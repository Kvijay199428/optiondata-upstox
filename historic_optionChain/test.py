# OC_Sensex.py

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
import threading
from queue import Queue
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich import print as rprint
from pathlib import Path

# At the top of the file
BASE_DIR = Path(__file__).parent
ACCESS_TOKEN_FILE_PATH = BASE_DIR / "api" / "token" / "accessToken_oc.txt"
CONFIG_FILE_PATH = BASE_DIR / "api" / "ini" / "test.ini"
LOG_DIRECTORY = BASE_DIR / "api" / "logs"
LOG_FILE = LOG_DIRECTORY / "Nifty50.log"

# Ensure the directory exists
def ensure_directory_exists(directory):
    """Create directory if it doesn't exist."""
    try:
        os.makedirs(directory, exist_ok=True)
        logging.debug(f"Ensuring directory exists: {directory}")
    except Exception as e:
        logging.error(f"Error creating directory {directory}: {e}")
        raise

# Enhanced Logging Configuration
def setup_logging():
    """Set up comprehensive logging with detailed configuration."""
    try:
        # Ensure log directory exists
        ensure_directory_exists(LOG_DIRECTORY)

        # Configure logging with more detailed rotating file handler
        handler = RotatingFileHandler(
            LOG_FILE, 
            maxBytes=10*1024*1024,  # 10 MB 
            backupCount=10  # Keep more backup logs
        )
        
        # More detailed formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(funcName)s() - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)

        # Console handler for immediate visibility
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)

        # Root logger configuration
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.addHandler(console_handler)

        logging.info("Logging system initialized successfully.")
    except Exception as e:
        print(f"Critical error setting up logging: {e}")
        sys.exit(1)

# Call logging setup at the start
setup_logging()

class MarketCalendar:
    def __init__(self):
        self.holidays = []
        self.special_timings = {}
        self.fetch_holiday_data()
        
    def fetch_holiday_data(self):
        """Fetch holiday data once during initialization"""
        try:
            holiday_response = market_holiday_date_wise()
            if holiday_response and holiday_response.get('status') == 'success':
                for holiday in holiday_response.get('data', []):
                    date = holiday['date']
                    self.holidays.append(date)
                    
                    # Store special timing information if available
                    exchange_info = self.parse_exchange_timings(holiday)
                    if exchange_info:
                        self.special_timings[date] = exchange_info
                        
                logging.info(f"Successfully loaded {len(self.holidays)} holidays and {len(self.special_timings)} special timing dates")
            else:
                logging.warning("Failed to fetch holiday data, assuming regular trading hours")
        except Exception as e:
            logging.error(f"Error fetching holiday data: {e}")

    def parse_exchange_timings(self, holiday_data):
        """Parse exchange timings from holiday data"""
        try:
            for exchange_info in holiday_data.get('open_exchanges', []):
                if isinstance(exchange_info, str):
                    parts = exchange_info.split('(')
                    exchange_name = parts[0].strip()
                    if exchange_name == 'NSE':
                        timing_parts = parts[1].strip(')').split(',')
                        start_ms = int(timing_parts[0].split(':')[1].strip())
                        end_ms = int(timing_parts[1].split(':')[1].strip())
                        return {
                            'start': start_ms,
                            'end': end_ms
                        }
        except Exception as e:
            logging.error(f"Error parsing exchange timings: {e}")
        return None
    
    def is_market_open(self):
        """Comprehensive check for market open status"""
        now = datetime.now(pytz.timezone('Asia/Kolkata'))
        current_date = now.strftime('%Y-%m-%d')
        current_time = now.time()
        
        # Check if it's a holiday or Sunday (only Sunday is weekend now)
        if (current_date in self.holidays or 
            now.weekday() >= 6):  # Only Sunday (6) is considered weekend
            logging.info(f"Market closed: Holiday or Sunday {current_date}")
            return False
        
        # Check for special timing dates
        if current_date in self.special_timings:
            timing_info = self.special_timings[current_date]
            start_time = self.convert_milliseconds_to_time(timing_info['start'])
            end_time = self.convert_milliseconds_to_time(timing_info['end'])
            
            market_open = start_time.time() <= current_time <= end_time.time()
            logging.info(f"Special timing check - Open: {market_open}")
            return market_open
        
        # Regular market hours (9:15 AM to 3:30 PM)
        regular_market_open = (
            dt_time(0, 15) <= current_time <= dt_time(15, 30)
        )
        
        if regular_market_open:
            logging.info(f"Market open during regular hours at {current_time}")
        
        return regular_market_open
    
    @staticmethod
    def convert_milliseconds_to_time(milliseconds):
        """Convert milliseconds timestamp to datetime object in IST"""
        try:
            seconds = milliseconds / 1000
            return datetime.fromtimestamp(seconds, pytz.timezone('Asia/Kolkata'))
        except Exception as e:
            logging.error(f"Error converting milliseconds {milliseconds}: {e}")
            return None

class ExpiryManager:
    def __init__(self):
        self.expiry_dates = []
        self.threads = {}
        self.progress_data = {}
        self.lock = threading.Lock()
        
    def get_next_n_expiries(self, n=5):
        """Get the next n valid thursday expiry dates starting from current date."""
        current_date = datetime.now()
        expiries = []
        month = current_date.month
        year = current_date.year
        
        while len(expiries) < n:
            # Get all thursday expiries for current month
            month_expiries = self.get_month_thursday_expiries(year, month)
            
            # Filter out past dates if we're looking at current month
            if month == current_date.month and year == current_date.year:
                month_expiries = [date for date in month_expiries if date >= current_date.date()]
            
            # Add expiries until we reach n or run out of dates
            for expiry in month_expiries:
                if len(expiries) < n:
                    expiries.append(expiry)
                else:
                    break
            
            # Move to next month if we still need more dates
            if len(expiries) < n:
                if month == 12:
                    month = 1
                    year += 1
                else:
                    month += 1
                    
        return expiries
    
    def get_month_thursday_expiries(self, year, month):
        """Get all thursday expiry dates for a given month."""
        first_day = datetime(year, month, 1)
        last_day = (first_day + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        
        thursdays = []
        current_day = first_day
        
        while current_day <= last_day:
            if current_day.weekday() == 3:  # thursday
                thursday_date = current_day.date()
                # Check if it's not a holiday
                if self.is_valid_trading_day(thursday_date):
                    thursdays.append(thursday_date)
            current_day += timedelta(days=1)
            
        return thursdays
    
    def is_valid_trading_day(self, date):
        """Check if the given date is a valid trading day."""
        holiday_response = market_holiday_date_wise()
        if holiday_response and holiday_response.get('status') == 'success':
            holidays_list = [holiday['date'] for holiday in holiday_response.get('data', [])]
            return date.strftime('%Y-%m-%d') not in holidays_list
        return True

def resource_path(relative_path):
    """Get the absolute path to the resource, works for dev and for PyInstaller."""
    try:
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

def get_current_timestamp():
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S") + f".{now.microsecond // 1000:03d}"

def configDB(filename=CONFIG_FILE_PATH, section="postgresql"):
    """Enhanced database configuration with more robust error handling."""
    try:
        logging.debug(f"Attempting to read database configuration from {filename}")
        parser = ConfigParser()
        
        # Ensure file exists and is readable
        if not os.path.exists(filename):
            logging.error(f"Configuration file not found: {filename}")
            raise FileNotFoundError(f"Configuration file not found: {filename}")
        
        parser.read(filename)
        
        if not parser.has_section(section):
            logging.error(f"Section {section} not found in {filename}")
            raise ValueError(f'Section {section} not found in {filename} file.')
        
        db = dict(parser.items(section))
        logging.info(f"Successfully loaded database configuration for section: {section}")
        return db
    
    except Exception as e:
        logging.error(f"Error reading database configuration: {e}")
        raise

def check_and_create_db(db_config):
    """Check and create database if not exists."""
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

def sanitize_table_name(expiry_date, instrument_key):
    """Sanitize table name for PostgreSQL."""
    logging.debug(f"Sanitizing table name for expiry {expiry_date} and instrument {instrument_key}")
    sanitized_instrument_key = instrument_key.replace(' ', '_').replace('|', '_').lower()
    sanitized_expiry_date = str(expiry_date).replace('-', '_')
    return f"{sanitized_instrument_key}_{sanitized_expiry_date}"

def insert_data_into_db(db_config, table_name, data):
    """Insert option chain data into PostgreSQL database."""
    logging.debug(f"Inserting data into table {table_name}.")
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Create table if not exists
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

        # Prepare insert query
        insert_query = f"""
        INSERT INTO {table_name} (timestamp, expiry, strike_price, underlying_spot_price, call_ltp, 
            call_close_price, call_volume, call_oi, call_bid_price, call_bid_qty, call_ask_price, 
            call_ask_qty, call_vega, call_theta, call_gamma, call_delta, call_iv, put_ltp, 
            put_close_price, put_volume, put_oi, put_bid_price, put_bid_qty, put_ask_price, 
            put_ask_qty, put_vega, put_theta, put_gamma, put_delta, put_iv, pcr, underlying_key)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        # Prepare values for insertion
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

        # Create indexes to improve query performance
        cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS {} ON {} (underlying_key)
        """).format(
            sql.Identifier(f"idx_{table_name}_underlying_key"),
            sql.Identifier(table_name)
        ))
        cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS {} ON {} (strike_price)
        """).format(
            sql.Identifier(f"idx_{table_name}_strike_price"),
            sql.Identifier(table_name)
        ))
        cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS {} ON {} (expiry)
        """).format(
            sql.Identifier(f"idx_{table_name}_expiry"),
            sql.Identifier(table_name)
        ))

        # Execute insert query
        cur.execute(insert_query, values)
        conn.commit()
        logging.info(f"Data inserted successfully into {table_name}")
        cur.close()

    except Exception as error:
        logging.error(f"Error while inserting data into {table_name}: {error}")
    finally:
        if conn is not None:
            conn.close()

def validate_access_token(token):
    """Validate that the access token is not empty and has the expected format"""
    if not token:
        raise ValueError("Access token is empty")
    if not token.strip():
        raise ValueError("Access token contains only whitespace")
    # Add any other validation specific to your Upstox API token format
    return token.strip()

class DataFetcher(threading.Thread):
    def __init__(self, expiry_date, progress_data, lock, db_config, market_calendar):
        super().__init__()
        self.expiry_date = expiry_date
        self.progress_data = progress_data
        self.lock = lock
        self.db_config = db_config
        self.market_calendar = market_calendar
        self.daemon = True
        
        # Retrieving access token with enhanced error handling
        try:
            token_path = Path(ACCESS_TOKEN_FILE_PATH)
            if not token_path.exists():
                raise FileNotFoundError(
                    f"\nAccess token file not found at: {token_path}\n"
                    f"Please ensure you have:\n"
                    f"1. Created the directory: {token_path.parent}\n"
                    f"2. Created the file: {token_path.name}\n"
                    f"3. Added your Upstox API access token to the file"
                )
            
            with open(token_path, 'r') as file:
                self.access_token = validate_access_token(file.read())
                
            if not self.access_token:
                logging.warning("Access token is empty")
        
        except Exception as e:
            logging.error(f"Error reading access token: {e}")
            raise
        
    def run(self):
        while self.market_calendar.is_market_open():
            try:
                with self.lock:
                    self.progress_data[self.expiry_date] = {
                        'status': 'Fetching',
                        'last_update': datetime.now().strftime('%H:%M:%S'),
                        'records_count': 0
                    }
                
                # Prepare API request parameters
                params = {
                    'instrument_key': 'NSE_INDEX|Nifty 50',
                    'expiry_date': self.expiry_date
                }
                headers = {
                    'Accept': 'application/json',
                    'Authorization': f'Bearer {self.access_token}'
                }

                # Make API request
                response = requests.get('https://api.upstox.com/v2/option/chain', 
                                     params=params, headers=headers)
                data = response.json()

                if data.get('status') == 'success' and 'data' in data:
                    records_count = len(data['data'])
                    
                    # Process each record
                    for item in data['data']:
                        table_name = sanitize_table_name(self.expiry_date, item.get('underlying_key'))
                        insert_data_into_db(self.db_config, table_name, item)

                    with self.lock:
                        self.progress_data[self.expiry_date] = {
                            'status': 'Success',
                            'last_update': datetime.now().strftime('%H:%M:%S'),
                            'records_count': records_count
                        }
                
                t.sleep(1)  # 1-second delay between fetches
                
            except Exception as e:
                with self.lock:
                    self.progress_data[self.expiry_date] = {
                        'status': f'Error: {str(e)}',
                        'last_update': datetime.now().strftime('%H:%M:%S'),
                        'records_count': 0
                    }
                logging.error(f"Error in DataFetcher for {self.expiry_date}: {e}")
                t.sleep(1)  # Wait before retrying

class ProgressDisplay:
    def __init__(self):
        self.console = Console()
        
    def generate_table(self, progress_data):
        table = Table(show_header=True, header_style="bold magenta", 
                     title="Option Chain Data Fetching Progress")
        table.add_column("Expiry Date", style="cyan")
        table.add_column("Status", justify="center")
        table.add_column("Last Update", justify="center")
        table.add_column("Records", justify="right")
        
        for expiry, data in progress_data.items():
            status_color = {
                'Fetching': 'yellow',
                'Success': 'green'
            }.get(data['status'], 'red')
            
            table.add_row(
                str(expiry),
                f"[{status_color}]{data['status']}[/{status_color}]",
                data['last_update'],
                str(data['records_count'])
            )
        
        return table

def get_market_open_countdown():
    """Calculate time remaining until market opens"""
    now = datetime.now(pytz.timezone('Asia/Kolkata'))
    today = now.date()
    
    # Market open time (9:15 AM)
    market_open_time = datetime.combine(today, dt_time(9, 15)).replace(tzinfo=pytz.timezone('Asia/Kolkata'))
    
    # If current time is past market open, calculate for next day
    if now.time() > dt_time(15, 30):
        market_open_time += timedelta(days=1)
        while market_open_time.weekday() >= 6:  # Skip weekend days
            market_open_time += timedelta(days=1)
    
    # Check if market is closed due to holiday
    holiday_response = market_holiday_date_wise_safe()
    while holiday_response.get('status') == 'success' and \
          market_open_time.strftime('%Y-%m-%d') in [holiday['date'] for holiday in holiday_response.get('data', [])]:
        market_open_time += timedelta(days=1)
        while market_open_time.weekday() >= 6:  # Skip weekend days
            market_open_time += timedelta(days=1)
    
    time_to_market = market_open_time - now
    return time_to_market

def format_countdown(time_delta):
    """Format timedelta into human-readable countdown"""
    total_seconds = int(time_delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def market_holiday_date_wise_safe():
    """
    Safely fetch market holiday data with error handling and default return
    """
    try:
        holiday_response = market_holiday_date_wise()
        if holiday_response and holiday_response.get('status') == 'success':
            return holiday_response
        logging.warning("Failed to fetch holiday data, using empty list")
        return {'status': 'success', 'data': []}
    except Exception as e:
        logging.error(f"Error in market_holiday_date_wise: {e}")
        return {'status': 'success', 'data': []}

def main():
    threads = []
    try:
        if not Path(ACCESS_TOKEN_FILE_PATH).exists():
            rprint("[bold red]Error: Access token file not found.[/bold red]")
            rprint(f"[yellow]Please create the file at: {ACCESS_TOKEN_FILE_PATH}[/yellow]")
            rprint("[yellow]And add your Upstox API access token to it.[/yellow]")
            return
        # Initialize market calendar and expiry manager
        market_calendar = MarketCalendar()
        expiry_manager = ExpiryManager()

        # Get database configuration
        db_config = configDB()
        check_and_create_db(db_config)

        # Initialize progress display
        progress_display = ProgressDisplay()

        # Get the next 5 expiry dates
        expiry_dates = expiry_manager.get_next_n_expiries(5)
        logging.info(f"Found {len(expiry_dates)} expiry dates: {expiry_dates}")

        # Get current year and month
        now = datetime.now()

        rprint(f"[bold green]Found {len(expiry_dates)} expiry dates for {now.strftime('%B %Y')}:[/bold green]")
        for date in expiry_dates:
            rprint(f"[blue]• {date}[/blue]")

        # Wait for market to open with countdown
        while not market_calendar.is_market_open():
            countdown = get_market_open_countdown()
            rprint(f"[yellow]Time until market opens: {format_countdown(countdown)}[/yellow]")
            t.sleep(1)  # Update every second

        rprint("[bold green]Market is now open. Starting data collection.[/bold green]")

        # Initialize progress data
        progress_data = {
            str(date): {
                'status': 'Initializing',
                'last_update': '-',
                'records_count': 0
            } for date in expiry_dates
        }

        lock = threading.Lock()

        # Initialize threads
        for expiry_date in expiry_dates:
            thread = DataFetcher(
                expiry_date=expiry_date,
                progress_data=progress_data,
                lock=lock,
                db_config=db_config,
                market_calendar=market_calendar
            )
            threads.append(thread)
            thread.start()
            logging.info(f"Started thread for expiry date: {expiry_date}")

        # Display progress with Rich
        with Live(progress_display.generate_table(progress_data), refresh_per_second=1) as live:
            while market_calendar.is_market_open():
                if not any(thread.is_alive() for thread in threads):
                    rprint("[bold yellow]All threads have completed. Restarting threads...[/bold yellow]")
                    # Restart threads
                    threads.clear()  # Clear existing threads
                    for expiry_date in expiry_dates:
                        thread = DataFetcher(expiry_date, progress_data, lock, db_config, market_calendar)
                        thread.start()
                        threads.append(thread)

                live.update(progress_display.generate_table(progress_data))
                t.sleep(1)

            rprint("[bold red]Market is closed. Stopping data collection.[/bold red]")

    except KeyboardInterrupt:
        rprint("[bold red]Received keyboard interrupt. Shutting down...[/bold red]")
    except Exception as e:
        logging.error(f"Error in main: {e}")
        rprint(f"[bold red]Error: {str(e)}[/bold red]")
    finally:
        # Clean shutdown
        rprint("[yellow]Waiting for threads to complete...[/yellow]")
        for thread in threads:  # Now threads is guaranteed to be a list
            thread.join(timeout=1)
        rprint("[green]Script terminated successfully.[/green]")

if __name__ == '__main__':
    main()