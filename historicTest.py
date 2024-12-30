import os
import requests
import pandas as pd
import sqlite3
from SmartApi import SmartConnect
import pyotp
from loginCLI import *  # Custom module containing login credentials
import logging
import json
from datetime import datetime, timedelta
import ttkbootstrap as ttkb
from ttkbootstrap import Progressbar
import threading
from logzero import logger

# Set up logging configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize SmartConnect object
obj = SmartConnect(api_key=api_key)

# Generate session
data = obj.generateSession(user_name, password, pyotp.TOTP(totp_secret).now())
refreshToken = data['data']['refreshToken']

# Fetch the feedtoken
feedToken = obj.getfeedToken()
feed_token = feedToken

# Download and parse the scrip master file
def download_scrip_master(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        scrip_master_data = response.json()
        scrip_master_df = pd.DataFrame(scrip_master_data)
        scrip_master_df.to_csv("temp.HISTORIC.OpenAPIScripMaster.csv", index=False)
        logging.info("Scrip master file downloaded and saved as temp.HISTORIC.OpenAPIScripMaster.csv")
        return scrip_master_df
    except requests.RequestException as e:
        logging.error(f"Failed to download scrip master file: {e}")
        return None

# Function to fetch OHLC historical data
def OHLCData(symbol, token, interval, fdate, tdate):
    try:
        historicParam = {
            "exchange": 'NSE',
            "tradingsymbol": symbol,
            "symboltoken": token,
            "interval": interval,
            "fromdate": fdate,
            "todate": tdate
        }
        history = obj.getCandleData(historicParam)['data']
        history = pd.DataFrame(history)
        history = history.rename(columns={0: "Datetime", 1: "open", 2: "high", 3: "low", 4: "close", 5: "Volume"})
        history['Datetime'] = pd.to_datetime(history['Datetime'])
        
        # Separate "Datetime" into "date" and "time"
        history['date'] = history['Datetime'].dt.date
        history['time'] = history['Datetime'].dt.time
        
        # Set "exch_seg" as index
        history = history.set_index(pd.Series([symbol] * len(history), name='exch_seg'))
        
        return history
    except Exception as e:
        logging.error(f"Historic API failed: {e}")
        return None

# Function to fetch one year of data in intervals
def fetch_one_year_data(symbol, token, interval, progress_var, status_label, progress_callback):
    max_days = 30
    start_date = datetime(2024, 8, 29)  # Define the start date
    end_date = datetime(2024, 8, 31)  # Define the end date
    
    data_frames = []
    step_count = 0
    total_steps = (end_date - start_date).days // max_days + 1

    while start_date < end_date:
        fdate = start_date.strftime("%Y-%m-%d 09:00")
        tdate = (start_date + timedelta(days=max_days)).strftime("%Y-%m-%d 16:00")
        
        status_label.config(text=f"Fetching data for {symbol} from {fdate} to {tdate}...")
        logging.info(f"Fetching data from {fdate} to {tdate}...")

        # Check if data already exists
        if not data_exists(symbol, "NSE", interval, fdate, tdate):
            ohlc_data = OHLCData(symbol, token, interval, fdate, tdate)
            if ohlc_data is not None:
                data_frames.append(ohlc_data)
        
        start_date += timedelta(days=max_days)
        step_count += 1
        progress_callback((step_count / total_steps) * 100)

    if data_frames:
        return pd.concat(data_frames)
    else:
        return None

# Store data in SQLite3 database
def store_data_in_db(data, symbol, exchange_segment, interval):
    db_name = f"{exchange_segment}_OHLC_{interval}.db"
    try:
        with sqlite3.connect(db_name) as conn:
            table_name = symbol.replace("-", "_")
            data.to_sql(table_name, conn, if_exists='replace', index=True)
        logging.info(f"Data for {symbol} stored in {db_name} database.")
    except Exception as e:
        logging.error(f"Failed to store data in database: {e}")

# Function to check if data already exists in the database
def data_exists(symbol, exchange_segment, interval, fdate, tdate):
    db_name = f"{exchange_segment}_OHLC_{interval}.db"
    table_name = symbol.replace("-", "_")
    query = f"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{table_name}'"

    try:
        with sqlite3.connect(db_name) as conn:
            cur = conn.cursor()
            cur.execute(query)
            if cur.fetchone():
                # Table exists, now check for the date range
                query = f"SELECT 1 FROM {table_name} WHERE Datetime BETWEEN ? AND ? LIMIT 1"
                cur.execute(query, (fdate, tdate))
                if cur.fetchone():
                    logging.info(f"Data for {symbol} from {fdate} to {tdate} already exists in the database.")
                    return True
    except Exception as e:
        logging.error(f"Failed to check data in database: {e}")
    
    return False

# Retrieve tokens for symbols ending with '-EQ'
def get_symbol_tokens(scrip_master_df):
    tokens = {}
    for _, row in scrip_master_df.iterrows():
        symbol = row['symbol']
        if symbol.endswith('-EQ'):
            tokens[symbol] = row['token']
    return tokens

# Progress dialog
def progress_dialog():
    app = ttkb.Window(themename="darkly")
    app.title("Historic Data Recorder")
    app.geometry("500x100")

    # Make the window non-resizable
    app.resizable(False, False)

    status_label = ttkb.Label(app, text="Starting...")
    status_label.pack(pady=10)

    progress_var = ttkb.DoubleVar()
    progress = Progressbar(app, bootstyle="danger-striped", length=400, variable=progress_var)
    progress.pack(pady=20)

    exit_button = ttkb.Button(app, text="Ok to EXIT", bootstyle="success", command=app.destroy)
    exit_button.pack(pady=10)
    exit_button.pack_forget()  # Initially hide the button

    return app, progress_var, status_label, exit_button

# Main execution
def main():
    scrip_master_df = download_scrip_master("https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json")
    if scrip_master_df is not None:
        symbol_tokens = get_symbol_tokens(scrip_master_df)
        symbols = [row['symbol'] for _, row in scrip_master_df.iterrows() if row['symbol'].endswith('-EQ')]
        total_symbols = len(symbols)
        
        app, progress_var, status_label, exit_button = progress_dialog()  # Unpack four values
        app.update_idletasks()  # Ensure the progress dialog is visible
        
        def update_progress(progress_var, status_label, total_symbols):
            def progress_callback(progress):
                progress_var.set(progress)
                app.update_idletasks()
            
            for i, symbol in enumerate(symbols):
                exchange_segment = scrip_master_df[scrip_master_df['symbol'] == symbol]['exch_seg'].values[0]
                token = symbol_tokens.get(symbol, None)
                if token:
                    logging.info(f"Fetching OHLC data for {symbol} for one year...")
                    ohlc_data = fetch_one_year_data(symbol, token, "ONE_MINUTE", progress_var, status_label, progress_callback)
                    if ohlc_data is not None:
                        store_data_in_db(ohlc_data, symbol, exchange_segment, "ONE_MINUTE")
                progress_callback((i + 1) / total_symbols * 100)
            
            progress_var.set(100)  # Ensure progress is set to 100% at the end
            app.update_idletasks()
            exit_button.pack()  # Show the exit button when done
            app.quit()  # Close the dialog when done
        
        threading.Thread(target=update_progress, args=(progress_var, status_label, total_symbols)).start()
        app.mainloop()

if __name__ == "__main__":
    main()
