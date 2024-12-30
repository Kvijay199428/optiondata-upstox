import sys
import os
import glob
import gzip
import json
import shutil
import pyotp
import requests as rq
from time import sleep
from configparser import ConfigParser
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import upstox_client
from upstox_client.rest import ApiException
from colorama import Fore, Style
import PySimpleGUI as sg
from api import (apikey_order, apisecret_order, apikey_oc, apisecret_oc, apikey_his, 
                 apisecret_his, redirect_url, totp_secret, mobile_no, pin, 
                 nse, bse, mcx, complete, suspended)

# # Directory paths
# token_dir = "api/token/"
# instrument_dir = "api/instrument/"
# ini_dir = "api/ini/"
# logs_dir = "api/logs/"

# # Ensure required directories exist
# def ensure_directories():
#     for directory in [token_dir, instrument_dir, ini_dir, logs_dir]:
#         if not os.path.exists(directory):
#             os.makedirs(directory)

# Function to ensure paths are correctly located in executable
def resource_path(relative_path):
    """Get the absolute path to the resource, works for dev and for PyInstaller."""
    try:
        # PyInstaller creates a temp folder and stores the path in _MEIPASS
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)

# Directory paths (using resource_path)
token_dir = resource_path("api/token/")
instrument_dir = resource_path("api/instrument/")
ini_dir = resource_path("api/ini/")
logs_dir = resource_path("api/logs/")

# Ensure required directories exist
def ensure_directories():
    """Ensure all required directories exist. If not, create them."""
    for directory in [token_dir, instrument_dir, ini_dir, logs_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)

# Example usage
ensure_directories()

# Generate and save access token
def generate_access_token(code, client_id, client_secret, token_filename):
    url = 'https://api-v2.upstox.com/login/authorization/token'
    headers = {'accept': 'application/json', 'Api-Version': '2.0', 'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'code': code,
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': redirect_url,
        'grant_type': 'authorization_code'
    }
    
    response = rq.post(url, headers=headers, data=data)
    jsr = response.json()
    token_path = os.path.join(token_dir, token_filename)
    
    with open(token_path, 'w') as file:
        file.write(jsr['access_token'])
    return token_path

# Perform login and get authorization code
def perform_login_process():
    auth_url = f'https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={apikey_his}&redirect_uri={redirect_url}'
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    #options.add_argument('--incognito')
    options.add_argument('--headless')
    
    driver_dir = os.path.join('api', 'chromedriver.exe')
    service = ChromeService(service=driver_dir)
    driver = webdriver.Chrome(service=service, options=options)
    
    driver.get(auth_url)

    try:
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.XPATH, '//input[@type="text"]'))).send_keys(mobile_no)
        wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="getOtp"]'))).click()
        
        # Enter TOTP and continue
        totp = pyotp.TOTP(totp_secret).now()
        sleep(1)
        wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="otpNum"]'))).send_keys(totp)
        wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="continueBtn"]'))).click()
        
        # Enter PIN and continue
        sleep(1)
        wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="pinCode"]'))).send_keys(pin)
        wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="pinContinueBtn"]'))).click()
        
        # Get the authorization code from URL
        sleep(1)
        token_url = driver.current_url
        code = urlparse(token_url).query
        code = dict(x.split('=') for x in code.split('&')).get('code')

        return code
    except Exception as e:
        print(f"Login error: {e}")
        return None
    finally:
        driver.quit()

# Get user profile using access token
def get_user_profile(access_token):
    configuration = upstox_client.Configuration()
    configuration.access_token = access_token
    api_instance = upstox_client.UserApi(upstox_client.ApiClient(configuration))

    try:
        api_response = api_instance.get_profile(api_version='2.0')
        if api_response.status == "success":
            user_data = api_response.data
            return user_data
        else:
            print(Fore.RED + "❌ Failed to retrieve user profile." + Style.RESET_ALL)
    except ApiException as e:
        print(Fore.RED + f"⚠️ API exception: {e}" + Style.RESET_ALL)
    return None

# Print user profile with formatting
def format_user_profile(user_data):
    profile_info = [
        f"📧 Email: {user_data.email}",
        f"🏦 Broker: {user_data.broker}",
        f"🆔 Client ID: {user_data.user_id}",
        f"🆔 Name: {user_data.user_name}",
        f"🔖 Type: {user_data.user_type}",
        f"{'✅' if user_data.is_active else '🔴'} Active: {user_data.is_active}"
    ]
    return "\n".join(profile_info)

# Process tokens from the directory and fetch profiles
def process_tokens():
    token_files = glob.glob(token_dir + '*.txt')
    if not token_files:
        return "No tokens found in the directory."

    profile_data = ""
    for token_file in token_files:
        with open(token_file, 'r') as file:
            access_token = file.read().strip()
            user_data = get_user_profile(access_token)
            if user_data:
                profile_data += format_user_profile(user_data) + "\n\n"
    return profile_data if profile_data else "No user profiles found."

# Download and parse instrument JSONs
def download_instruments():
    today = datetime.now().strftime("%Y-%m-%d")
    instrument_files = {
        "nse": "nse.json.gz",
        "bse": "bse.json.gz",
        "mcx": "mcx.json.gz",
        "complete": "complete.json.gz",
        "suspended": "suspended.json.gz"
    }
    instrument_links = {
        "nse": nse,
        "bse": bse,
        "mcx": mcx,
        "complete": complete,
        "suspended": suspended
    }

    for instrument, file_name in instrument_files.items():
        file_path = os.path.join(instrument_dir, file_name)
        if not os.path.exists(file_path) or today != get_file_date(file_path):
            print(f"Downloading {instrument} instrument data...")
            download_and_extract(instrument_links[instrument], file_path)

# Download and extract .json.gz files
def download_and_extract(url, output_file):
    try:
        response = rq.get(url, stream=True)
        with open(output_file, 'wb') as f_out:
            shutil.copyfileobj(response.raw, f_out)

        with gzip.open(output_file, 'rb') as f_in:
            json_content = json.loads(f_in.read().decode('utf-8'))
            json_file = output_file.replace('.gz', '')
            with open(json_file, 'w') as json_out:
                json.dump(json_content, json_out, indent=4)
        print(f"Extracted {output_file} to {json_file}")
        os.remove(output_file)
    except Exception as e:
        print(f"Error downloading or extracting {output_file}: {e}")

# Get file modification date
def get_file_date(file_path):
    modified_time = os.path.getmtime(file_path)
    return datetime.fromtimestamp(modified_time).strftime("%Y-%m-%d")

# Create .ini configuration files
def create_ini_files():
    databases = {
        'NSE': 'historicalNSE-EQ-1min',
        'BSE': 'historicalBSE-EQ-1min',
        'MCX': 'historicalMCX-1min',
        'NFO': 'historicalNFO-1min',
        'INDEX': 'historicalINDEX-1min',
        'OptionChain': 'optionChain'
    }
    config = ConfigParser()

    for db_name, db_file in databases.items():
        config['postgresql'] = {
            'host': 'localhost',
            'database': db_file,
            'user': 'postgres',
            'password': 'admin',
            'port': '5432'
        }
        file_path = os.path.join(ini_dir, f'{db_name}.ini')
        with open(file_path, 'w') as configfile:
            config.write(configfile)

# Main function
def main():
    ensure_directories()

    # Login process for multiple services
    services = [
        {"client_id": apikey_oc, "client_secret": apisecret_oc, "token_filename": "accessToken_OC.txt"},
        {"client_id": apikey_his, "client_secret": apisecret_his, "token_filename": "accessToken_his.txt"},
        {"client_id": apikey_order, "client_secret": apisecret_order, "token_filename": "accessToken_order.txt"},
        # Add other services here in the same format
    ]
    
    sg.theme('DarkAmber')
    layout = [
        [sg.Text('Click to start login process')],
        [sg.Button('Login'), sg.Button('Exit')],
        [sg.Multiline(size=(50, 20), key='-OUTPUT-', disabled=True)],
    ]
    
    window = sg.Window('Login TMA', layout)

    while True:
        event, values = window.read()
        if event in (sg.WIN_CLOSED, 'Exit'):
            break
        if event == 'Login':
            for service in services:
                code = perform_login_process()
                if code:
                    token_path = generate_access_token(code, service['client_id'], service['client_secret'], service['token_filename'])
                    window['-OUTPUT-'].update(f"Token saved to: {token_path}\n")
                else:
                    window['-OUTPUT-'].update("Failed to login.\n")
            
            # Process tokens and download instruments
            profile_data = process_tokens()
            window['-OUTPUT-'].update(f"{profile_data}\n")
            download_instruments()
            create_ini_files()

    window.close()

if __name__ == '__main__':
    main()
