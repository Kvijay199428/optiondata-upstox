# loginCLI.py

from pathlib import Path
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
from api.api import (apikey_order, apisecret_order, apikey_oc, apisecret_oc, 
                     apikey_his, apisecret_his, apikey_latency, apisecret_latency,
                     redirect_url, totp_secret, mobile_no, pin, 
                     nse, bse, mcx, complete, suspended)

BASE_DIR = Path(__file__).parent
TOKEN_DIR = BASE_DIR / "api" / "token"
CONFIG_DIR = BASE_DIR / "api" / "ini"
LOG_DIR = BASE_DIR / "api" / "logs"
INSTRUMENT_DIR = BASE_DIR / "api" / "instrument"

# Ensure required directories exist
def ensure_directories():
    for directory in [TOKEN_DIR, INSTRUMENT_DIR, CONFIG_DIR, LOG_DIR]:
        directory.mkdir(parents=True, exist_ok=True)

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
    token_path = TOKEN_DIR / token_filename  # Using Path operator
    
    with open(token_path, 'w') as file:
        file.write(jsr['access_token'])
    print(f"Access Token saved to {token_path}")

# Perform login and get authorization code
def perform_login_process():
    auth_url = f'https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={apikey_his}&redirect_uri={redirect_url}'
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--headless')
    options.add_argument('--disable-software-rasterizer') 
    options.add_argument('--log-level=3')  # Suppress most logging

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
            print_user_profile(user_data)
        else:
            print(Fore.RED + "❌ Failed to retrieve user profile." + Style.RESET_ALL)
    except ApiException as e:
        print(Fore.RED + f"⚠️ API exception: {e}" + Style.RESET_ALL)

# Print user profile with formatting
def print_user_profile(user_data):
    print(Fore.GREEN + "\n🎉 Login Successful!" + Style.RESET_ALL)
    print(Fore.CYAN + "="*30 + Style.RESET_ALL)
    print(f"{Fore.YELLOW}📧 Email: {Style.BRIGHT}{user_data.email}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}🏦 Broker: {Style.BRIGHT}{user_data.broker}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}🆔 Client ID: {Style.BRIGHT}{user_data.user_id}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}🆔 Name: {Style.BRIGHT}{user_data.user_name}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}🔖 Type: {Style.BRIGHT}{user_data.user_type}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}{'✅' if user_data.is_active else '🔴'} Active: {Style.BRIGHT}{user_data.is_active}{Style.RESET_ALL}")
    
    print(Fore.CYAN + "="*30 + Style.RESET_ALL)

# Process tokens from the directory and fetch profiles
def process_tokens():
    # Fixed: Use Path's glob method or convert to string and use proper path joining
    token_files = list(TOKEN_DIR.glob('*.txt'))  # Using Path's glob method
    if not token_files:
        print(Fore.RED + "No tokens found in the directory." + Style.RESET_ALL)
        return

    for token_file in token_files:
        with open(token_file, 'r') as file:
            access_token = file.read().strip()
            print(Fore.CYAN + f"\n🔑 Processing token from {token_file.name}" + Style.RESET_ALL)
            get_user_profile(access_token)

# Download and parse instrument JSONs
def download_instruments():
    today = datetime.now().strftime("%Y-%m-%d")
    instrument_files = {"nse": "nse.json.gz", "bse": "bse.json.gz", "mcx": "mcx.json.gz", "complete": "complete.json.gz", "suspended": "suspended.json.gz"}
    instrument_links = {"nse": nse, "bse": bse, "mcx": mcx, "complete": complete, "suspended": suspended}

    for instrument, file_name in instrument_files.items():
        file_path = INSTRUMENT_DIR / file_name  # Using Path operator
        if not file_path.exists() or today != get_file_date(file_path):
            print(f"Downloading {instrument} instrument data...")
            download_and_extract(instrument_links[instrument], str(file_path))  # Convert Path to string


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
        'OptionChain': 'OptionData'
    }
    config = ConfigParser()

    for db_name, db_file in databases.items():
        config['postgresql'] = {
            'host': 'aws-postgresql-database-optiondata.c16gu26gwzeq.ap-south-1.rds.amazonaws.com',
            'database': db_file,
            'user': 'postgres',
            'password': 'Vijay280801994',
            'port': '5432'
        }
        file_path = CONFIG_DIR / f'{db_name}.ini'  # Using Path operator
        with open(file_path, 'w') as configfile:
            config.write(configfile)
# Main function
def main():
    ensure_directories()

    # Login process for multiple services
    services = [
        # {"client_id": apikey_his, "client_secret": apisecret_his, "token_filename": "accessToken_his.txt"},
        {"client_id": apikey_oc, "client_secret": apisecret_oc, "token_filename": "accessToken_oc.txt"},
        # {"client_id": apikey_order, "client_secret": apisecret_order, "token_filename": "accessToken_order.txt"},
        # {"client_id": apikey_latency, "client_secret": apisecret_latency, "token_filename": "accessToken_latency.txt"}
    ]

    for service in services:
        code = perform_login_process()
        if code:
            generate_access_token(code, service["client_id"], service["client_secret"], service["token_filename"])

    process_tokens()
    # download_instruments()
    # create_ini_files()

if __name__ == "__main__":
    main()
