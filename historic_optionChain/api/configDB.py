# historic_optionChain\api\configDB.py

import os
import psycopg2
from psycopg2 import sql
from configparser import ConfigParser
import json

def configDB(filename="historical.ini", section="postgresql"):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in {filename} file.')
    return db

def check_and_create_db(db_config):
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
            print(f"Database '{db_config['database']}' does not exist. Creating it...")
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_config['database'])))
            print(f"Database '{db_config['database']}' created successfully.")
        else:
            print(f"Database '{db_config['database']}' already exists.")
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()

def create_table(conn, table_name):
    cur = conn.cursor()
    cur.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            timestamp TIMESTAMP,
            open_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            close_price FLOAT,
            volume INTEGER,
            open_interest INTEGER,
            PRIMARY KEY (timestamp)
        )
    """).format(sql.Identifier(table_name)))
    conn.commit()
    cur.close()
    print(f"Table '{table_name}' created or already exists.")

def main():
    # Step 1: Select JSON file
    json_directory = 'instruments'
    files = [f for f in os.listdir(json_directory) if f.endswith('.json')]
    
    if not files:
        print("No JSON files found.")
        return
    
    print("Available JSON files:")
    for idx, file in enumerate(files):
        print(f"{idx + 1}: {file}")
    
    file_choice = input("Enter the number corresponding to your choice: ")
    selected_file = files[int(file_choice) - 1]
    file_path = os.path.join(json_directory, selected_file)
    
    # Step 2: Load and filter JSON data
    with open(file_path, 'r') as f:
        instruments_data = json.load(f)
    
    criteria = {
        "segment": "NSE_EQ",
        "instrument_type": "EQ"
    }

    filtered_instruments = [instr for instr in instruments_data 
                            if instr.get("segment") == criteria["segment"] 
                            and instr.get("instrument_type") == criteria["instrument_type"]]

    if not filtered_instruments:
        print("No instruments match the criteria.")
        return
    
    # Step 3: Use trading_symbol as table name
    for instrument in filtered_instruments:
        table_name = instrument["trading_symbol"]
        print(f"Using '{table_name}' as table name.")

        # Step 4: Check if database exists, then connect
        db_config = configDB()
        check_and_create_db(db_config)

        # Step 5: Connect to the specified database
        conn = psycopg2.connect(**db_config)

        # Step 6: Create table if it doesn't exist
        create_table(conn, table_name)
        
        # Close connection
        conn.close()

if __name__ == "__main__":
    main()
