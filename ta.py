import logging
from logging.handlers import RotatingFileHandler
import psycopg2
import pandas as pd
import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
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

# Test configDB function
print("Testing configDB function...")
db_config = configDB()
print("Database Config:", db_config)
print("configDB function test complete.\n")

# Connect to PostgreSQL database and fetch data
def connect(table_name, limit=5000):
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

# Filter DataFrame to include only market hours and remove gaps
def filter_data(df):
    # Ensure the 'timestamp' column is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Define market hours
    df['time'] = df['timestamp'].dt.time
    df = df[(df['time'] >= pd.to_datetime('09:15').time()) & (df['time'] <= pd.to_datetime('15:30').time())]
    
    # Remove rows where all columns are NaN
    df = df.dropna(how='all')
    
    return df

# Dash App
app = dash.Dash(__name__)

# Layout of the Dash App
app.layout = html.Div([
    html.H1("Candlestick Chart from PostgreSQL Database"),
    
    # Dropdown to select table
    dcc.Dropdown(
        id='table-dropdown',
        options=[{'label': table, 'value': table} for table in get_table_names()],
        placeholder="Select a table",
    ),

    # Graph to display the candlestick chart
    dcc.Graph(id='candlestick-chart', style={'width': '100%', 'height': '700px', 'overflowX': 'scroll'})
])

# Callback to update the chart based on selected table
@app.callback(
    Output('candlestick-chart', 'figure'),
    [Input('table-dropdown', 'value')]
)
def update_chart(table_name):
    if table_name:
        # Fetch the data from the selected table with a limit
        df = connect(table_name, limit=5000)

        # Ensure the DataFrame has necessary columns for candlestick chart
        if df is not None and all(col in df.columns for col in ['timestamp', 'open_price', 'high_price', 'low_price', 'close_price']):
            # Filter the DataFrame to include only market hours and remove gaps
            df = filter_data(df)
            
            # Create the candlestick chart using Plotly
            fig = go.Figure(data=[go.Candlestick(x=df['timestamp'],
                                                 open=df['open_price'],
                                                 high=df['high_price'],
                                                 low=df['low_price'],
                                                 close=df['close_price'])])
            
            # Update layout for better visibility and horizontal scrolling
            fig.update_layout(
                title=f'Candlestick Chart for {table_name}',
                xaxis_title='Timestamp',
                yaxis_title='Price',
                xaxis_rangeslider_visible=False,  # Disable the default rangeslider
                xaxis={
                    'range': [df['timestamp'].min(), df['timestamp'].max()],  # Adjust range to fit the data
                    'rangeslider': {'visible': True},  # Custom rangeslider to allow scrolling
                    'type': 'date',
                },
                autosize=True,  # Make the chart responsive
                height=700,  # Height of the chart
                margin=dict(l=50, r=50, t=50, b=50),  # Margins for the chart
            )
            return fig
        else:
            return go.Figure()  # Return empty figure if columns are missing
    return go.Figure()  # Return empty figure initially

# Run the app
if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)  # Disable reloader to prevent performance issues during development
