import mysql.connector
from binance.client import Client
import pandas as pd

# --- 1. CONFIGURATION ---
MYSQL_CONFIG = {
    'host': '127.0.0.1', 
    'user': 'root', 
    'password': '', 
    'database': 'CryptoBot_Step2'
}

# Binance API Credentials
BINANCE_API_KEY = 'YOUR_BINANCE_KEY'
BINANCE_API_SECRET = 'YOUR_BINANCE_SECRET'

# Initialize Binance Client
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

# All 8 Target Markets (Including USDC for the Stablecoin category)
symbols = ["BTCUSDT", "ETHUSDT", "ETHBTC", "BNBUSDT", "SOLUSDT", "ADAUSDT", "DOTUSDT", "USDCUSDT"]

def ingest_deep_history():
    try:
        # Establish connection to MySQL
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # STEP 1: RESET - Clear old history to avoid duplicates during bulk load
        print("Cleaning local MySQL history (Truncate)...")
        cursor.execute("TRUNCATE TABLE Price_Hist")
        conn.commit()
        
        # STEP 2: FETCH HISTORY FROM BINANCE
        # We fetch daily values (1DAY) to maintain high performance in Snowflake/Power BI
        for symbol in symbols:
            print(f"Fetching history for {symbol} (Daily intervals starting 01.01.2024)...")
            
            # Fetch historical klines from Binance API
            klines = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1DAY, "1 Jan, 2024")
            
            # Convert to DataFrame for easier processing
            df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 
                                               'close_time', 'qav', 'num_trades', 'taker_base', 'taker_quote', 'ignore'])
            
            # Convert timestamp (ms) to readable format
            df['TIME_ID'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # Retrieve Pair_ID from Master Data table
            cursor.execute("SELECT Pair_ID FROM Pair WHERE Pair_Name = %s", (symbol,))
            result = cursor.fetchone()
            
            if result:
                pair_id = result[0]
                
                # Bulk Insert into MySQL
                for _, row in df.iterrows():
                    sql = "INSERT IGNORE INTO Price_Hist (Pair_ID, Price, Timestamp) VALUES (%s, %s, %s)"
                    cursor.execute(sql, (pair_id, row['close'], row['TIME_ID']))
                
                conn.commit()
                print(f"-> {symbol}: {len(df)} data points successfully imported.")
            else:
                print(f"-> WARNING: Symbol {symbol} not found in 'Pair' table. Run Step 2v1 first!")

        print("\nSUCCESS: Deep history since 2024 stored in MySQL.")

    except Exception as e:
        print(f"Critical error during bulk import: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    ingest_deep_history()