# this script automates the ETL process 

import mysql.connector
import requests
import pandas as pd
from datetime import datetime
 


# --- 1. Generic function from Step1 to fetch latest price from Binance API
def get_binance_data(endpoint="/api/v3/ticker/price", symbol=None):
    base_url = "https://api.binance.com"
    url = f"{base_url}{endpoint}"
    params = {}
    if symbol:
        params['symbol'] = symbol.upper()
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

# --- 2. Local MySQL Connection ---
db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '',
    'database': 'CryptoBot_Step2'
}

# --- 3. Functions to ingest data ---

# Map asset types  
STABLECOINS = {"USDT", "USDC", "BUSD"}
MAJOR_COINS = {"BTC", "ETH", "BNB", "SOL"}

def get_asset_type(currency: str) -> str:
    currency = currency.upper().strip()  # normalize
    if currency in STABLECOINS:
        return "stablecoin"
    if currency in MAJOR_COINS:
        return "crypto"
    return "token"

# Split trading pair into base and quote currency (BTCUSDT → BTC, USDT)
def split_symbol(symbol):
    known_quotes = ["USDT", "BTC", "ETH", "BNB"]
    for quote in known_quotes:
        if symbol.endswith(quote):
            base = symbol.replace(quote, "")
            return base.upper().strip(), quote.upper().strip()
    raise ValueError(f"Unknown quote currency in {symbol}")


# Ensure currency exists, return its ID & asset type
def get_currency_id(cursor, currency_name):
    asset_type = get_asset_type(currency_name)

    cursor.execute("""
        INSERT IGNORE INTO Currency (Currency_Name, Asset_Type)
        VALUES (%s, %s)
    """, (currency_name, asset_type))

    cursor.execute(
        "SELECT Currency_ID FROM Currency WHERE Currency_Name=%s",
        (currency_name,)
    )
    return cursor.fetchone()[0]

# Ensure trading pair exists and return its ID
def get_pair_id(cursor, symbol, base_id, quote_id):
    cursor.execute("""
        INSERT IGNORE INTO Pair (Pair_Name, Base_Currency_ID, Quote_Currency_ID)
        VALUES (%s, %s, %s)
    """, (symbol, base_id, quote_id))

    cursor.execute("SELECT Pair_ID FROM Pair WHERE Pair_Name=%s", (symbol,))
    return cursor.fetchone()[0]

# run ingestion
def run_ingestion():
    target_markets = ["BTCUSDT", "ETHUSDT", "ETHBTC", "BNBUSDT", "SOLUSDT"]

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        for symbol in target_markets:
            data = get_binance_data(symbol=symbol)
            if not data:
                continue

            price = float(data['price'])
            retrieved_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Split symbol into base/quote currencies
            base, quote = split_symbol(symbol)

            # Get or create currency IDs
            base_id = get_currency_id(cursor, base)
            quote_id = get_currency_id(cursor, quote)

            # Get or create pair ID
            pair_id = get_pair_id(cursor, symbol, base_id, quote_id)

            # Insert price snapshot
            cursor.execute("""
                INSERT INTO Price_Hist (Price, Timestamp, Pair_ID)
                VALUES (%s, %s, %s)
            """, (price, retrieved_at, pair_id))

            print(f"Saved: {symbol} -> {price}")

        conn.commit()
        print("\nETL finished successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()


if __name__ == "__main__":
    run_ingestion()