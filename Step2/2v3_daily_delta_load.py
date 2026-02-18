import mysql.connector
import requests
from datetime import datetime

# --- 1. CONFIGURATION ---
MYSQL_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '',
    'database': 'CryptoBot_Step2'
}

BINANCE_URL = "https://api.binance.com/api/v3/ticker/price"
CMC_API_KEY = "9d2a04faa72a4d6295c495fa99ce63d1"
CMC_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"

# 8 Markets covering all strategic sectors
TARGET_MARKETS = ["BTCUSDT", "ETHUSDT", "ETHBTC", "BNBUSDT", "SOLUSDT", "ADAUSDT", "DOTUSDT", "USDCUSDT"]

# --- 2. API HELPER FUNCTIONS ---

def get_binance_price(symbol):
    """Fetches current price from Binance Ticker API"""
    try:
        response = requests.get(BINANCE_URL, params={"symbol": symbol.upper()})
        response.raise_for_status()
        return float(response.json()['price'])
    except Exception as e:
        print(f"Binance API Error ({symbol}): {e}")
        return None

def get_cmc_price(symbol):
    """Fetches current price from CoinMarketCap API"""
    mapping = {
        "BTCUSDT": "BTC", "ETHUSDT": "ETH", "ETHBTC": "ETH", 
        "BNBUSDT": "BNB", "SOLUSDT": "SOL", "ADAUSDT": "ADA", 
        "DOTUSDT": "DOT", "USDCUSDT": "USDC"
    }
    asset = mapping.get(symbol)
    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
    params = {"symbol": asset, "convert": "USD"}
    try:
        response = requests.get(CMC_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        return float(data['data'][asset]['quote']['USD']['price'])
    except Exception as e:
        print(f"CMC API Error ({symbol}): {e}")
        return None

# --- 3. DELTA INGESTION PROCESS ---

def run_delta_ingestion():
    print(f"--- Initializing Delta Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        for symbol in TARGET_MARKETS:
            # Get Pair_ID
            cursor.execute("SELECT Pair_ID FROM Pair WHERE Pair_Name = %s", (symbol,))
            result = cursor.fetchone()
            if not result:
                print(f"Skipping {symbol}: Not found in Master Data.")
                continue
            pair_id = result[0]

            # Ingest from both sources for dual-source validation
            for source, price in [("Binance", get_binance_price(symbol)), ("CMC", get_cmc_price(symbol))]:
                if price:
                    cursor.execute(
                        "INSERT INTO Price_Hist (Pair_ID, Price, Timestamp) VALUES (%s, %s, NOW())",
                        (pair_id, price)
                    )
                    print(f"{source.ljust(7)} -> {symbol}: {price}")

        conn.commit()
        print("Delta Ingestion successfully completed.")

    except Exception as e:
        print(f"Critical error during Delta Load: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    run_delta_ingestion()