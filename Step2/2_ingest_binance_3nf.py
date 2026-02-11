import mysql.connector
import requests
import pandas as pd
from datetime import datetime

# --- 1. Deine generische Funktion --- [cite: 3]
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

# --- 2. Datenbank-Konfiguration ---
db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '',
    'database': 'CryptoBot_Step2'
}

def run_ingestion():
    target_markets = ["BTCUSDT", "ETHUSDT", "ETHBTC", "BNBUSDT", "SOLUSDT"]
    
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        for symbol in target_markets:
            data = get_binance_data(symbol=symbol)
            if data:
                price = float(data['price'])
                retrieved_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # A. Currency Logik (Wir extrahieren den Namen aus dem Symbol, z.B. BTC) [cite: 9, 21-23]
                base_asset = symbol.replace("USDT", "").replace("BTC", "") if "ETHBTC" not in symbol else "ETH"
                cursor.execute("""
                    INSERT IGNORE INTO Currency (Currency_Name, Asset_Type) 
                    VALUES (%s, %s)
                """, (base_asset, 'Crypto'))
                conn.commit()

                cursor.execute("SELECT Currency_ID FROM Currency WHERE Currency_Name = %s", (base_asset,))
                curr_id = cursor.fetchone()[0]

                # B. Pair Logik [cite: 10, 29]
                cursor.execute("""
                    INSERT IGNORE INTO Pair (Pair_Name, Currency_ID) 
                    VALUES (%s, %s)
                """, (symbol, curr_id))
                conn.commit()

                cursor.execute("SELECT Pair_ID FROM Pair WHERE Pair_Name = %s", (symbol,))
                pair_id = cursor.fetchone()[0]

                # C. Price_Hist Logik [cite: 11, 30-31]
                cursor.execute("""
                    INSERT INTO Price_Hist (Price, Timestamp, Pair_ID) 
                    VALUES (%s, %s, %s)
                """, (price, retrieved_at, pair_id))
                
                print(f"Gespeichert: {symbol} -> {price} €/$)")

        conn.commit()
        print("\nAlle Daten erfolgreich in die 3NF-Struktur eingepflegt.")

    except Exception as e:
        print(f"Fehler: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    run_ingestion()