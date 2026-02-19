import mysql.connector
import requests
from datetime import datetime

# --- 1. CONFIGURATION ---
CMC_API_KEY = "9d2a04faa72a4d6295c495fa99ce63d1"
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '',
    'database': 'CryptoBot_Step2'
}

CATEGORY_MAP = {
    "DeFi (Decentralized Finance)": ["defi", "decentralized-finance"],
    "Stablecoins": ["stablecoin", "asset-backed-stablecoin", "fiat-stablecoin"],
    "NFTs (Non-Fungible Tokens)": ["nft", "collectibles", "art"],
    "Gaming & Metaverse": ["gaming", "metaverse", "play-to-earn"],
    "Exchange Tokens": ["centralized-exchange", "exchange-based-tokens"],
    "Layer 1 & Layer 2": ["layer-1", "layer-2", "smart-contracts"],
    "Privacy Coins": ["privacy"],
    "AI & Big Data": ["ai-big-data", "big-data"]
}

# --- 2. DATA FETCHING FUNCTIONS ---

def get_binance_data(symbol):
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol.upper()}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching Binance data for {symbol}: {e}")
        return None

def get_cmc_metadata(symbols):
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
    params = {"symbol": ",".join(symbols), "convert": "USD"}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json().get('data', {})
    except Exception as e:
        print(f"CMC API Error: {e}")
        return {}

def map_tags_to_8_categories(raw_tags):
    if not raw_tags: return "To be defined"
    matched = []
    tags_lower = [t.lower() for t in raw_tags]
    for pretty_name, keywords in CATEGORY_MAP.items():
        if any(kw in tags_lower for kw in keywords):
            matched.append(pretty_name)
    return ", ".join(matched) if matched else "Other / Not Mapped"

def get_asset_type(currency):
    stablecoins = {"USDT", "USDC", "BUSD"}
    major_coins = {"BTC", "ETH", "BNB", "SOL"}
    if currency.upper() in stablecoins: return "stablecoin"
    if currency.upper() in major_coins: return "crypto"
    return "token"

# --- 3. MAIN INGESTION PROCESS ---

def run_ingestion():
    # Full list of 8 pairs
    target_markets = ["BTCUSDT", "ETHUSDT", "ETHBTC", "BNBUSDT", 
                      "SOLUSDT", "ADAUSDT", "DOTUSDT", "USDCUSDT"]
    
    base_assets = ["BTC", "ETH", "BNB", "SOL", "ADA", "DOT", "USDC", "USDT"]

    print("Step 1: Fetching global metadata from CoinMarketCap...")
    cmc_info = get_cmc_metadata(base_assets)

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for symbol in target_markets:
            # A. Fetch Binance Price
            data = get_binance_data(symbol)
            if not data: continue
            price = float(data['price'])
            
            # B. Split Symbol
            if symbol.endswith("USDT"):
                base, quote = symbol.replace("USDT", ""), "USDT"
            elif symbol.endswith("BTC"):
                base, quote = symbol.replace("BTC", ""), "BTC"
            else:
                base, quote = symbol[:3], symbol[3:]

            # C. NEW ENRICHMENT LOGIC: Process BOTH Base and Quote
            # This loop ensures USDT (quote) gets price/tags just like BTC (base)
            for curr in [base, quote]:
                asset_data = cmc_info.get(curr, {})
                raw_tags = asset_data.get('tags', [])
                global_usd_price = asset_data.get('quote', {}).get('USD', {}).get('price')
                
                # Update Currency table for this specific asset
                cursor.execute("""
                    INSERT INTO Currency (Currency_Name, Asset_Type, CMC_Global_Price_USD, CMC_Raw_Tags)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        CMC_Global_Price_USD = VALUES(CMC_Global_Price_USD),
                        CMC_Raw_Tags = VALUES(CMC_Raw_Tags)
                """, (curr, get_asset_type(curr), global_usd_price, str(raw_tags)))

            # D. Get IDs and process the Pair table
            cursor.execute("SELECT Currency_ID FROM Currency WHERE Currency_Name=%s", (base,))
            base_id = cursor.fetchone()[0]
            cursor.execute("SELECT Currency_ID FROM Currency WHERE Currency_Name=%s", (quote,))
            quote_id = cursor.fetchone()[0]

            # Determine category for the pair (based on Base Asset tags)
            base_tags = cmc_info.get(base, {}).get('tags', [])
            mapped_cat = map_tags_to_8_categories(base_tags)

            cursor.execute("""
                INSERT INTO Pair (Pair_Name, Base_Currency_ID, Quote_Currency_ID, Manual_Category)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE Manual_Category = VALUES(Manual_Category)
            """, (symbol, base_id, quote_id, mapped_cat))

            # E. Insert current Price History point
            cursor.execute("SELECT Pair_ID FROM Pair WHERE Pair_Name=%s", (symbol,))
            pair_id = cursor.fetchone()[0]
            cursor.execute("INSERT INTO Price_Hist (Pair_ID, Price) VALUES (%s, %s)", (pair_id, price))

            print(f"Processed: {symbol} | Price: {price} | Category: {mapped_cat}")

        conn.commit()
        print("\nSUCCESS: Phase 1 completed. All assets (including USDT) enriched.")

    except Exception as e:
        print(f"Error during ingestion: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    run_ingestion()
    