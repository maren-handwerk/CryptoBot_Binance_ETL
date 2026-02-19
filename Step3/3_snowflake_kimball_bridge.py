import mysql.connector
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

# --- 1. CONFIGURATIONS ---
MYSQL_CONFIG = {
    'host': '127.0.0.1', 
    'user': 'root', 
    'password': '', 
    'database': 'CryptoBot_Step2'
}

SNOWFLAKE_CONFIG = {
    'user': 'MAREN3', 
    'password': 'snowflake_Maren_32', 
    'account': 'CZWRRCH-NL09103',
    'warehouse': 'COMPUTE_WH', 
    'database': 'CRYPTO_PROJECT', 
    'schema': 'STAR_SCHEMA'
}

def run_kimball_bridge():
    try:
        # Establish connections
        my_conn = mysql.connector.connect(**MYSQL_CONFIG)
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        
        # --- STEP 1: SYNCHRONIZE DIM_PAIR (Full Master Data Sync) ---
        print("Synchronizing dimensions (DIM_PAIR) with corrected mapping...")
        
        # Get PAIR_ID, Name, Category AND the Asset Types via JOIN
        # This automatically corrects the "BTC = Stablecoin" error from MySQL
        pairs_query = """
            SELECT 
                p.Pair_ID as PAIR_ID, 
                p.Pair_Name as PAIR_NAME, 
                p.Manual_Category as CATEGORY_NAME,
                c1.Currency_Name as BASE_CURRENCY,
                c1.Asset_Type as BASE_ASSET_TYPE,
                c2.Currency_Name as QUOTE_CURRENCY,
                c2.Asset_Type as QUOTE_ASSET_TYPE
            FROM Pair p
            JOIN Currency c1 ON p.Base_Currency_ID = c1.Currency_ID
            JOIN Currency c2 ON p.Quote_Currency_ID = c2.Currency_ID
        """
        pairs_mysql = pd.read_sql(pairs_query, my_conn)
        
        # Overwrite=True clears Snowflake on each run (Best Practice for Dimensions)
        write_pandas(sf_conn, pairs_mysql, 'DIM_PAIR', auto_create_table=False, overwrite=True)
        print(f"Successfully synced {len(pairs_mysql)} pairs to Snowflake.")

        # --- STEP 2: DELTA CHECK FOR PRICES ---
        print("Checking current data status in FACT_PRICE...")
        try:
            last_ts_df = pd.read_sql("SELECT MAX(TIME_ID) as LAST_TS FROM FACT_PRICE", sf_conn)
            last_snowflake_ts = last_ts_df['LAST_TS'].iloc[0]
        except:
            last_snowflake_ts = None
        
        print(f"Latest timestamp in Snowflake: {last_snowflake_ts}")

        # --- STEP 3: FETCH NEW DATA FROM MYSQL ---
        query = "SELECT Pair_ID as PAIR_ID, Price as PRICE, Timestamp as TIME_ID FROM Price_Hist"
        df = pd.read_sql(query, my_conn)
        
        df['TIME_ID'] = pd.to_datetime(df['TIME_ID'])
        
        if last_snowflake_ts:
            df = df[df['TIME_ID'] > pd.to_datetime(last_snowflake_ts)]
        
        if df.empty:
            print("No new price data found. Bridge process terminated.")
            return

        # --- STEP 4: UPLOAD FACT DATA (Append Mode) ---
        df['SOURCE_ID'] = 1
        fact_df = df[['TIME_ID', 'PAIR_ID', 'SOURCE_ID', 'PRICE']]
        fact_df['TIME_ID'] = fact_df['TIME_ID'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')

        # IMPORTANT: Do NOT overwrite here, so the history grows!
        write_pandas(sf_conn, fact_df, 'FACT_PRICE', auto_create_table=False)
        
        print(f"SUCCESS: {len(fact_df)} new price entries uploaded to Snowflake!")

    except Exception as e:
        print(f"Error during Bridge execution: {e}")
    finally:
        if 'my_conn' in locals(): my_conn.close()
        if 'sf_conn' in locals(): sf_conn.close()

if __name__ == "__main__":
    run_kimball_bridge()