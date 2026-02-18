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
        
        # --- STEP 1: SYNCHRONIZE DIM_PAIR (Categories from MySQL -> Snowflake) ---
        print("Synchronizing dimensions (DIM_PAIR)...")
        pairs_mysql = pd.read_sql("SELECT Pair_ID as PAIR_ID, Pair_Name as PAIR_NAME, Manual_Category as CATEGORY_NAME FROM Pair", my_conn)
        
        # Overwrite=True ensures that if you change a category in MySQL, it updates in Snowflake
        write_pandas(sf_conn, pairs_mysql, 'DIM_PAIR', auto_create_table=False, overwrite=True)

        # --- STEP 2: DELTA CHECK FOR PRICES ---
        # We check Snowflake for the most recent data point to avoid duplicates
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
        
        # Date formatting and filtering
        df['TIME_ID'] = pd.to_datetime(df['TIME_ID'])
        
        if last_snowflake_ts:
            # Only keep rows that are newer than the latest entry in Snowflake
            df = df[df['TIME_ID'] > pd.to_datetime(last_snowflake_ts)]
        
        if df.empty:
            print("No new data found. Bridge process terminated.")
            return

        print(f"Found {len(df)} new records for upload.")

        # --- STEP 4: PREPARE AND UPLOAD FACT DATA ---
        # Define Source_ID (Default 1 for Binance)
        df['SOURCE_ID'] = 1
        fact_df = df[['TIME_ID', 'PAIR_ID', 'SOURCE_ID', 'PRICE']]
        
        # Format timestamp for Snowflake compatibility
        fact_df['TIME_ID'] = fact_df['TIME_ID'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')

        # Upload using the high-performance write_pandas function (Append mode)
        write_pandas(sf_conn, fact_df, 'FACT_PRICE', auto_create_table=False)
        
        print(f"SUCCESS: {len(fact_df)} new price entries uploaded to Snowflake!")

    except Exception as e:
        print(f"Error during Bridge execution: {e}")
    finally:
        if 'my_conn' in locals(): my_conn.close()
        if 'sf_conn' in locals(): sf_conn.close()

if __name__ == "__main__":
    run_kimball_bridge()