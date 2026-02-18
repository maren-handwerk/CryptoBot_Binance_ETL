import mysql.connector
import snowflake.connector
from datetime import datetime

MYSQL_CONFIG = {
    'host': '127.0.0.1', 'user': 'root', 'password': '', 'database': 'CryptoBot_Step2'
}

SNOWFLAKE_CONFIG = {
    'user': 'MAREN3',
    'password': 'snowflake_Maren_32',
    'account': 'CZWRRCH-NL09103', 
    'warehouse': 'COMPUTE_WH',
    'database': 'CRYPTO_PROJECT',
    'schema': 'STAR_SCHEMA'
}

def run_bridge_etl():
    try:
        my_conn = mysql.connector.connect(**MYSQL_CONFIG)
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        my_cursor = my_conn.cursor(dictionary=True)
        sf_cursor = sf_conn.cursor()

        # Extract
        my_cursor.execute("""
            SELECT p.Pair_Name, p.Manual_Category, ph.Price, ph.Timestamp, 
                   c.Currency_Name, c.Asset_Type, c.Currency_ID as Local_Curr_ID
            FROM Price_Hist ph
            JOIN Pair p ON ph.Pair_ID = p.Pair_ID
            JOIN Currency c ON p.Base_Currency_ID = c.Currency_ID
        """)
        rows = my_cursor.fetchall()
        print(f"Gefunden in MySQL: {len(rows)} Datensätze.")

        for row in rows:
            if not row['Pair_Name'].endswith(('USD', 'USDT')):
                continue

            # Check Category
            # Wir nehmen nur den ersten Teil vor dem Komma, falls mehrere Tags da sind
            primary_cat = row['Manual_Category'].split(',')[0].strip()
            sf_cursor.execute("SELECT CATEGORY_ID FROM DIM_CATEGORY WHERE CATEGORY_NAME = %s", (primary_cat,))
            res = sf_cursor.fetchone()
            
            if not res:
                print(f"WARNUNG: Kategorie '{primary_cat}' nicht in Snowflake DIM_CATEGORY gefunden!")
                continue
            category_id = res[0]

            ts = row['Timestamp']
            # DIM_TIME
            sf_cursor.execute("INSERT INTO DIM_TIME SELECT %s,%s,%s,%s,%s,%s,%s,%s WHERE NOT EXISTS (SELECT 1 FROM DIM_TIME WHERE TIME_ID=%s)",
                             (ts, ts.day, ts.month, ts.year, (ts.month-1)//3+1, ts.hour, ts.minute, ts.second, ts))

            # DIM_CURRENCY
            sf_cursor.execute("MERGE INTO DIM_CURRENCY t USING (SELECT %s AS i, %s AS n, %s AS y) s ON t.CURRENCY_ID=s.i WHEN NOT MATCHED THEN INSERT VALUES (s.i,s.n,s.y)",
                             (row['Local_Curr_ID'], row['Currency_Name'], row['Asset_Type']))

            # FACT_PAIRS
            sf_cursor.execute("INSERT INTO FACT_PAIRS (PAIR_NAME, CURRENCY_ID, TIME_ID, CATEGORY_ID, SOURCE_ID, PRICE) VALUES (%s,%s,%s,%s,1,%s)",
                             (row['Pair_Name'], row['Local_Curr_ID'], ts, category_id, row['Price']))

        sf_conn.commit()
        print("ETL erfolgreich abgeschlossen.")

    except Exception as e:
        print(f"Fehler: {e}")
    finally:
        if my_conn: my_conn.close()
        if sf_conn: sf_conn.close()

if __name__ == "__main__":
    run_bridge_etl()