from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, year, month, dayofmonth, quarter, row_number
import requests

# 1. Initialize Spark Session with MySQL Driver
spark = SparkSession.builder \
    .appName("CryptoBot_Full_ETL") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars", "lib/mysql-connector-j-9.6.0.jar") \
    .getOrCreate()

# 2. EXTRACTION Phase
jdbc_url = "jdbc:mysql://localhost:3306/CryptoBot_Step2"
db_props = {"user": "root", "password": "", "driver": "com.mysql.cj.jdbc.Driver"}

print("Extracting data from MySQL...")
df_prices = spark.read.jdbc(jdbc_url, "Price_Hist", properties=db_props)
df_pairs = spark.read.jdbc(jdbc_url, "Pair", properties=db_props)
df_currencies = spark.read.jdbc(jdbc_url, "Currency", properties=db_props)

print("Fetching metadata from CoinMarketCap...")
cmc_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
headers = {'X-CMC_PRO_API_KEY': '9d2a04faa72a4d6295c495fa99ce63d1', 'Accept': 'application/json'}
cmc_data = requests.get(cmc_url, headers=headers).json()['data']
df_cmc = spark.createDataFrame(cmc_data)

# Goal: Filter CMC data to keep only the primary asset for each symbol (e.g., the real BTC with Rank 1) - Deduplication step
# We partition by 'symbol' and order by 'rank' to identify the top asset
window_spec = Window.partitionBy("symbol").orderBy("rank")

df_cmc_unique = df_cmc.withColumn("rank_priority", row_number().over(window_spec)) \
    .filter(col("rank_priority") == 1) \
    .drop("rank_priority")

# 3. TRANSFORMATION Phase (The Joins)
# Join 1: Prices + Pairs (via Pair_ID)
df_step1 = df_prices.join(df_pairs, df_prices.Pair_ID == df_pairs.Pair_ID)

# Join 2: Result + Currencies (via Base_Currency_ID to get clean Symbol like 'BTC')
df_step2 = df_step1.join(df_currencies, df_step1.Base_Currency_ID == df_currencies.Currency_ID)

# Join 3: Result + UNIQUE CMC Metadata (via Symbol)
# We use the deduplicated df_cmc_unique here to prevent row multiplication
df_final = df_step2.join(df_cmc_unique, df_step2.Currency_Name == df_cmc_unique.symbol, "left")

# 4. PREPARING DIMENSIONS (Star Schema Structure)
# DIM_TIME: Breakdown of the Timestamp as planned in your diagram
df_dim_time = df_prices.select("Timestamp").distinct() \
    .withColumn("Year", year("Timestamp")) \
    .withColumn("Month", month("Timestamp")) \
    .withColumn("Day", dayofmonth("Timestamp")) \
    .withColumn("Quarter", quarter("Timestamp"))

# 5. FINAL PREVIEW
print("--- FINAL TRANSFORMATION PREVIEW (DEDUPLICATED) ---")
df_final.select(
    "Timestamp", 
    "Currency_Name", 
    "Price", 
    "Manual_Category", # This remains 'To be defined' from your MySQL setup
    "symbol", 
    "name"             # Full name from CMC (e.g., 'Bitcoin')
).show(5)

print("--- DIM_TIME PREVIEW ---")
df_dim_time.show(5)

# Optional: Stop Spark
# spark.stop()

# EXPORT PHASE FOR SNOWFLAKE ---
# We are now writing the data to your 'data/export' folder as CSV
print("Exporting data for Snowflake import...")

# 1. Export Fact Data (Fixed the ambiguous Pair_ID reference)
# By using df_prices["Pair_ID"], we tell Spark exactly which one to take.
df_final.select(
    col("Timestamp").alias("TIME_ID"),
    df_currencies["Currency_ID"], # Clarify origin
    df_prices["Pair_ID"],         # Clarify origin to fix [AMBIGUOUS_REFERENCE]
    col("Pair_Name"),
    col("Price")
).coalesce(1).write.mode("overwrite").option("header", "true").csv("data/export/fact_pairs")

# 2. Export Time Dimension (This should work as is)
df_dim_time.coalesce(1).write.mode("overwrite").option("header", "true").csv("data/export/dim_time")

# 3. Export Currency Dimension
df_currencies.select("Currency_ID", "Currency_Name", "Asset_Type") \
    .coalesce(1).write.mode("overwrite").option("header", "true").csv("data/export/dim_currency")

print("Export finished! Check your 'data/export' folder for the CSV files.")

# Optional: Stop Spark
spark.stop()