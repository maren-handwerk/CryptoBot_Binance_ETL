from pyspark.sql import SparkSession
import requests
import os

# Step 3: Extraction Phase (A)
# Goal: Extract data from MySQL and CMC API, then save to local disk (Staging)

# 1. Initialize Spark Session
# - Includes the MySQL JDBC driver (version 9.6.0) from the 'lib' folder
# - Sets local networking fixes for macOS (127.0.0.1)
spark = SparkSession.builder \
    .appName("CryptoBot_Extraction_Staging") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars", "lib/mysql-connector-j-9.6.0.jar") \
    .getOrCreate()

# 2. MySQL Configuration (from Step 2)
jdbc_url = "jdbc:mysql://localhost:3306/CryptoBot_Step2"
db_properties = {
    "user": "root",
    "password": "", # Enter your password if applicable
    "driver": "com.mysql.cj.jdbc.Driver"
}

# 3. Extraction from MySQL (3NF Tables)
print("Reading tables from MySQL...")
try:
    df_prices = spark.read.jdbc(jdbc_url, "Price_Hist", properties=db_properties)
    df_pairs = spark.read.jdbc(jdbc_url, "Pair", properties=db_properties)
    df_currencies = spark.read.jdbc(jdbc_url, "Currency", properties=db_properties)
    print("MySQL data loaded successfully.")
except Exception as e:
    print(f"Error reading MySQL: {e}")

# 4. Extraction from CoinMarketCap API
print("Fetching metadata from CoinMarketCap...")
cmc_api_key = "9d2a04faa72a4d6295c495fa99ce63d1"
cmc_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
headers = {
    'X-CMC_PRO_API_KEY': cmc_api_key,
    'Accept': 'application/json'
}

response = requests.get(cmc_url, headers=headers)
if response.status_code == 200:
    cmc_data = response.json()['data']
    df_cmc = spark.createDataFrame(cmc_data)
    print("CMC metadata loaded successfully.")
else:
    print(f"Error CMC API: {response.status_code}")

# 5. Save to Disk (Staging Layer)
# We save the data as Parquet files to maintain schema and data types
raw_path = "data/raw/"
print(f"Saving raw data to {raw_path}...")

# Create directory if it doesn't exist
if not os.path.exists(raw_path):
    os.makedirs(raw_path)

df_prices.write.mode("overwrite").parquet(raw_path + "prices")
df_pairs.write.mode("overwrite").parquet(raw_path + "pairs")
df_currencies.write.mode("overwrite").parquet(raw_path + "currencies")
df_cmc.write.mode("overwrite").parquet(raw_path + "cmc_metadata")

print("--- EXTRACTION COMPLETED AND DATA PERSISTED ---")

# Stop the session
spark.stop()