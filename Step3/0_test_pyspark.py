from pyspark.sql import SparkSession

# initialize Spark Session
spark = SparkSession.builder \
    .appName("PySpark_Test") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Test Data Set
data = [("Binance", 1), ("CoinMarketCap", 2)]
df = spark.createDataFrame(data, ["Source", "ID"])

print("--- PYSPARK TEST ERFOLGREICH ---")
df.show()

spark.stop()