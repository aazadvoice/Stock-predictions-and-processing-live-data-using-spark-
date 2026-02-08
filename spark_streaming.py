import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Load config
with open("/home/jovyan/spark-app/config.json") as f:
    config = json.load(f)

# Spark session
spark = SparkSession.builder.appName("StockStreamApp").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read Kafka stream
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-stream") \
    .load()

# Define schema
schema = StructType() \
    .add("stock", StringType()) \
    .add("price", DoubleType()) \
    .add("time", StringType())

# Parse JSON messages
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Output to console
query = json_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
