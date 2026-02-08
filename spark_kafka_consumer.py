from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
import joblib
import pandas as pd
import os

# 1. Spark Session
spark = SparkSession.builder \
    .appName("StockPredictionConsumer") \
    .getOrCreate()

# 2. Load Logistic Regression Model
model_path = "/home/jovyan/spark-app/logreg_stock_model.pkl"
model = joblib.load(model_path)
if not os.path.exists(model_path):
    raise FileNotFoundError(f"❌ Model not found at {model_path}")
model = joblib.load(model_path)

# 3. Kafka Stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_topic") \
    .load()

# 4. Define schema to match producer JSON
schema = """
stock STRING,
price DOUBLE,
time STRING
"""

parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# 5. Prediction per micro-batch
def predict_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    pdf = batch_df.toPandas()
    
    # Example: Build dummy features for logistic regression
    pdf['Return'] = pdf['price'].pct_change().fillna(0)
    pdf['SMA_5'] = pdf['price'].rolling(5).mean().fillna(pdf['price'])
    pdf['SMA_10'] = pdf['price'].rolling(10).mean().fillna(pdf['price'])
    pdf['Volatility'] = pdf['price'].rolling(5).std().fillna(0)
    
    features = ['Return', 'SMA_5', 'SMA_10', 'Volatility']
    
    if not pdf.empty:
        pdf['Prediction'] = model.predict(pdf[features])
        print("\n🔮 Live Predictions:")
        print(pdf[['time', 'stock', 'price', 'Prediction']].tail(10))

# 6. Start Stream
query = parsed_df.writeStream \
    .foreachBatch(predict_batch) \
    .start()

query.awaitTermination()
