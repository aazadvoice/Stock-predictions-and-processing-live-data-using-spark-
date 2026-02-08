import json
import time
import yfinance as yf
from kafka import KafkaProducer

with open("config.json") as f:
    config = json.load(f)

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


tickers = ['AAPL', 'GOOGL', 'TSLA', 'MSFT']

while True:
    for symbol in tickers:
        data = yf.Ticker(symbol).history(period="1d", interval="1m")
        if not data.empty:
            latest = data.iloc[-1]
            message = {
                "stock": symbol,
                "price": round(latest['Close'], 2),
                "time": str(latest.name)
            }
            producer.send("stock-stream", message)
            print(f"Produced: {message}")
    time.sleep(5)
