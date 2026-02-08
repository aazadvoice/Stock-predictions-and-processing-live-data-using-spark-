import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine

# Database connection
engine = create_engine("postgresql+psycopg2://user:password@localhost:5432/stockdb")

tickers = ["AAPL", "GOOGL", "TSLA", "MSFT"]
df_list = []

for ticker in tickers:
    print(f"Downloading {ticker}...")
    data = yf.download(ticker, period="60d", interval="15m")
    if not data.empty:
        # Ensure no multi-index columns
        data.columns = [col[0] if isinstance(col, tuple) else col for col in data.columns]
        data["Ticker"] = ticker
        data.reset_index(inplace=True)
        df_list.append(data)
    else:
        print(f"⚠️ No data found for {ticker}")

if df_list:
    df = pd.concat(df_list, ignore_index=True)

    # ✅ Group by Ticker but guarantee 1-D Series
    df["Return"] = df.groupby("Ticker")["Close"].transform(lambda x: x.pct_change())
    df["SMA_5"] = df.groupby("Ticker")["Close"].transform(lambda x: x.rolling(5).mean())
    df["SMA_10"] = df.groupby("Ticker")["Close"].transform(lambda x: x.rolling(10).mean())
    df["Volatility"] = df.groupby("Ticker")["Return"].transform(lambda x: x.rolling(10).std())
    df["Target"] = df.groupby("Ticker")["Close"].transform(lambda x: x.shift(-1) > x).astype(int)

    df.dropna(inplace=True)

    # Save clean data
    df.to_sql("historical_data", engine, if_exists="replace", index=False)

    print("✅ Historical data saved to Postgres (table: historical_data)")
else:
    print("❌ No valid stock data downloaded.")
