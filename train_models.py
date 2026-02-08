import numpy as np
import pandas as pd
import yfinance as yf
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
import os

def create_dataset(data, look_back=100):
    X, Y = [], []
    for i in range(len(data) - look_back):
        X.append(data[i:(i + look_back), 0])
        Y.append(data[i + look_back, 0])
    return np.array(X), np.array(Y)

def train_and_save(stock_symbol, start_date, end_date):
    data = yf.download(stock_symbol, start=start_date, end=end_date)
    data.dropna(inplace=True)

    scaler = MinMaxScaler(feature_range=(0,1))
    scaled_data = scaler.fit_transform(data[['Close']])

    x, y = create_dataset(scaled_data)

    model = Sequential([
        Dense(100, activation='relu', input_shape=(x.shape[1],)),
        Dense(1)
    ])
    model.compile(optimizer='adam', loss='mse')
    model.fit(x, y, epochs=10, batch_size=32, verbose=1)

    os.makedirs("models", exist_ok=True)
    model.save("models/KNN_model.h5")
    print("Model saved!")

if __name__ == "__main__":
    train_and_save("MSFT", "2000-01-01", "2024-02-01")
