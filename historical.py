import pandas as pd

def trading_algorithm(data):
    # Create a DataFrame from the input data
    df = pd.DataFrame(data, columns=["Open", "High", "Low", "Close"])
    
    # Create a new column to store the daily returns
    df["Returns"] = df["Close"] / df["Open"] - 1
    
    # Create a variable to store the current position (1 = long, -1 = short)
    position = 0
    
    # Create a variable to store the capital
    capital = 100
    
    # Create a list to store the trades
    trades = []
    
    # Loop through each row in the DataFrame
    for index, row in df.iterrows():
        if position == 0:
            # If we don't have a position, check if the returns are positive
            if row["Returns"] > 0:
                # If the returns are positive, buy one share
                trades.append(("Buy", row["Open"], 1))
                capital -= row["Open"]
                position = 1
            elif row["Returns"] < 0:
                # If the returns are negative, short one share
                trades.append(("Short", row["Open"], 1))
                capital += row["Open"]
                position = -1
        elif position == 1:
            # If we have a long position, check if the returns are negative
            if row["Returns"] < 0:
                # If the returns are negative, sell one share
                trades.append(("Sell", row["Open"], 1))
                capital += row["Open"]
                position = 0
        elif position == -1:
            # If we have a short position, check if the returns are positive
            if row["Returns"] > 0:
                # If the returns are positive, cover one share
                trades.append(("Cover", row["Open"], 1))
                capital -= row["Open"]
                position = 0
    
    # Return the capital and the trades
    return capital, trades
import pandas as pd
import numpy as np

def trading_algorithm(data, window=14):
    # Create a DataFrame from the input data
    df = pd.DataFrame(data, columns=["Open", "High", "Low", "Close"])
    
    # Calculate the moving average
    df["MA"] = df["Close"].rolling(window=window).mean()
    
    # Create a variable to store the current position (1 = long, -1 = short)
    position = 0
    
    # Create a variable to store the capital
    capital = 100
    
    # Create a list to store the trades
    trades = []
    
    # Loop through each row in the DataFrame
    for index, row in df.iterrows():
        if position == 0:
            # If we don't have a position, check if the price is above the moving average
            if row["Close"] > row["MA"]:
                # If the price is above the moving average, buy one share
                trades.append(("Buy", row["Close"], 1))
                capital -= row["Close"]
                position = 1
            elif row["Close"] < row["MA"]:
                # If the price is below the moving average, short one share
                trades.append(("Short", row["Close"], 1))
                capital += row["Close"]
                position = -1
        elif position == 1:
            # If we have a long position, check if the price is below the moving average
            if row["Close"] < row["MA"]:
                # If the price is below the moving average, sell one share
                trades.append(("Sell", row["Close"], 1))
                capital += row["Close"]
                position = 0
        elif position == -1:
            # If we have a short position, check if the price is above the moving average
            if row["Close"] > row["MA"]:
                # If the price is above the moving average, cover one share
                trades.append(("Cover", row["Close"], 1))
                capital -= row["Close"]
                position = 0
    
    # Return the capital and the trades
    return capital, trades
