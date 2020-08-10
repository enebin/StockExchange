import pandas_datareader as web
from datetime import datetime

today = datetime.today().strftime('%Y-%m-%d')

prices = web.DataReader("003280.KS", "yahoo", "2020-08-01")
print(prices.head())
curPrice = prices.Close.iloc[0]
prevPrice = prices.Open.iloc[0]

print(curPrice, prevPrice)

