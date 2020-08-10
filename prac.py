import pandas_datareader as web
from datetime import datetime

today = datetime.today().strftime('%Y-%m-%d')

prices = web.DataReader("005380.KS", "yahoo", today)
curPrice = prices.Close.iloc[0]
prevPrice = prices.Open.iloc[0]

print(curPrice, prevPrice)

