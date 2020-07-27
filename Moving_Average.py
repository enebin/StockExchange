import pandas as pd
import pandas_datareader.data as web
import matplotlib.pyplot as plt

#name = input("Name?")
start = input("From? 'YYYY-MM-DD'\n(If input is 0, start point is 2017-01-01)\n")

if start == '0':
    start = "2017-01-01"

# Get Stock Data from Yahoo
gs = web.DataReader("078930.KS", "yahoo", start)
new_gs = gs[gs['Volume']!=0]

# Moving average
ma5 = new_gs['Adj Close'].rolling(window=5).mean()
ma20 = new_gs['Adj Close'].rolling(window=20).mean()
ma60 = new_gs['Adj Close'].rolling(window=60).mean()
ma120 = new_gs['Adj Close'].rolling(window=120).mean()

# Insert columns
new_gs.insert(len(new_gs.columns), "MA5", ma5)
new_gs.insert(len(new_gs.columns), "MA20", ma20)
new_gs.insert(len(new_gs.columns), "MA60", ma60)
new_gs.insert(len(new_gs.columns), "MA120", ma120)

# Plot
plt.plot(new_gs.index, new_gs['Adj Close'], label='Adj Close')
plt.plot(new_gs.index, new_gs['MA5'], label='MA5')
plt.plot(new_gs.index, new_gs['MA20'], label='MA20')
plt.plot(new_gs.index, new_gs['MA60'], label='MA60')
plt.plot(new_gs.index, new_gs['MA120'], label='MA120')

plt.legend(loc="best")
plt.grid()
plt.show()