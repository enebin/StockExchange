import pandas as pd
import pandas_datareader.data as web
import matplotlib.pyplot as plt

# Get KOSPI & KOSDAQ's stocks info.
print("Data downloading...")
stock_data = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download', header=0)[0]
print("done")

# If you want to see how it looks like, uncomment below.
#print(stock_data.head(10), stock_data.info())

while True:
    try:
        name = input("Name is?\n")
        stock_code = stock_data[stock_data["회사명"] == name]["종목코드"].tolist()[0]
    except:
        print("Wrong input. Type again.")
        continue
    break


# If you want to see the actual stock code, uncomment below.
# print(stock_code, end='\n')

start = input("From when? 'YYYY-MM-DD'\n(If input is 0, start point is 2017-01-01)\n")

# If input is 0, start point becomes 2017-01-01
if start == '0':
    start = "2017-01-01"

# Data's form should be like '001234.KS'
stock_code_mod = str(stock_code).zfill(6) + ".KS"
print("Code : " + stock_code_mod)

# Get Stock Data from Yahoo between start point and today
gs = web.DataReader(stock_code_mod, "yahoo", start)
new_gs = gs[gs['Volume'] != 0]

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