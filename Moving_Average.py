import os
import pandas as pd
import pandas_datareader.data as web
import matplotlib.pyplot as plt


# Current directory path
cur_path = os.path.dirname(os.path.realpath(__file__))

print('==== Automatic MA-graph Drawer ====\n')


# Get KOSPI & KOSDAQ's stocks info.
# If you haven't executed it before, you must do it once.
# ------------------------------------------------------------
'''
def getStockCode(market):
    if market == 'kosdaq':
        url_market = 'kosdaqMkt'
    elif market == 'kospi':
        url_market = 'stockMkt'
    else:
        print('invalid market ')
        return
    url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=%s' % url_market

    print(market + "'s data downloading...")
    _stock_data = pd.read_html(url, header=0)[0]
    print("done")

    return _stock_data


stock_csv_kdq = getStockCode('kosdaq')
stock_csv_ksp = getStockCode('kospi')

stock_csv_kdq.to_csv(cur_path + '_kosdaq.csv', encoding='utf-8-sig')
stock_csv_ksp.to_csv(cur_path + '_kospi.csv', encoding='utf-8-sig')
'''
# ------------------------------------------------------------

# If you want to see how the data look like, uncomment below.
# print(stock_data.head(10), stock_data.info())

stock_data_kdq = pd.read_csv(cur_path + '_kosdaq.csv')
stock_data_ksp = pd.read_csv(cur_path + '_kospi.csv')

# If you want to see the actual stock code, uncomment below.
# print(stock_data_kdq.info(), end='\n')

# Find input from stock data.
while True:
    name = input("Name is?\n")
    is_kdq = stock_data_kdq['회사명'].isin([name]).any()
    is_ksp = stock_data_ksp['회사명'].isin([name]).any()

    if is_kdq:
        stock_code = stock_data_kdq[stock_data_kdq["회사명"] == name]["종목코드"].tolist()[0]
        break
    elif is_ksp:
        stock_code = stock_data_ksp[stock_data_ksp["회사명"] == name]["종목코드"].tolist()[0]
        break
    else:
        print("Wrong input. Try again!")

# Data's form should be like KOSPI:'001234.KS', KOSDAQ:'001234.KQ'
if is_kdq:
    stock_code_mod = str(stock_code).zfill(6) + ".KQ"
elif is_ksp:
    stock_code_mod = str(stock_code).zfill(6) + ".KS"
print("Code : " + stock_code_mod)


# Get start date from input
start = input("From when? 'YYYY-MM-DD'\n(If input is 0, start point is 2017-01-01)\n")

# If input is 0, start point becomes 2017-01-01
if start == '0':
    start = "2017-01-01"


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
plt.plot(new_gs.index, new_gs['Adj Close'], label='Adj Price')
plt.plot(new_gs.index, new_gs['MA5'], label='MA5')
plt.plot(new_gs.index, new_gs['MA20'], label='MA20')
plt.plot(new_gs.index, new_gs['MA60'], label='MA60')
plt.plot(new_gs.index, new_gs['MA120'], label='MA120')


plt.legend(loc="best")
plt.grid()
plt.show()