import pandas as pd
import sys

cur_path = "C:\\Users\\집\\PycharmProjects\\StockExchange\\Practice"

stock_data_kdq = pd.read_csv(cur_path + '\\Practice_kosdaq.csv')
stock_data_ksp = pd.read_csv(cur_path + '\\Practice_kospi.csv')

franz = []
listz = []
for code in stock_data_ksp["종목코드"]:
    franz.append(str(code).zfill(6))

for code in stock_data_kdq["종목코드"]:
    listz.append(str(code).zfill(6))

sys.stdout = open('../Crawl/KOSPI.txt', 'w')
print(franz)
