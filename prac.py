import pandas as pd
import requests

URL = "https://finance.naver.com/item/main.nhn?code=005930"

samsung_electronic = requests.get(URL)
html = samsung_electronic.text

financial_stmt = pd.read_html(samsung_electronic.text)[3]

financial_stmt.set_index(('주요재무정보', '주요재무정보', '주요재무정보'), inplace=True)
financial_stmt.index.rename('주요재무정보', inplace=True)
financial_stmt.columns = financial_stmt.columns.droplevel(2)
financial_stmt = financial_stmt.iloc[1, :]
print(type(financial_stmt))
financial_stmt.to_csv("./prac.csv", encoding='utf-8-sig')


