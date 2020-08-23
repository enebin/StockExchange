import pandas as pd
import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup


input_code = '000120'

# 종목코드를 가져와 NAVER 증권 정보 URL에 대입합니다.
url = "https://finance.naver.com/item/main.nhn?code=" + input_code

# BS4를 이용한 HTML 소스 크롤링입니다.
url_result = urlopen(url)
html = url_result.read()
soup = BeautifulSoup(html, 'lxml')

'''samsung_electronic = requests.get(url)
html = samsung_electronic.text'''

financial_stmt = pd.read_html(html, encoding='euc-kr')[3]
financial_stmt.set_index(('주요재무정보', '주요재무정보', '주요재무정보'), inplace=True)
financial_stmt.index.rename('주요재무정보', inplace=True)
financial_stmt.columns = financial_stmt.columns.droplevel(2)
financial_stmt = financial_stmt.iloc[1, :]
profits_list = financial_stmt.tolist()

print(profits_list)

