# coding = utf-8-sig

import sys
import pandas as pd
from tqdm import tqdm
from urllib.request import urlopen
from bs4 import BeautifulSoup
from multiprocessing import Pool


# 콤마를 숫자에서 지워주는 편의성을 위한 함수입니다. 반환값은 float 형 입니다.
def remove_coma(_input):
    try:
        return float(_input.replace(',', ''))
    except ValueError:
        return float(_input)


# ========아래로 메인 코드입니다========= #
print("======ANALYSIS STARTS=======")

# 종목 코드 리스트를 가져옵니다.
with open('KOSPI.txt', 'r') as f:
    CODES = f.readlines()
f.close()

CODES[0] = CODES[0].replace(" \'", "")
CODES[0] = CODES[0].replace("\'", "")
CODES[0] = CODES[0].replace("\' ", "")

CODES = CODES[0].split(',')

# 저장할 값들을 판다스 데이터프레임으로 구성합니다.
measurements = pd.DataFrame(columns=("NAME", "CODE", "PRICE", "PER", "EPS", "E_PER", "E_EPS", "PBR", "BPS", "ITR"))
print(measurements.columns)

for code in CODES:
    # 종목코드를 가져와 NAVER 증권 정보 URL에 대입합니다.
    url = "https://finance.naver.com/item/main.nhn?code=" + code

    # BS4를 이용한 HTML 소스 크롤링입니다.
    result = urlopen(url)
    html = result.read()
    soup = BeautifulSoup(html, 'lxml')

    # 잘못된 코드나 ETF를 솎아내기 위해 예외처리를 하였습니다.
    try:
        # HTML 소스에서 회사명을 찾아 저장합니다.
        NAME = soup.find("div", {"class": "wrap_company"}).find("h2").text

        # 소스에서 현재가를 찾아 저장합니다. (주로 크롤링한 날 종가)
        PRICE = soup.find("p", {"class": "no_today"}).text
        PRICE = remove_coma(PRICE.split('\n')[2])

        # HTML 소스에서 투자지표 Table을 찾아 저장한 후 값을 추출합니다.
        table_tag = soup.find("table", {"class": "per_table"})
        value_tag = table_tag.find_all("em")
    except AttributeError:
        continue

    # 기본 데이터를 저장합니다.
    values = [NAME, code, PRICE]

    # PER, PBR 테이플의 값을 리스트에 저장합니다.
    for i in value_tag:
        content = i.text
        if content == 'N/A':
            values.append('N/A')
        else:
            values.append(remove_coma(content))

    # 데이터프레임에 값들을 저장합니다.
    temp = pd.DataFrame(columns=("NAME", "CODE", "PRICE", "PER", "EPS", "E_PER", "E_EPS", "PBR", "BPS", "ITR"))
    temp.loc[0] = values

    measurements = measurements.append(temp, ignore_index=1)

    # 진행상황을 체크하며 값을 확인합니다.
    print(measurements.tail(1))


# 프로그램이 끝났다면 CSV 파일로 저장한 후 종료합니다.
measurements.to_csv('measurements.csv', encoding='utf-8-sig')


