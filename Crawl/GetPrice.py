import pandas as pd
import time
import logging
import multiprocessing

from urllib.request import *
from urllib.error import *
from tqdm import tqdm
from bs4 import BeautifulSoup


def get_price(code, try_cnt):
    try:
        print(code)
        url = "http://asp1.krx.co.kr/servlet/krx.asp.XMLSiseEng?code={}".format(code)
        req = urlopen(url)
        print("url")
        result = req.read()

        xmlsoup = BeautifulSoup(result, "lxml-xml")
        curPrice = xmlsoup.find("TBL_StockInfo").attrs["CurJuka"]
        prevPrice = xmlsoup.find("TBL_StockInfo").attrs["PrevJuka"]
        print(curPrice, prevPrice)

        curPrice = remove_coma(curPrice)
        prevPrice = remove_coma(prevPrice)

        time.sleep(0.3)

        return curPrice, prevPrice

    except HTTPError as e:
        logging.warning(e)
        if try_cnt >= 3:
            return None
        else:
            get_price(code, try_cnt=+1)


def get_FR(cur, prev):
    fr = ((cur - prev) / prev) * 100
    fr = round(fr, 2)
    return fr


# 콤마를 숫자에서 지워주는 편의성을 위한 함수입니다. 반환값은 float 형 입니다.
def remove_coma(input_no):
    try:
        return float(input_no.replace(',', ''))
    except ValueError:
        return float(input_no)


# 주식 시세 DB에 저장하기
stocks = pd.read_csv("./DATA/KOSPI_noBank.csv")
stocks_codes = stocks.CODE

test = ['005380', '066570']
res = pd.DataFrame(columns=['CODE', 'CUR PRICE', 'PREV PRICE', 'FR'])

for index in tqdm(range(len(test))):
    trial = 1
    code = stocks_codes[index]
    code = str(code).zfill(6)
    cur_price, prev_price = get_price(code, trial)

    res.loc[index, 'CODE'] = code
    res.loc[index, 'CUR PRICE'] = cur_price
    res.loc[index, 'PREV PRICE'] = prev_price
    res.loc[index, 'FR'] = float(get_FR(cur_price, prev_price))

    time.sleep(0.3)

print(res.head(10))



