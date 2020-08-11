# coding = utf-8-sig
import os.path
import time
import bcolors
import pandas as pd
import pandas_datareader.data as web
import multiprocessing as mp
import logging

from datetime import datetime
from tqdm import tqdm
from urllib.request import urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup


# 콤마를 숫자에서 지워주는 편의성을 위한 함수입니다. 반환값은 float 형 입니다.
def remove_coma(input_no):
    try:
        return float(input_no.replace(',', ''))
    except ValueError:
        return float(input_no)


# 리스트를 일정한 크기로 분할하여줍니다. 멀티프로세싱의 프로세스 배열 슬라이싱을 위해 사용합니다.
def chunks(input_list, size):
    for element in range(0, len(input_list), size):
        yield input_list[element: element + size]


# 종목 등락률을 계산합니다.
def get_FR(cur, prev):
    fr = ((cur - prev) / prev) * 100
    fr = round(fr, 2)
    return fr


# 종목 코드 리스트를 가져온 후 전처리합니다. init 함수에서 클래스 생성과 함께 실행됩니다.
def get_code_list(market, m_type):
    print(bcolors.WAITMSG + "Data processing for " + market + '_' + m_type + " starts now!" + bcolors.ENDC)

    df = pd.read_csv('./DATA/' + market + '_' + m_type + '.csv')
    df.CODE = df.CODE.map('{:06d}'.format)

    code_list = df.CODE.tolist()

    return code_list


# 쓰레딩을 위해 사용하는 스타트 함수입니다.
def starter(input_code, glob, period):
    if period == 'week':
        name, value_tag = get_data(input_code)

        # 에러상황(ETF, 리츠 등 펀드류 코드 경우)시 리턴합니다.
        if name == -1:
            logging.warning(input_code)
            return
        else:
            temp_row = make_data_frame(name, input_code, value_tag)
            glob.df = glob.df.append(temp_row)

    if period == 'day':
        curPrice, prevPrice = get_price(input_code, 1)
        temp_row = make_price_frame(input_code, curPrice, prevPrice)
        glob.df = glob.df.append(temp_row)


# 종목코드 하나를 받아 투자지표를 크롤링합니다. [종목명, [투자지표]]를 반환합니다.
def get_data(input_code):
    # 종목코드를 가져와 NAVER 증권 정보 URL에 대입합니다.
    url = "https://finance.naver.com/item/main.nhn?code=" + input_code

    # BS4를 이용한 HTML 소스 크롤링입니다.
    url_result = urlopen(url)
    html = url_result.read()
    soup = BeautifulSoup(html, 'lxml')

    # 잘못된 코드나 ETF를 솎아내기 위해 예외처리를 하였습니다.
    try:
        # HTML 소스에서 회사명을 찾아 저장합니다.
        name = soup.find("div", {"class": "wrap_company"}).find("h2").text

        # HTML 소스에서 투자지표 Table을 찾아 저장한 후 값을 추출합니다.
        table_tag = soup.find("table", {"class": "per_table"})
        values_raw = table_tag.find_all("em")

        return name, values_raw

    except AttributeError:
        print(bcolors.ERRMSG + "ERROR OCCURS\n" + bcolors.ITALIC +
              "Possible Error: It can be ETF, REITs, etc...")
        return -1, [-1]


# 종목코드 하나를 받아 주가를 받아옵니다. [현재가, 전일종가]를 반환합니다.
def get_price(input_code, try_cnt):
    """
    # 야후 파이낸스릉 이용한 방법. 없는 데이터가 너무 많다.
    if market == 'KOSPI':
        code_mod = code + '.KS'
    elif market == 'TEST':
        code_mod = code + '.KS'
    else:
        code_mod = code + '.KQ'

    today = datetime.today().strftime('%Y-%m-%d')

    try:
        prices = web.DataReader(code_mod, "yahoo", today)
        print("url opened")
        curPrice = prices.Close.iloc[0]
        prevPrice = prices.Open.iloc[0]
    except KeyError:
        print(code_mod)
    """

    """
    # KRX 한국 거래소를 이용한 방법. 빠를땐 광속이나 대부분 더럽게 느리다. 
    url = "http://asp1.krx.co.kr/servlet/krx.asp.XMLSiseEng?code={}".format(code)
    req = urlopen(url)
    print("url opened")
    result = req.read()
    xmlsoup = BeautifulSoup(result, "lxml-xml")
    curPrice = xmlsoup.find("TBL_StockInfo").attrs["CurJuka"]
    prevPrice = xmlsoup.find("TBL_StockInfo").attrs["PrevJuka"]

    curPrice = remove_coma(curPrice)
    prevPrice = remove_coma(prevPrice)
    """

    # 네이버 금융을 이용한 방법. 그나마 가장 안정적이고 준수하다.
    # 소스에서 현재가를 찾아 저장합니다. (주로 크롤링한 날 종가)
    url = "https://finance.naver.com/item/main.nhn?code=" + input_code

    # BS4를 이용한 HTML 소스 크롤링입니다.
    url_result = urlopen(url)
    html = url_result.read()
    soup = BeautifulSoup(html, 'lxml')

    try:
        # 소스에서 현재가를 찾아 저장합니다. (주로 크롤링한 날 종가)
        curPrice = soup.find("p", {"class": "no_today"}).text
        curPrice = remove_coma(curPrice.split('\n')[2])

        prevPrice = soup.find("td", {"class": "first"}).text
        prevPrice = remove_coma(prevPrice.split('\n')[3])

    except AttributeError:
        print(bcolors.ERRMSG + "ERROR OCCURS\n" + bcolors.ITALIC +
              "Possible Error: It can be transaction suspension, delisting, etc...")

    return curPrice, prevPrice


# get_data 에서 크롤링한 데이터를 받아 데이터프레임에 저장하는 함수입니다.
# 데이터프레임을 반환합니다.
def make_data_frame(name, input_code, values_raw):
    # 크롤링으로 받은 HTML 소스(values_raw)에서 PER, PBR 등의 값을 추출합니다.
    values = []
    for i in values_raw:
        content = i.text
        if content == 'N/A':
            values.append('N/A')
        else:
            values.append(remove_coma(content))

    # 1줄짜리 임시 데이터프레임을 구성하여 데이터를 저장합니다.
    temp_data = pd.DataFrame(columns=("NAME", "CODE", 'CUR PRICE', 'PREV PRICE', 'FR',
                                      "PER", "EPS", "E_PER", "E_EPS", "PBR", "BPS", "ITR"))

    temp_data.loc[0, 'NAME'] = name
    temp_data.loc[0, 'CODE'] = input_code

    temp_data.loc[0, 'PER':"ITR"] = values

    # 진행상황을 체크하며 값을 확인합니다.
    # print(temp_data.tail(1))

    # 데이터프레임을 반환합니다.
    return temp_data


# 종목코드를 받아 주식의 현재가, 전일 종가, 등락률을 저장하는 함수입니다.
# 데이터프레임을 반환합니다.
def make_price_frame(input_code, curPrice, prevPrice):
    res = pd.DataFrame(columns=['CODE', 'CUR PRICE', 'PREV PRICE', 'FR'])

    res.loc[0, 'CODE'] = input_code
    res.loc[0, 'CUR PRICE'] = curPrice
    res.loc[0, 'PREV PRICE'] = prevPrice
    res.loc[0, 'FR'] = float(get_FR(curPrice, prevPrice))

    # 진행상황을 체크하며 값을 확인합니다.
    # print(res.head())

    return res


# 프로그램이 끝났다면 CSV 파일로 저장한 후 종료합니다.
def merger(globs, market, m_type, code_list, period):
    result = measurements
    for glob in globs:
        result = result.append(glob.df)

    print(bcolors.OKMSG + "Finished! %d items were collected, except for %d errors"
          % (result.shape[0], len(code_list) - result.shape[0]))

    # 결과데이터를 csv 로 출력하기 위한 과정입니다.
    print(bcolors.WAITMSG + "Now processing output... " + bcolors.ENDC)
    result.drop(['NAME'], axis='columns', inplace=True)

    original_df = pd.read_csv('./DATA/' + market + '_' + m_type + '.csv')
    original_df.CODE = original_df.CODE.map('{:06d}'.format)

    result = pd.merge(original_df, result, on='CODE')
    file_name = ('./DATA/Results/DATA_' + market + '_' +
                 m_type + datetime.today().strftime("_%Y%m%d") + '.csv')

    if os.path.isfile(file_name):
        object_df = pd.read_csv(file_name)
        if period == 'day':
            result.iloc[:, 9:16] = object_df.iloc[:, 9:16]
        else:
            result.iloc[:, 6:9] = object_df.iloc[:, 6:9]
    else:
        pass

    result = result.fillna('N/A')
    print(bcolors.HELP + "↓ Information about results is here ↓" + bcolors.ENDC)
    print(result.info())

    result.to_csv(file_name, encoding='utf-8-sig', index=False)
    print(bcolors.OKMSG + "Done Successfully!" + bcolors.ENDC)
    print('\n')


def multiprocess(globs, market, m_type, period, code_list, numberOfThreads=8):
    # 값들을 저장할 Pandas 데이터프레임을 구성합니다.
    # Multiprocessing 을 위한 전처리도 같이 합니다.
    # 프로세스의 개수 (MAX = 18)
    if numberOfThreads > 18:
        print(bcolors.ERRMSG + "Too much process. Check number of threads again")
        return

    if period == 'week':
        print(bcolors.OKMSG + "Processing type is: Investment index.")
    else:
        print(bcolors.OKMSG + "Processing type is: Price informations.")

    for glob in globs:
        glob.df = measurements

    # 데이터프레임을 프로세스 개수에 따라 분배하며 프로세스 개수는 성능 여유분에 따라 조절 가능합니다.
    processes = []
    index = 0
    for code in tqdm(code_list, desc="Processing DATA"):
        process = mp.Process(target=starter, args=(str(code), globs[index], str(period)))
        index = (index + 1) % numberOfThreads
        processes.append(process)

    print(bcolors.OKMSG + "Done Successfully" + bcolors.ENDC)
    print(bcolors.OKMSG + "Number of processes: " + str(numberOfThreads) + bcolors.ENDC)
    print(bcolors.WAITMSG + "Analysis starts now. " + bcolors.ITALIC +
          "FYI: Only stocks are included" + bcolors.ENDC)

    for i in chunks(processes, numberOfThreads):
        for j in i:
            j.start()
        for j in i:
            j.join()

        # 진행상황을 체크하며 값을 확인합니다.
        count = 0
        for glob in globs:
            count += glob.df.shape[0]
        tqdm(total=len(code_list)).update(count)

        # for glob in globs:
        #     print(glob.df.tail(1), end='\n')

    merger(globs, market, m_type, code_list, period)


def initialize(input_list):
    for x in input_list:
        x = 0


# ========아래로 메인 코드입니다========= #
if __name__ == '__main__':
    start_time = time.time()

    measurements = pd.DataFrame(columns=("NAME", "CODE", 'CUR PRICE', 'PREV PRICE', 'FR', "PER", "EPS",
                                         "E_PER", "E_EPS", "PBR", "BPS", "ITR"))

    market = 'KOSPI'
    m_type = 'noBank'
    period = 'day'

    unit = [['KOSPI', 'noBank', 'week'],
            ['KOSPI', 'noBank', 'day'],
            ['KOSDAQ', 'noBank', 'week'],
            ['KOSDAQ', 'noBank', 'day']]

    unit_test = [['TEST', 'noBank', 'week'],
                 ['TEST', 'noBank', 'day'],
                 ['TEST2', 'noBank', 'day'],
                 ['TEST2', 'noBank', 'week']]

    for a in unit:
        # 12개의 데이터프레임을 만들어줍니다. 각각의 프로세서가 사용할 데이터프레임입니다.
        manager = mp.Manager()
        global1 = manager.Namespace()
        global2 = manager.Namespace()
        global3 = manager.Namespace()
        global4 = manager.Namespace()
        global5 = manager.Namespace()
        global6 = manager.Namespace()
        global7 = manager.Namespace()
        global8 = manager.Namespace()
        global9 = manager.Namespace()
        global10 = manager.Namespace()
        global11 = manager.Namespace()
        global12 = manager.Namespace()
        global13 = manager.Namespace()
        global14 = manager.Namespace()
        global15 = manager.Namespace()
        global16 = manager.Namespace()
        global17 = manager.Namespace()
        global18 = manager.Namespace()

        globs = [global1, global2, global3, global4, global5, global6,
                 global7, global8, global9, global10, global11, global12,
                 global13, global14, global15, global16, global17, global18, ]

        '''===
        market은 'KOSPI', 'KOSDAQ'중 하나를 선택합니다. 선택하지 않을 시 기본값은 'KOSPI'입니다.
    
        m_type은 'noBank'(지주회사, 리츠, 은행 등 금융회사 제외), 'noUse'(noBank에서 제외된 요소만), 'ALL'(제외 없이 모두 다 포함)-
        -중 하나를 선택합니다. 선택하지 않을 시 기본값은 'noBank'입니다.
    
        period 는 'day'와 'week' 중 하나를 선택합니다. 'day'를 선택할 경우 가격에 관한 정보가, 'week'을 선택할 경우 투자지표에 관한 정보가 
        수집됩니다. 
        ==='''

        code_list = get_code_list(market=a[0], m_type=a[1])

        # 멀티 프로세스는 속도 향상을 위해 필요합니다. n개의 프로세스를 사용해 속도를 n배로 끌어 올립니다.
        # 기본값은 8입니다.
        multiprocess(globs, market=a[0], m_type=a[1], period=a[2], code_list=code_list, numberOfThreads=4)

        initialize(globs)
        initialize(code_list)

    ex_time = time.time() - start_time

    print('\n')
    print(bcolors.OKMSG + "It took %dm %.2fs." % (ex_time / 60, round(ex_time, 2) % 60) + bcolors.ENDC)


