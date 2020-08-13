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
from abc import ABCMeta, abstractmethod


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


def get_code_list(market, m_type, in_path):
    print(bcolors.WAITMSG + "Data processing for " + market + '_' +
          m_type + " starts now!" + bcolors.ENDC)

    df = pd.read_csv(in_path)
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

    elif period == 'day':
        curPrice, prevPrice = get_price(input_code)
        temp_row = make_price_frame(input_code, curPrice, prevPrice)
        glob.df = glob.df.append(temp_row)

    elif period == 'all':
        name, curPrice, prevPrice, value_tag = get_all(input_code)

        # 에러상황(ETF, 리츠 등 펀드류 코드 경우)시 리턴합니다.
        if name == -1:
            logging.warning(input_code)
            return
        else:
            temp_row = make_all_frame(name, input_code, curPrice, prevPrice, value_tag)
            glob.df = glob.df.append(temp_row)


def multiprocess(globs, period, code_list, in_path, out_path, numberOfThreads=8):
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

    merger(globs, code_list, period, in_path, out_path)


# start -> get -> make -> merge

class collectBase(metaclass=ABCMeta):
    def __init__(self, code):
        self.code = code
        self.values = []

    @abstractmethod
    def collect_data(self):
        pass

    @abstractmethod
    def give_date(self):
        pass


class makeBase(metaclass=ABCMeta):
    def __init__(self):
        pass

    @abstractmethod
    def make_data_frame(self):
        pass


class collectAll(collectBase):
    def __init__(self, code):
        super.__init__(code)
        self.name = ''
        self.curPrice = 0
        self.prevPrice = 0
        self.values_raw = []

    def collect_data(self):
        input_code = self.code

        # 종목코드를 가져와 NAVER 증권 정보 URL에 대입합니다.
        url = "https://finance.naver.com/item/main.nhn?code=" + input_code

        # BS4를 이용한 HTML 소스 크롤링입니다.
        url_result = urlopen(url)
        html = url_result.read()
        soup = BeautifulSoup(html, 'lxml')

        # 잘못된 코드나 ETF를 솎아내기 위해 예외처리를 하였습니다.
        try:
            # HTML 소스에서 회사명을 찾아 저장합니다.
            self.name = soup.find("div", {"class": "wrap_company"}).find("h2").text

            # HTML 소스에서 투자지표 Table을 찾아 저장한 후 값을 추출합니다.
            table_tag = soup.find("table", {"class": "per_table"})
            self.values_raw = table_tag.find_all("em")

            # 소스에서 현재가를 찾아 저장합니다. (주로 크롤링한 날 종가)
            curPrice = soup.find("p", {"class": "no_today"}).text
            self.curPrice = remove_coma(curPrice.split('\n')[2])

            prevPrice = soup.find("td", {"class": "first"}).text
            self.prevPrice = remove_coma(prevPrice.split('\n')[3])

        except AttributeError:
            print(bcolors.ERRMSG + "ERROR OCCURS\n" + bcolors.ITALIC +
                  "Possible Error: It can be REITs, Transaction Suspension etc...")
            logging.warning(input_code)

            self.name = -1

    def give_date(self):
        return self.name, self.curPrice, self.prevPrice, self.values_raw


def make_all_frame(name, input_code, curPrice, prevPrice, values_raw):
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
    temp_data.loc[0, 'CUR PRICE'] = curPrice
    temp_data.loc[0, 'PREV PRICE'] = prevPrice

    # 진행상황을 체크하며 값을 확인합니다.
    # print(temp_data.tail(1))

    # 데이터프레임을 반환합니다.
    return temp_data


# 프로그램이 끝났다면 CSV 파일로 저장한 후 종료합니다.
def merger(globs, code_list, period, in_path, out_path):
    result = measurements
    for glob in globs:
        result = result.append(glob.df)

    print(bcolors.OKMSG + "Finished! %d items were collected, except for %d errors"
          % (result.shape[0], len(code_list) - result.shape[0]))

    # 결과데이터를 csv 로 출력하기 위한 과정입니다.
    print(bcolors.WAITMSG + "Now processing output... " + bcolors.ENDC)
    result.drop(['NAME'], axis='columns', inplace=True)

    original_df = pd.read_csv(in_path)
    original_df.CODE = original_df.CODE.map('{:06d}'.format)

    result = pd.merge(original_df, result, on='CODE')
    file_name = out_path

    if os.path.isfile(file_name):
        object_df = pd.read_csv(file_name)

        if period == 'day':
            result.iloc[:, 10:16] = object_df.iloc[:, 10:16]
        elif period == 'week':
            result.loc[:, 'CUR PRICE'] = object_df.loc[:, 'CUR PRICE']
            result.loc[:, 'PREV PRICE'] = object_df.loc[:, 'PREV PRICE']
        elif period == 'all':
            result.loc[:, 'PER':"ITR"] = object_df.loc[:, 'PER':"ITR"]
            result.loc[:, 'CUR PRICE'] = object_df.loc[:, 'CUR PRICE']
            result.loc[:, 'PREV PRICE'] = object_df.loc[:, 'PREV PRICE']
    else:
        pass

    result = result.fillna('N/A')
    print(bcolors.HELP + "↓ Information about results is here ↓" + bcolors.ENDC)
    print(result.info())

    result.to_csv(file_name, encoding='utf-8-sig', index=False)
    print(bcolors.OKMSG + "Done Successfully!" + bcolors.ENDC)
    print('\n')




def initialize(input_list):
    for x in input_list:
        x = 0


# ========아래로 메인 코드입니다========= #
if __name__ == '__main__':
    start_time = time.time()

    measurements = pd.DataFrame(columns=("NAME", "CODE", 'CUR PRICE', 'PREV PRICE', 'FR', "PER", "EPS",
                                         "E_PER", "E_EPS", "PBR", "BPS", "ITR"))

    unit_all = [['KOSPI', 'noBank', 'all'],
                ['KOSDAQ', 'noBank', 'all']]

    price_ks = [['KOSPI', 'noBank', 'day']]

    unit_test = [['TEST', 'noBank', 'all'],
                 ['TEST2', 'noBank', 'all']]

    for a in unit_test:
        market = a[0]
        m_type = a[1]
        period = a[2]

        in_path = './DATA/' + market + '_' + m_type + '.csv'
        out_path = './DATA/Results/DATA_' + market + '_' + m_type + datetime.today().strftime("_%Y%m%d") + '.csv'

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

        globs = [global1, global2, global3, global4, global5, global6,
                 global7, global8, global9, global10, global11, global12]

        '''===
        market은 'KOSPI', 'KOSDAQ'중 하나를 선택합니다. 선택하지 않을 시 기본값은 'KOSPI'입니다.

        m_type은 'noBank'(지주회사, 리츠, 은행 등 금융회사 제외), 'noUse'(noBank에서 제외된 요소만), 'ALL'(제외 없이 모두 다 포함)-
        -중 하나를 선택합니다. 선택하지 않을 시 기본값은 'noBank'입니다.

        period 는 'day'와 'week' 중 하나를 선택합니다. 'day'를 선택할 경우 가격에 관한 정보가, 'week'을 선택할 경우 투자지표에 관한 정보가 
        수집됩니다. 
        ==='''

        code_list = get_code_list(market=market, m_type=m_type, in_path=in_path)

        # 멀티 프로세스는 속도 향상을 위해 필요합니다. n개의 프로세스를 사용해 속도를 n배로 끌어 올립니다.
        # 기본값은 8입니다.
        multiprocess(globs, period=period, code_list=code_list, in_path=in_path, out_path=out_path, numberOfThreads=6)

        initialize(globs)
        initialize(code_list)

    ex_time = time.time() - start_time

    print('\n')
    print(bcolors.OKMSG + "It took %dm %.2fs." % (ex_time / 60, round(ex_time, 2) % 60) + bcolors.ENDC)


