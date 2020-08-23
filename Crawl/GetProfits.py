# coding = utf-8-sig
import os.path
import time
import bcolors
import pandas as pd
import multiprocessing as mp
import logging

from datetime import datetime
from tqdm import tqdm
from urllib.request import urlopen
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


def initialize(input_list):
    for x in input_list:
        x = 0


# 종목 코드 리스트를 가져온 후 전처리합니다. init 함수에서 클래스 생성과 함께 실행됩니다.
def get_code_list(market, m_type, in_path):
    print(bcolors.WAITMSG + "Data processing for " + market + '_' + m_type + " starts now!" + bcolors.ENDC)

    df = pd.read_csv(in_path)
    df.CODE = df.CODE.map('{:06d}'.format)

    code_list = df.CODE.tolist()

    return code_list


# 쓰레딩을 위해 사용하는 스타트 함수입니다.
def starter(input_code, glob):
    name, profit_list = get_all(input_code)

    # 에러상황(ETF, 리츠 등 펀드류 코드 경우)시 리턴합니다.
    if name == -1:
        logging.warning(input_code)
        return
    else:
        temp_row = make_all_frame(name, input_code, profit_list)
        glob.df = glob.df.append(temp_row)


# 주가와 지표 모두 받아옵니다.
def get_all(input_code):
    # 종목코드를 가져와 NAVER 증권 정보 URL에 대입합니다.
    url = "https://finance.naver.com/item/main.nhn?code=" + input_code

    # BS4를 이용한 HTML 소스 크롤링입니다.
    url_result = urlopen(url)
    html = url_result.read()
    soup = BeautifulSoup(html, 'lxml')

    # 잘못된 코드나 ETF를 솎아내기 위해 예외처리를 하였습니다.
    try:
        url = "https://finance.naver.com/item/main.nhn?code=" + input_code
        # BS4를 이용한 HTML 소스 크롤링입니다.
        url_result = urlopen(url)
        html = url_result.read()
        soup = BeautifulSoup(html, 'lxml')

        # 이름을 받아온다
        name = soup.find("div", {"class": "wrap_company"}).find("h2").text
        
        # 제무재표를 받아온다
        financial_stmt = pd.read_html(html, encoding='euc-kr')[3]
        financial_stmt.set_index(('주요재무정보', '주요재무정보', '주요재무정보'), inplace=True)
        financial_stmt.index.rename('주요재무정보', inplace=True)
        financial_stmt.columns = financial_stmt.columns.droplevel(2)
        financial_stmt = financial_stmt.iloc[1, :]
        profits_list = financial_stmt.tolist()

        return name, profits_list

    except AttributeError:
        print(bcolors.ERRMSG + "ERROR OCCURS\n" + bcolors.ITALIC +
              "Possible Error: It can be REITs, Transaction Suspension etc...")
        logging.warning(input_code)
        return -1, [-1]


def make_all_frame(name, input_code, profit_list):
    # 1줄짜리 임시 데이터프레임을 구성하여 데이터를 저장합니다.
    temp_data = pd.DataFrame(columns=("NAME", "CODE", "2017.12", "2018.12", "2019.12", "2020.12(E)", "2019.06",
                                      "2019.09", "2019.12", "2020.03", "2020.06", "2020.09(E)"))

    temp_data.loc[0, 'NAME'] = name
    temp_data.loc[0, 'CODE'] = input_code
    temp_data.loc[0, '2017.12':"2020.09(E)"] = profit_list

    # 진행상황을 체크하며 값을 확인합니다.
    # print(temp_data.tail(1))

    # 데이터프레임을 반환합니다.
    return temp_data


# 프로그램이 끝났다면 CSV 파일로 저장한 후 종료합니다.
def merger(globs, code_list, in_path, out_path):
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
        result.loc[:, '2017.12':'2020.09(E)'] = object_df.loc[:, '2017.12':'2020.09(E)']
    else:
        pass

    result = result.fillna('N/A')
    print(bcolors.HELP + "↓ Information about results is here ↓" + bcolors.ENDC)
    print(result.info())

    result.to_csv(file_name, encoding='utf-8-sig', index=False)
    print(bcolors.OKMSG + "Done Successfully!" + bcolors.ENDC)
    print('\n')


def multiprocess(globs, code_list, in_path, out_path, numberOfThreads=8):
    # 값들을 저장할 Pandas 데이터프레임을 구성합니다.
    # Multiprocessing 을 위한 전처리도 같이 합니다.
    # 프로세스의 개수 (MAX = 18)
    if numberOfThreads > 18:
        print(bcolors.ERRMSG + "Too much process. Check number of threads again")
        return

    for glob in globs:
        glob.df = measurements

    # 데이터프레임을 프로세스 개수에 따라 분배하며 프로세스 개수는 성능 여유분에 따라 조절 가능합니다.
    processes = []
    index = 0
    for code in tqdm(code_list, desc="Processing DATA"):
        process = mp.Process(target=starter, args=(str(code), globs[index]))
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

    merger(globs, code_list, in_path, out_path)


# ========아래로 메인 코드입니다========= #
if __name__ == '__main__':
    start_time = time.time()

    measurements = pd.DataFrame(columns=("NAME", "CODE", "2017.12", "2018.12", "2019.12", "2020.12(E)", "2019.06",
                                         "2019.09", "2019.12", "2020.03", "2020.06", "2020.09(E)"))

    unit_all = [['KOSPI', 'noBank', 'all'],
                ['KOSDAQ', 'noBank', 'all']]

    price_ks = [['KOSPI', 'noBank', 'day']]

    unit_test = [['TEST', 'noBank', 'all'],
                 ['TEST2', 'noBank', 'all']]

    for a in price_ks:
        market = a[0]
        m_type = a[1]

        in_path = './DATA/' + market + '_' + m_type + '.csv'
        out_path = './DATA/Results/PROFIT_' + market + '_' + m_type + datetime.today().strftime("_%Y%m%d") + '.csv'

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
        multiprocess(globs, code_list=code_list, in_path=in_path, out_path=out_path, numberOfThreads=8)

        initialize(globs)
        initialize(code_list)

    ex_time = time.time() - start_time

    print('\n')
    print(bcolors.OKMSG + "It took %dm %.2fs." % (ex_time / 60, round(ex_time, 2) % 60) + bcolors.ENDC)


