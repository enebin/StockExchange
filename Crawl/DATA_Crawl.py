# coding = utf-8-sig
import time
import bcolors
import pandas as pd
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


class PERMulti:
    def __init__(self, market='KOSPI', m_type='noBank', period='day'):
        self.market = market
        self.m_type = m_type
        self.period = period

        self.code_list = []
        self._get_code_list(market, m_type)

        self.measurements = pd.DataFrame(columns=("NAME", "CODE", 'CUR PRICE', 'PREV PRICE', 'FR',  "PER", "EPS",
                                                  "E_PER", "E_EPS", "PBR", "BPS", "ITR"))

        # 8개의 데이터프레임을 만들어줍니다. 각각의 프로세서가 사용할 데이터프레임입니다.
        manager = mp.Manager()
        self.global1 = manager.Namespace()
        self.global2 = manager.Namespace()
        self.global3 = manager.Namespace()
        self.global4 = manager.Namespace()
        self.global5 = manager.Namespace()
        self.global6 = manager.Namespace()
        self.global7 = manager.Namespace()
        self.global8 = manager.Namespace()
        self.global9 = manager.Namespace()
        self.global10 = manager.Namespace()
        self.global11 = manager.Namespace()
        self.global12 = manager.Namespace()

        self.globs = [self.global1, self.global2, self.global3, self.global4,
                      self.global5, self.global6, self.global7, self.global8,
                      self.global9, self.global10, self.global11, self.global12]

    # 종목 코드 리스트를 가져온 후 전처리합니다. init 함수에서 클래스 생성과 함께 실행됩니다.
    def _get_code_list(self, market, m_type):
        print(bcolors.WAITMSG + "Data processing for " + market + '_' + m_type + " starts now!" + bcolors.ENDC)

        df = pd.read_csv('./DATA/' + market + '_' + m_type + '.csv')
        df.CODE = df.CODE.map('{:06d}'.format)

        self.code_list = df.CODE.tolist()

    # 종목코드 하나를 받아 주가를 받아옵니다. [현재가, 전일종가]를 반환합니다.
    def _get_price(self, code, try_cnt):
        print(code)
        try:
            url = "http://asp1.krx.co.kr/servlet/krx.asp.XMLSiseEng?code={}".format(code)
            req = urlopen(url)
            result = req.read()

            xmlsoup = BeautifulSoup(result, "lxml-xml")
            curPrice = xmlsoup.find("TBL_StockInfo").attrs["CurJuka"]
            prevPrice = xmlsoup.find("TBL_StockInfo").attrs["PrevJuka"]

            curPrice = remove_coma(curPrice)
            prevPrice = remove_coma(prevPrice)

            time.sleep(0.5)

            return curPrice, prevPrice

        except HTTPError as e:
            logging.warning(e)
            if try_cnt >= 3:
                return None
            else:
                self._get_price(code, try_cnt=+1)

    # 종목코드 하나를 받아 투자지표를 크롤링합니다. [종목명, [투자지표]]를 반환합니다.
    def _get_data(self, input_code):
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

    # get_data 에서 크롤링한 데이터를 받아 데이터프레임에 저장하는 함수입니다.
    # 데이터프레임을 반환합니다.
    def _make_data_frame(self, name, input_code, price, values_raw):
        # 기본 데이터를 저장합니다.
        values = [name, input_code, price]

        # 크롤링으로 추출한 PER, PBR 값을 리스트에 저장합니다.
        for i in values_raw:
            content = i.text
            if content == 'N/A':
                values.append('N/A')
            else:
                values.append(remove_coma(content))

        # 1줄짜리 임시 데이터프레임에 값들을 저장합니다.
        data = pd.DataFrame(columns=("NAME", "CODE", "PRICE", "PER", "EPS", "E_PER", "E_EPS", "PBR", "BPS", "ITR"))
        data.loc[0] = values

        # 진행상황을 체크하며 값을 확인합니다.
        # print(data.tail(1))

        # 데이터프레임을 반환합니다.
        return data

    # 종목코드를 받아 주식의 현재가, 전일 종가, 등락률을 저장하는 함수입니다.
    # 데이터프레임을 반환합니다.
    def _make_price_frame(self, curPrice, prevPrice):
        res = pd.DataFrame(columns=['CODE', 'CUR PRICE', 'PREV PRICE', 'FR'])

        res.loc[0, 'CUR PRICE'] = curPrice
        res.loc[0, 'PREV PRICE'] = prevPrice
        res.loc[0, 'FR'] = float(get_FR(curPrice, prevPrice))

        return res

   # 쓰레딩을 위해 사용하는 스타트 함수입니다.
    def _starter(self, input_code, glob):
        if self.period == 'week':
            name, value_tag = self._get_data(input_code)

            # 에러상황(ETF, 리츠 등 펀드류 코드 경우)시 리턴합니다.
            if name == -1:
                logging.warning(input_code)
                return
            else:
                temp_row = self._make_data_frame(name, input_code, value_tag)
                glob.df = glob.df.append(temp_row)

        if self.period == 'day':
            curPrice, prevPrice = self._get_price(input_code, 1)
            temp_row = self._make_price_frame(curPrice, prevPrice)
            glob.df = glob.df.append(temp_row)

    # 프로그램이 끝났다면 CSV 파일로 저장한 후 종료합니다.
    def _merger(self):
        result = self.measurements
        for glob in self.globs:
            result = result.append(glob.df)

        # result = result.sort_values(["CODE"], ascending=True)

        # 결과 데이터와 에러상태에 관한 간략한 정보를 보여줍니다.
        print("\n")
        print(bcolors.HELP + "↓ Information about results is here ↓" + bcolors.ENDC)
        print(result.info())
        print(bcolors.OKMSG + "Finished! %d items were collected, except for %d errors"
              % (result.shape[0], len(self.code_list) - result.shape[0]))

        # 결과데이터를 csv 로 출력하기 위한 과정입니다.
        print(bcolors.WAITMSG + "Now processing output... " + bcolors.ENDC)
        result.drop(['NAME'], axis='columns', inplace=True)

        original_df = pd.read_csv('./DATA/' + self.market + '_' + self.m_type + '.csv')
        original_df.CODE = original_df.CODE.map('{:06d}'.format)

        result = pd.merge(original_df, result, on='CODE')
        result.to_csv('./DATA/Results/DATA_' + self.market + '_' + self.m_type + datetime.today().strftime("_%Y%m%d") +
                      '.csv', encoding='utf-8-sig', index=False)

        print(bcolors.OKMSG + "Done Successfully!" + bcolors.ENDC)

    def multiprocess(self, numberOfThreads=8):
        # 값들을 저장할 Pandas 데이터프레임을 구성합니다.
        # Multiprocessing 을 위한 전처리도 같이 합니다.
        # 프로세스의 개수 (MAX = 12)
        if numberOfThreads > 12:
            print(bcolors.ERRMSG + "Too much process. Check number of threads again")
            return

        if self.period == 'week':
            print(bcolors.OKMSG + "Processing type is: Investment index.")
        else:
            print(bcolors.OKMSG + "Processing type is: Price informations.")

        for glob in self.globs:
            glob.df = self.measurements

        # 데이터프레임을 프로세스 개수에 따라 분배하며 프로세스 개수는 성능 여유분에 따라 조절 가능합니다.
        processes = []
        index = 0
        for code in tqdm(self.code_list, desc="Processing DATA"):
            process = mp.Process(target=self._starter, args=(str(code), self.globs[index]))
            index = (index + 1) % numberOfThreads
            processes.append(process)

        print(bcolors.OKMSG + "Done Successfully" + bcolors.ENDC)
        print(bcolors.OKMSG + "Number of processes: " + str(numberOfThreads) + bcolors.ENDC)
        print(bcolors.WAITMSG + "Analysis starts now. " + bcolors.ITALIC + "FYI: Only stocks are included" + bcolors.ENDC)

        for i in chunks(processes, numberOfThreads):
            for j in i:
                j.start()
            for j in i:
                j.join()

            # 진행상황을 체크하며 값을 확인합니다.
            count = 0
            for glob in self.globs:
                count += glob.df.shape[0]
            tqdm(total=len(self.code_list)).update(count)

            # for glob in globs:
            #     print(glob.df.tail(1), end='\n')

        self._merger()


# ========아래로 메인 코드입니다========= #
if __name__ == '__main__':
    start_time = time.time()

    # market은 'KOSPI', 'KOSDAQ'중 하나를 선택합니다.
    # 선택하지 않을 시 기본값은 'KOSPI'입니다.
    # m_type은 'noBank'(지주회사, 리츠, 은행 등 금융회사 제외), 'noUse'(noBank에서 제외된 요소만), 'ALL'(제외 없이 모두 다 포함)-
    # -중 하나를 선택합니다. 선택하지 않을 시 기본값은 'noBank'입니다.
    pm1 = PERMulti(market='TEST', m_type='noBank')
    # pm2 = PERMulti(market='KOSDAQ', m_type='noBank')

    # 멀티 프로세스는 속도 향상을 위해 필요합니다. n개의 프로세스를 사용해 속도를 n배로 끌어 올립니다.
    # 기본값은 8입니다.
    pm1.multiprocess(numberOfThreads=2)
    # pm2.multiprocess(numberOfThreads=8)

    ex_time = time.time() - start_time

    print('\n')
    print(bcolors.OKMSG + "It took %dm %.2fs." % (ex_time / 60, round(ex_time, 2) % 60) + bcolors.ENDC)


