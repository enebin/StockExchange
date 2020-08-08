# coding = utf-8-sig
import time
import bcolors
import pandas as pd
import multiprocessing as mp

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


class PERMulti:
    def __init__(self, market='KOSPI'):
        self.market = market
        self.code_list = []
        self._get_code_list(market)
        self.measurements = pd.DataFrame(columns=("NAME", "CODE", "PRICE", "PER", "EPS",
                                                  "E_PER", "E_EPS", "PBR", "BPS", "ITR"))

        # 8개의 데이터프레임을 만들어줍니다.
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

    # 종목 코드 리스트를 가져온 후 전처리합니다.
    def _get_code_list(self, market):
        print(bcolors.WAITMSG + "Data colloection for " + market + " starts now!" + bcolors.ENDC)

        with open(market + '.txt', 'r') as f:
            self.code_list = f.readlines()

        self.code_list[0] = self.code_list[0].replace(" \'", "")
        self.code_list[0] = self.code_list[0].replace("\'", "")
        self.code_list[0] = self.code_list[0].replace("\' ", "")

        self.code_list = self.code_list[0].split(',')

    # 종목코드를 받아 웹크롤링합니다.
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

            # 소스에서 현재가를 찾아 저장합니다. (주로 크롤링한 날 종가)
            price = soup.find("p", {"class": "no_today"}).text
            price = remove_coma(price.split('\n')[2])

            # HTML 소스에서 투자지표 Table을 찾아 저장한 후 값을 추출합니다.
            table_tag = soup.find("table", {"class": "per_table"})
            values_raw = table_tag.find_all("em")

            return name, price, values_raw

        except AttributeError:
            print(bcolors.ERRMSG + "ERROR OCCURS\n" + bcolors.ITALIC +
                  "Possible Error: It can be ETF, REITs, etc...")
            return -1, -1, [-1]

    # get_data 에서 크롤링한 데이터를 받아 데이터프레임에 저장하는 함수입니다.
    def _save_data(self, name, input_code, price, values_raw):
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

    # 프로그램이 끝났다면 CSV 파일로 저장한 후 종료합니다.
    def _merger(self):
        result = self.measurements
        for glob in self.globs:
            result = result.append(glob.df)

        print("\n")
        print(bcolors.HELP + "↓ Information about results is here ↓" + bcolors.ENDC)
        print(result.info())

        number_of_error = len(self.code_list) - result.shape[0]
        print(bcolors.OKMSG + "Finished! %d items were collected, except for %d errors"
              % (result.shape[0], number_of_error))

        result.to_csv('DATA_' + self.market + datetime.today().strftime("_%Y%m%d") +
                      '.csv', encoding='utf-8-sig', index=False)

    # 쓰레딩을 위해 사용하는 스타트 함수입니다.
    def _starter(self, input_code, glob):
        name, price, value_tag = self._get_data(input_code)

        # 에러상황(ETF, 리츠 등 펀드류 코드 경우)시 리턴합니다.
        if name == -1:
            return
        else:
            temp_row = self._save_data(name, input_code, price, value_tag)
            glob.df = glob.df.append(temp_row)

    def multiprocess(self, numberOfThreads=8):
        # 값들을 저장할 Pandas 데이터프레임을 구성합니다.
        # Multiprocessing 을 위한 전처리도 같이 합니다.
        # 프로세스의 개수 (MAX = 12)
        if numberOfThreads > 12:
            print(bcolors.ERRMSG + "Too much process. Check number of threads again")
            return

        for glob in self.globs:
            glob.df = self.measurements

        # 멀티 프로세스는 속도 향상을 위해 필요합니다. n개의 프로세스를 사용해 속도를 n배로 끌어 올립니다.
        # 데이터프레임을 프로세스 개수에 따라 분배하며 프로세스 개수는 성능 여유분에 따라 조절 가능합니다.
        processes = []
        index = 0
        for code in tqdm(self.code_list, desc="Processing"):
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

    pm = PERMulti(market='test')
    pm.multiprocess(numberOfThreads=4)

    ex_time = time.time() - start_time

    print('\n')
    print(bcolors.OKMSG + "It took %dm %.2fs." % (ex_time / 60, round(ex_time, 2) % 60) + bcolors.ENDC)


