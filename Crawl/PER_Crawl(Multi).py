# coding = utf-8-sig
import time
import bcolors
import pandas as pd
import multiprocessing as mp

from tqdm import tqdm
from urllib.request import urlopen
from bs4 import BeautifulSoup


# 콤마를 숫자에서 지워주는 편의성을 위한 함수입니다. 반환값은 float 형 입니다.
def remove_coma(input_no):
    try:
        return float(input_no.replace(',', ''))
    except ValueError:
        return float(input_no)


# 리스트를 일정한 크기로 분할하여줍니다. 멀티프로세싱의 쓰레딩을 위해 사용합니다.
def chunks(input_list, size):
    for element in range(0, len(input_list), size):
        yield input_list[element: element + size]


# 종목코드를 받아 크롤링하는 함수입니다.
def get_data(input_code):
    # 종목코드를 가져와 NAVER 증권 정보 URL에 대입합니다.
    url = "https://finance.naver.com/item/main.nhn?code=" + input_code

    # BS4를 이용한 HTML 소스 크롤링입니다.
    result = urlopen(url)
    html = result.read()
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
        print(bcolors.ERRMSG + "ERROR OCCURS" + bcolors.ITALIC + "\nPossible Error: It can be ETF, REITs, etc...")
        return -1, -1, [-1]
    

# get_data 에서 크롤링한 데이터를 받아 데이터프레임에 저장하는 함수입니다.
def save_data(name, input_code, price, values_raw):
    # 기본 데이터를 저장합니다.
    values = [name, input_code, price]

    # PER, PBR 테이플의 값을 리스트에 저장합니다.
    for i in values_raw:
        content = i.text
        if content == 'N/A':
            values.append('N/A')
        else:
            values.append(remove_coma(content))

    # 데이터프레임에 값들을 저장합니다.
    data = pd.DataFrame(columns=("NAME", "CODE", "PRICE", "PER", "EPS", "E_PER", "E_EPS", "PBR", "BPS", "ITR"))
    data.loc[0] = values

    # 진행상황을 체크하며 값을 확인합니다.
    # print(data.tail(1))

    return data


# 쓰레딩을 위해 사용하는 스타트 함수입니다.
def starter(input_code, Glob):
    name, price, value_tag = get_data(input_code)

    if name == -1:
        return
    else:
        temp_row = save_data(name, input_code, price, value_tag)
        Glob.df = Glob.df.append(temp_row)


# ========아래로 메인 코드입니다========= #
if __name__ == '__main__':
    print("======ANALYSIS STARTS=======")
    start_time = time.time()

    # 종목 코드 리스트를 가져온 후 전처리합니다.
    with open('KOSPI.txt', 'r') as f:
        CODES = f.readlines()
    f.close()

    CODES[0] = CODES[0].replace(" \'", "")
    CODES[0] = CODES[0].replace("\'", "")
    CODES[0] = CODES[0].replace("\' ", "")

    CODES = CODES[0].split(',')

    # 값들을 저장할 Pandas 데이터프레임을 구성합니다.
    # Multiprocessing 을 위한 전처리도 같이 합니다.
    numberOfThreads = 8    # 프로세스의 개수 (MAX = 8)

    # 8개의 데이터프레임을 만들어줍니다.
    manager = mp.Manager()
    Global1 = manager.Namespace()
    Global2 = manager.Namespace()
    Global3 = manager.Namespace()
    Global4 = manager.Namespace()
    Global5 = manager.Namespace()
    Global6 = manager.Namespace()
    Global7 = manager.Namespace()
    Global8 = manager.Namespace()

    Globs = [Global1, Global2, Global3, Global4, Global5, Global6, Global7, Global8]
    measurements = pd.DataFrame(columns=("NAME", "CODE", "PRICE", "PER", "EPS", "E_PER",
                                         "E_EPS", "PBR", "BPS", "ITR"))
    for Glob in Globs:
        Glob.df = measurements

    # 멀티 프로세스는 속도 향상을 위해 필요합니다. 8개의 프로세스를 사용해 속도를 8배로 끌어 올립니다.
    # 프로세스 개수는 성능 여유분에 따라 조절 가능합니다.
    processes = []
    index = 0

    for code in tqdm(CODES, desc="Processing"):
        process = mp.Process(target=starter, args=(str(code), Globs[index]))
        index = (index + 1) % numberOfThreads
        processes.append(process)

    print(bcolors.OKMSG + "Done Successfully" + bcolors.ENDC)
    print(bcolors.WAITMSG + "Starting now. " + bcolors.ITALIC + "FYI: Only stocks are included" + bcolors.ENDC)

    for i in chunks(processes, numberOfThreads):
        for j in i:
            j.start()
        for j in i:
            j.join()

        # 진행상황을 체크하며 값을 확인합니다.
        count = 0
        for Glob in Globs:
            count += Glob.df.shape[0]
        tqdm(total=len(CODES)).update(count)

        # for Glob in Globs:
        #     print(Glob.df.tail(1), end='\n')

    # 프로그램이 끝났다면 CSV 파일로 저장한 후 종료합니다.
    ex_time = time.time() - start_time
    result = measurements
    for Glob in Globs:
        result = result.append(Glob.df)

    print("\n")
    print(bcolors.HELP + "↓ Information about results is here ↓" + bcolors.ENDC)
    print(result.info())
    print(bcolors.OKMSG + "Finished! It took %.2fs." % round(ex_time, 2) + bcolors.ENDC)

    result.to_csv('measurements_MULTI.csv', encoding='utf-8-sig')