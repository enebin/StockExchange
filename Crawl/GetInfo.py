import pandas as pd
from tqdm import tqdm

market = 'KOSDAQ_ALL'
df = pd.read_csv(market + '.csv')

df.종목코드 = df.종목코드.map('{:06d}'.format)
print(df.shape[0])

df_NoUse = pd.DataFrame()
to_drop = []
for i in tqdm(df.index):
    name = df.loc[i, '업종']

    if name == "보험업" or name == "기타 금융업" or name == "은행 및 저축기관" or name == "부동산 임대 및 공급업":
        df_NoUse = df_NoUse.append(df.loc[i, :])
        to_drop.append(i)


print(df_NoUse.shape[0])

df.drop(df.index[to_drop], axis='rows', inplace=True)
print(df.shape[0])

df_NoUse.to_csv(market + "_noUse.csv", encoding='utf-8-sig')
df.to_csv(market + "_noBank.csv", encoding='utf-8-sig')


# 보험업, 기타 금융업, 은행 및 저축기관, 부동산 임대 및 공급업
