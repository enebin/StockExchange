import pandas as pd
from tqdm import tqdm

m_flag = 1

if m_flag == 1:
    market = 'KOSDAQ'
else:
    market = 'KOSPI'

df = pd.read_csv('./DATA/' + market + '_ALL.csv')

df.CODE = df.CODE.map('{:06d}'.format)
print(df.shape[0])

df_NoUse = pd.DataFrame()
to_drop = []
for i in tqdm(df.index):
    name = df.loc[i, 'SECT.']
    if name == "보험업" or name == "기타 금융업" or name == "은행 및 저축기관" or name == "부동산 임대 및 공급업" \
            or name == "회사 본부 및 경영 컨설팅 서비스업" or name == "금융 지원 서비스업":
        df_NoUse = df_NoUse.append(df.loc[i, :])
        to_drop.append(i)

print(df_NoUse.shape[0])
df.drop(df.index[to_drop], axis='rows', inplace=True)
print(df.shape[0])

df_NoUse.to_csv('./DATA/' + market + "_noUse.csv", encoding='utf-8-sig')
df.to_csv('./DATA/' + market + "_noBank.csv", encoding='utf-8-sig')