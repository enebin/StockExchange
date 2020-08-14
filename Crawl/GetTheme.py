from pykiwoom.kiwoom import *
import pandas as pd

kiwoom = Kiwoom()
kiwoom.CommConnect(block=True)

'''
theme = pd.DataFrame(list(kiwoom.GetThemeGroupList(1).items()), columns=['Theme name', 'Theme code'])
theme.to_csv('Theme Information.csv', encoding='utf-8-sig')
'''

theme = pd.read_csv('Theme Information.csv')
theme_code = theme.loc[:, 'Theme code']

save_dict = {}
index = 0
for item in theme_code:
    tickers = kiwoom.GetThemeGroupCode(str(item))
    save_dict[theme.loc[index, 'Theme name']] = tickers
    index += 1

target = pd.read_csv('./DATA/KOSPI_noBank.csv')
temp = pd.DataFrame(columns=['CODE', 'THEME'])

for name, codes in save_dict.items():
    for c in codes:
        temp.loc[]



'''
print(target.columns)
'''
