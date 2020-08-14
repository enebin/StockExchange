from pykiwoom.kiwoom import *

kiwoom = Kiwoom()
kiwoom.CommConnect(block=True)

group = kiwoom.GetThemeGroupList(1)
print(group)
tickers = kiwoom.GetThemeGroupCode('141')
print(tickers)