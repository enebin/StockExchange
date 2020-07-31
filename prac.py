from datetime import datetime
import pandas_datareader as web
import backtrader as bt


# Create a subclass of Strategy to define the indicators and logic
class SmaCross(bt.Strategy):
    # list of parameters which are configurable for the strategy
    params = dict(
        pfast=5,   # period for the fast moving average
        pslow= 10  # period for the slow moving average
    )

    def __init__(self, mode, prop):
        self.mode = mode
        self.amount = 0
        self.prop = float(prop)

        if mode == 'SMA':
            ma1 = bt.ind.SMA(period=self.p.pfast)  # fast moving average
            ma2 = bt.ind.SMA(period=self.p.pslow)  # slow moving average
        elif mode == 'EMA':
            ma1 = bt.ind.EMA(period=self.p.pfast)  # fast moving average
            ma2 = bt.ind.EMA(period=self.p.pslow)  # slow moving average

        self.crossover = bt.ind.CrossOver(ma1, ma2)  # crossover signal

        print("====It's result of %d-MA and %d-MA====\n" % (self.p.pfast, self.p.pslow))
        # print("====Just Hold====\n")
        print('Starting portfolio Value : {}$'.format(start_val))

    def next(self):
        if not self.position:  # not in the market
            if self.crossover > 0:  # if fast crosses slow to the upside
                # print(int(self.broker.getcash() / self.datas[0].close[0]))
                self.amount = int((self.broker.getcash() / self.datas[0].close[0]) * self.prop)
                self.buy(size=self.amount)  # enter long

        elif self.crossover < 0:  # in the market & cross to the downside
            # print('sell', amount)
            self.close(size=self.amount)  # close long position


cerebro = bt.Cerebro()  # create a "Cerebro" engine instance

# Create a data feed
start = '2018-01-01'
name = 'TSLA'
stockData = web.DataReader(name, 'yahoo', start,)[['Open','Close']]
data = bt.feeds.PandasData(dataname=stockData, open='Open', close='Close')

# data = bt.feeds.YahooFinanceData(dataname='MSFT', fromdate=datetime(2020, 1, 1), todate=datetime(2020, 6, 31))

cerebro.adddata(data)  # Add the data feed

cerebro.addstrategy(SmaCross('SMA', 0.9))  # Add the trading strategy

cerebro.broker.setcash(10000000.0)
cerebro.broker.setcommission(commission=0.001)
cerebro.addobserver(bt.observers.BuySell)

start_val = cerebro.broker.getvalue()
cerebro.run()
end_val = cerebro.broker.getvalue()
print('Final portfolio value : {}$'.format(round(end_val, 2)))

cerebro.plot(volume=False)  # and plot it with a single command


# Reference : https://www.backtrader.com/home/helloalgotrading/

'''
import pandas_datareader as web
import backtrader as bt


# Create a subclass of Strategy to define the indicators and logic
class SmaCross(bt.Strategy):
    # list of parameters which are configurable for the strategy
    params = dict(
        pfast=5,   # period for the fast moving average
        pslow= 10  # period for the slow moving average
    )

    def __init__(self):
        self.prop = 0.9
        self.amount = 0

        sma1 = bt.ind.EMA(period=self.p.pfast)  # fast moving average
        sma2 = bt.ind.EMA(period=self.p.pslow)  # slow moving average
        self.crossover = bt.ind.CrossOver(sma1, sma2)  # crossover signal
        print("====It's result of %d-MA and %d-MA====\n" % (self.p.pfast, self.p.pslow))
        # print("====Just Hold====\n")
        print('Starting portfolio Value : {}$'.format(start_val))


    def next(self):
        if not self.position:  # not in the market
            if self.crossover > 0:  # if fast crosses slow to the upside
                self.amount = int((self.broker.getcash() / self.datas[0].close[0]) * self.prop)
                #print(int(self.broker.getcash() / self.datas[0].close[0]))
                self.buy(size=self.amount)  # enter long

        elif self.crossover < 0:  # in the market & cross to the downside
            #print('sell', amount)
            self.close(size=self.amount)  # close long position


cerebro = bt.Cerebro()  # create a "Cerebro" engine instance

# Create a data feed
start = '2018-01-01'
name = 'TSLA'
stockData = web.DataReader(name, 'yahoo', start,)[['Open','Close']]
data = bt.feeds.PandasData(dataname=stockData, open='Open', close='Close')

# data = bt.feeds.YahooFinanceData(dataname='MSFT', fromdate=datetime(2020, 1, 1), todate=datetime(2020, 6, 31))

cerebro.adddata(data)  # Add the data feed

print(SmaCross)
cerebro.addstrategy(SmaCross)  # Add the trading strategy

cerebro.broker.setcash(10000000.0)
cerebro.broker.setcommission(commission=0.001)
cerebro.addobserver(bt.observers.BuySell)

start_val = cerebro.broker.getvalue()
cerebro.run()
end_val = cerebro.broker.getvalue()
print('Final portfolio value : {}$'.format(round(end_val, 2)))

cerebro.plot(volume=False)  # and plot it with a single command


# Reference : https://www.backtrader.com/home/helloalgotrading/
'''