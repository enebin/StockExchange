import pandas_datareader as web
import backtrader as bt


# Create a subclass of Strategy to define the indicators and logic
class SmaCross(bt.Strategy):
    # list of parameters which are configurable for the strategy
    params = dict(
        pfast=5,   # period for the fast moving average
        pslow=10  # period for the slow moving average
    )

    def __init__(self):
        self.prop = 0.9
        self.amount = 0

        sma1 = bt.ind.SMA(period=self.p.pfast)  # fast moving average
        sma2 = bt.ind.SMA(period=self.p.pslow)  # slow moving average
        self.crossover = bt.ind.CrossOver(sma1, sma2)  # crossover signal
        print("====It's result of %d-MA and %d-MA(SMA)====" % (self.p.pfast, self.p.pslow))
        # print("====Just Hold====\n")
        print('Starting portfolio Value : {}$\n'.format(start_val))

    def next(self):
        if not self.position:  # not in the market
            if self.crossover > 0:  # if fast crosses slow to the upside
                self.amount = int((self.broker.getcash() / self.datas[0].close[0]) * self.prop)
                #print(int(self.broker.getcash() / self.datas[0].close[0]))
                self.buy(size=self.amount)  # enter long

        elif self.crossover < 0:  # in the market & cross to the downside
            #print('sell', amount)
            self.close(size=self.amount)  # close long position


class EmaCross(bt.Strategy):
    # list of parameters which are configurable for the strategy
    params = dict(
        pfast=5,   # period for the fast moving average
        pslow= 10  # period for the slow moving average
    )

    def __init__(self):
        self.prop = 0.9
        self.amount = 0

        ema1 = bt.ind.EMA(period=self.p.pfast)  # fast moving average
        ema2 = bt.ind.EMA(period=self.p.pslow)  # slow moving average
        self.crossover = bt.ind.CrossOver(ema1, ema2)  # crossover signal
        print("====It's result of %d-MA and %d-MA(EMA)====\n" % (self.p.pfast, self.p.pslow))
        # print("====Just Hold====\n")
        print('Starting portfolio Value : {}$'.format(start_val))


    def next(self):
        if not self.position:  # not in the market
            if self.crossover > 0:  # if fast crosses slow to the upside
                self.amount = int((self.broker.getcash() / self.datas[0].close[0]) * self.prop)
                self.buy(size=self.amount)  # enter long

        elif self.crossover < 0:  # in the market & cross to the downside
            self.close(size=self.amount)  # close long position


class WmaCross(bt.Strategy):
    # list of parameters which are configurable for the strategy
    params = dict(
        pfast=5,   # period for the fast moving average
        pslow= 10  # period for the slow moving average
    )

    def __init__(self):
        self.prop = 0.9
        self.amount = 0

        wma1 = bt.ind.WMA(period=self.p.pfast)  # fast moving average
        wma2 = bt.ind.WMA(period=self.p.pslow)  # slow moving average
        self.crossover = bt.ind.CrossOver(wma1, wma2)  # crossover signal
        print("====It's result of %d-MA and %d-MA(WMA)====\n" % (self.p.pfast, self.p.pslow))
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

    '''
    weights = range(1, period + 1)
    coef = 2 / (period * (period + 1))
    movav = coef * Sum(weight[i] * data[period - i] for i in range(period))
    '''


cerebro = bt.Cerebro()  # create a "Cerebro" engine instance

# Create a data feed
start = '2018-01-01'
name = 'AAPL'
stockData = web.DataReader(name, 'yahoo', start,)[['Open','Close']]
data = bt.feeds.PandasData(dataname=stockData, open='Open', close='Close')

cerebro.adddata(data)  # Add the data feed

strategies = [SmaCross, EmaCross, WmaCross]

cerebro.addstrategy(strategies[2], pfast=5, pslow=10)  # Add the trading strategy

cerebro.broker.setcash(10000000.0)
cerebro.broker.setcommission(commission=0.001)
cerebro.addobserver(bt.observers.BuySell)

start_val = cerebro.broker.getvalue()
cerebro.run()
end_val = cerebro.broker.getvalue()
print('Final portfolio value    : {}$\n'.format(round(end_val, 2)))

cerebro.plot(volume=False)  # and plot it with a single command

# Reference : https://www.backtrader.com/home/helloalgotrading/