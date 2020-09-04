
import os
import sys

import datetime
import time

import uuid

from Utils.MySQLConn import MySQLConn
from Utils.PyTime import PyDating

from Core.GildataRem import GildataDb
from Core.GildataRem import GildataRem

from Utils.PyFile import PyFile

class StrategySignalUpdater():
    '''strategy signals updater
    this object could update signal-item to Dev-MySQL;
    '''
    class signalitem():

        def __init__(self, secu_code, entry_index, entry_price, strategy_name, strategy_type, strategy_item, strategy_code='none', strategy_version='none', market_code='2', id=None, entry_time=None):

            self.secu_code = secu_code if isinstance(secu_code,str) else str(100000+secu_code)[1:]
            self.entry_index = entry_index

            self.entry_price = entry_price
            # "0", "1", "2", ...
            self.strategy_name = strategy_name

            self.strategy_type = strategy_type
            # 策略类型{SHAPEX:形态选股; FINX:策略选股}
            # detail-name
            self.strategy_item = strategy_item

            self.strategy_code = strategy_code
            self.strategy_version = strategy_version

            self.market_code = market_code

            # new id for each item;
            self.id = str(uuid.uuid4()).replace("-", "")[:18] if id is None else id;
            self.entry_time = datetime.datetime.now().strftime("%Y-%m-%d") if entry_time is None else entry_time


    def __init__(self):

        self.db = MySQLConn(name="dev")
        self.strategy_types = ["SHAPEX", "FINX"]
        self.strategy_names = [
            ["上涨趋势跟踪", "新高突破", "长下影", "成交量上扬", "MACD低位金叉"],
            ["PB-ROE价值投资", "小盘价值投资", "成长投资", "现金为王投资", "高股息价值投资"]
        ]

    def pop(self, type, item):
        '''pop item'''

        # status==1: make it not valid...
        command = '''
        update stock_smart_pick set status=1 where strategy_type="{}" and strategy_item="{}"
        '''.format(type, item)
        self.db.commit(command)
        time.sleep(0.1)

    def delete(self, strategy_type=None, strategy_name=None):
        '''delete item'''

        if (strategy_type is None) and (strategy_name is None):
            command = "delete from stock_smart_pick"
            self.db.commit(command)
            time.sleep(0.1)


    def push(self, type, item, name, code, version='none', stocks_list = []):
        '''push item'''

        if len(stocks_list)==0:
            print("empty push data. return. pls. check.")
            return

        my_signals = []
        for iter, xx in enumerate(stocks_list):
            # {"secuCode": "00001", "entryTime": "2020-6-12", "entryPrice": "10.00"}
            # secuCode = item["secuCode"]
            # entryPrice = item["entryPrice"]
            item_ = self.signalitem(secu_code = xx["secuCode"],
                                    entry_index = iter,
                                    entry_price = xx["entryPrice"],
                                    entry_time = xx.get("entryTime",None),
                                    strategy_type = type,
                                    strategy_item = str(item),
                                    strategy_name = name,
                                    strategy_code = code,
                                    strategy_version = version
                                    )
            my_signals.append(item_)

        for xo in my_signals:

            # xo.status = 0, is valid.
            command = '''
            INSERT INTO stock_smart_pick
            (id, secu_code, market_code, entry_index, strategy_type, strategy_item, strategy_code, strategy_name, strategy_version, entry_price, entry_time, status)
            VALUES
            ("{}", "{}", "{}", {}, "{}", "{}", "{}", "{}", "{}", {}, "{}", 0)
            '''.format(xo.id, xo.secu_code, xo.market_code,
                       xo.entry_index, xo.strategy_type,
                       xo.strategy_item, xo.strategy_code,
                       xo.strategy_name, xo.strategy_version,
                       round(float(xo.entry_price),2),
                       xo.entry_time)

            self.db.commit(command)
            time.sleep(0.1)

        # write signals to locals.

        str_ = datetime.datetime.now().strftime("%Y%m%d")

        PyFile.GenPath("Jsignals")

        filename_ = "Jsignals/strategy_signals_{}.csv".format(str_)

        if not os.path.exists(filename_):
            with open(filename_, "w+") as f:
                str_ = "id, secu_code, market_code, entry_index, strategy_type, strategy_item, strategy_code, strategy_name, strategy_version, entry_price, entry_time, status"
                str_ = str_.replace(" ", "")
                f.write(str_+'\n')

        with open(filename_, "ab+") as f:
            for xo in my_signals:
                str_ = "{},{},{},{},{},{},{},{},{},{},{},0" \
                    .format(xo.id, xo.secu_code, xo.market_code,
                           xo.entry_index, xo.strategy_type,
                           xo.strategy_item, xo.strategy_code,
                           xo.strategy_name, xo.strategy_version,
                           round(float(xo.entry_price),2),
                           xo.entry_time)
                f.write((str_+'\n').encode("utf-8-sig"))

class JstrategyBase():

    def __init__(self, loaddata = True, selectdate = None):

        print("init of ::JstrategyBase::")

        self.status = True
        self.strategy_api_name = None
        self.GR = GildataRem(db=GildataDb())

        self.lastupdatedate = self.GR.get_lastupdatedate()
        self.today = self.GR.get_today()

        if loaddata:
            maxdate_trading = self.GR._load_trading_data()
            maxdate_quoting = self.GR._load_quoting_data()
            print("trading data update to {}".format(maxdate_trading))
            print("quoting data update to {}".format(maxdate_quoting))
        else:
            maxdate_trading = self.lastupdatedate
            maxdate_quoting = self.lastupdatedate

        self.selectdate = selectdate if (selectdate is not None) else self.lastupdatedate
        if (self.selectdate>maxdate_trading):
            print("trading data is not complete. return. pls. check!")
            self.status = False
            return
        if (self.selectdate>maxdate_quoting):
            print("quoting data is not complete. return. pls. check!")
            self.status = False
            return

        self._init_GR()

        self.signal_updater = StrategySignalUpdater()

    def print_info(self):

        print("today: {}".format(self.today))
        print("last update date: {}".format(self.lastupdatedate))

        print("now having {} tradedays for trader.".format(len(self.tradedays)) )
        print("now having {} stocks for trader.".format(len(self.secucode)) )

        print("*"*32)
        print("select date is {}".format(self.selectdate))
        print("*"*32)

    def _init_GR(self):
        '''
        初始化交易日期序列和股票池;
        股票池过滤：
        MV+Amount过滤;
        '''

        tradedays = self.GR.get_tradedays()
        tradedays = [item for item in tradedays if item<=self.selectdate]

        SecuCode, SecuInfo = self.GR.get_stocklist()
        len_secucode = len(SecuCode)
        self.GR.set_secucode(SecuCode)

        days1 = tradedays[-1]
        HKStkMV = self.GR.get_secucode_quotedata("HKStkMV", days1).fillna(0)
        limit_mv = 5 * 10000.0 * 10000.0

        days20 = tradedays[-20:]
        Amount20 = self.GR.get_secucode_tradedata("AmountValue", days20)
        limit_amount = 100 * 10000.0
        Amount20['Avg'] = Amount20.values.mean(axis=1)

        screen_flag = (HKStkMV[days1].values > limit_mv) & (Amount20["Avg"].fillna(0).values > limit_amount)
        secucode = [SecuCode[i] for i, flag in enumerate(screen_flag) if flag == True]

        trade_maxdays = 300
        tradedays = tradedays[-trade_maxdays:]

        self.tradedays = tradedays
        self.secucode = secucode

        self.GR.set_secucode(self.secucode)


    @staticmethod
    def get_strategy_api_name(**kwargs):

        strategy_type = kwargs.get('type','none')
        strategy_item = kwargs.get('item','none')
        strategy_name = kwargs.get('name','none')
        strategy_code = kwargs.get('code','none')
        strategy_version = kwargs.get('version','none')

        strategy_api_name = "strategy_api_{}{}_{}_{}".format(strategy_type.lower(),
                                    strategy_item,
                                    strategy_code,
                                    strategy_version)
        print("strategy_api_name: {}".format(strategy_api_name))
        return strategy_api_name

    def set_strategy_info(self, **kwargs):

        self.strategy_info = kwargs
        self.strategy_api_name = JstrategyBase.get_strategy_api_name(**kwargs)

        print("strategy_api_name: {}".format(self.strategy_api_name))

        self.strategy_type = kwargs.get("type","none")
        self.strategy_item = kwargs.get("item","none")
        self.strategy_name = kwargs.get("name","none")
        self.strategy_code = kwargs.get("code","none")
        self.strategy_version = kwargs.get("version","none")

    def run_strategy_api(self, *args, **kwargs):

        if self.strategy_api_name is None:
            print("strategy api is not found.")
            return;

        print("run strategy_api::{}".format(self.strategy_api_name))
        eval("self.{}".format(self.strategy_api_name))(*args, **kwargs)

    def _prepare_update(self):

        self.GR.set_secucode(self.stocks_pool)

        data = self.GR.get_secucode_tradedata("ClosePrice", self.selectdate)
        stocks_price = list(data[self.selectdate].values)

        updatetime = PyDating.dt_to_string(PyDating.ymd_int_to_dt(self.today), format='%Y-%m-%d')

        stocks_dict = []
        for i in range(len(stocks_price)):
            stocks_dict.append({'secuCode': self.stocks_pool[i], 'entryPrice': stocks_price[i], 'entryTime': updatetime})

        return stocks_dict

    def run_strategy_update(self):

        print("::strategy-update::")
        stocks_list = self._prepare_update()

        # print(stocks_list)
        print("submit stocks list:")
        [print(item) for item in stocks_list]

        print("\npop strategy of {}".format(self.strategy_name))
        self.signal_updater.pop(self.strategy_type, self.strategy_item)

        print("\npush strategy of {}".format(self.strategy_name))
        self.signal_updater.push(type=self.strategy_type,
                                 item=self.strategy_item,
                                 name=self.strategy_name,
                                 code=self.strategy_code,
                                 version=self.strategy_version,
                                 stocks_list=stocks_list)

    def set_strategy_max_num(self, num):
        self.strategy_max_num = num

