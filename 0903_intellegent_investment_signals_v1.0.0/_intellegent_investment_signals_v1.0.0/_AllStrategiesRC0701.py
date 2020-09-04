import numpy as np
import pandas as pd

from Utils.talib_ import MACD

from Core.StrategyConnRC0701 import JstrategyBase

class Jstrategy(JstrategyBase):
    '''
    detail for strategy.
    '''
    def __init__(self,*args, **kwargs):
        super(Jstrategy, self).__init__(*args, **kwargs)


    ## FINANCE STRATEGY;
    '''
    type: 策略选股; name: PB-ROE估值; item: 0; code: pbroejiazhitouzi; version: rc20200616;
    '''
    def strategy_api_finx0_pbroeguzhi_rc20200616(self):

        print('start strategy computation.')
        tradedays, secucode, selectdate = self.tradedays, self.secucode, self.selectdate

        findata = pd.DataFrame(data=None, index=secucode)

        data = self.GR.get_secucode_financeindex_byfields(["TotalShareholderEquity", "EarningAfterTax"], (2019, 3))
        findata['earn2019'] = data['EarningAfterTax'].values
        findata['roe2019'] = data['EarningAfterTax'].values / data['TotalShareholderEquity'].values
        findata.loc[findata['earn2019'] < 0, 'roe2019'] = np.nan

        data = self.GR.get_secucode_financeindex_byfields(["TotalShareholderEquity", "EarningAfterTax"], (2018, 3))
        findata['earn2018'] = data['EarningAfterTax'].values
        findata['roe2018'] = data['EarningAfterTax'].values / data['TotalShareholderEquity'].values
        findata.loc[findata['earn2018'] < 0, 'roe2018'] = np.nan

        data = self.GR.get_secucode_financeindex_byfields(["TotalShareholderEquity", "EarningAfterTax"], (2017, 3))
        findata['earn2017'] = data['EarningAfterTax'].values
        findata['roe2017'] = data['EarningAfterTax'].values / data['TotalShareholderEquity'].values
        findata.loc[findata['earn2017'] < 0, 'roe2017'] = np.nan

        data = self.GR.get_secucode_financeindex_byfields(['OperatingIncome', 'OperatingProfit'], (2019, 3))
        findata['Sales2019'] = data['OperatingIncome'].values
        findata['OperatingProfit2019'] = data['OperatingProfit'].values

        data = self.GR.get_secucode_financeindex_byfields(['OperatingIncome', 'OperatingProfit'], (2018, 3))
        findata['Sales2018'] = data['OperatingIncome'].values
        findata['OperatingProfit2018'] = data['OperatingProfit'].values

        quote_fields = ['HKStkMV', 'PERatio', 'PB']
        data = self.GR.get_secucode_quotedata_byfields(quote_fields, selectdate)
        for field in quote_fields:
            findata[field] = data[field].values

        screen = pd.DataFrame(data=None, index=secucode)

        findata = findata.fillna(0)

        screen['score1'] = findata['roe2019'].values > 0.1
        screen['score2'] = findata['roe2018'].values > 0.1
        screen['score3'] = findata['roe2017'].values > 0.1
        screen['score4'] = findata['OperatingProfit2019'].values > findata['OperatingProfit2018'].values * 1.1
        screen['score5'] = findata['roe2019'].values > findata['roe2018'].values
        screen['score6'] = findata['Sales2019'].values > findata['Sales2018'].values
        screen['score7'] = findata['OperatingProfit2019'].values > findata['OperatingProfit2018'].values
        screen['earn2019'] = findata['earn2019']

        findata.loc[findata['PB'] <= 0, 'PB'] = np.nan
        findata.loc[findata['earn2019'] < 0, 'roe2019'] = np.nan

        total_scores = screen[['score1', 'score2', 'score3', 'score4', 'score5', 'score6', 'score7']].astype(int)

        screen['total_scores'] = np.sum(total_scores, axis=1)
        screen['index'] = findata['roe2019'].values / findata['PB'].values
        screen = screen.fillna(0.0)
        screen = screen.loc[screen['total_scores'] == 7, :].copy()
        screen = screen.sort_values(by=['index'], ascending=False)

        #
        stocks_pool = list(screen.iloc[:self.strategy_max_num].index)
        self.stocks_pool = stocks_pool
        print(self.stocks_pool)
        return self.stocks_pool


    '''
    type: 策略选股; name: 小盘价值; item: 1; code: xiaopanjiazhitouzi; version: rc20200616;
    '''
    def strategy_api_finx1_xiaopanjiazhi_rc20200616(self):

        print('start strategy computation.')
        tradedays, secucode, selectdate = self.tradedays, self.secucode, self.selectdate

        findata = pd.DataFrame(data=None, index=secucode)

        myfields = ["EarningAfterTax", "NetOperateCashFlow", "NonurrentLiability", "TotalLiability", 'OperatingIncome']
        data = self.GR.get_secucode_financeindex_byfields(myfields, (2019, 3))

        findata['earn2019'] = data['EarningAfterTax'].values
        findata['cash2019'] = data['NetOperateCashFlow'].values
        findata['longdebttodebt2019'] = data['NonurrentLiability'].values / data['TotalLiability'].values
        findata['grossmargin2019'] = data['EarningAfterTax'].values / data['OperatingIncome'].values

        myfields = ["EarningAfterTax", "NetOperateCashFlow", "NonurrentLiability", "TotalLiability", 'OperatingIncome']
        data = self.GR.get_secucode_financeindex_byfields(myfields, (2018, 3))

        findata['earn2018'] = data['EarningAfterTax'].values
        findata['cash2018'] = data['NetOperateCashFlow'].values
        findata['longdebttodebt2018'] = data['NonurrentLiability'].values / data['TotalLiability'].values
        findata['grossmargin2018'] = data['EarningAfterTax'].values / data['OperatingIncome'].values

        # findata['roe2019'] = data['EarningAfterTax'].values / data['TotalShareholderEquity'].values
        # findata.loc[findata['earn2019']<0,'roe2019'] = np.nan

        quote_fields = ['HKStkMV', 'PERatio', 'PB']
        data = self.GR.get_secucode_quotedata_byfields(quote_fields, selectdate)
        for field in quote_fields:
            findata[field] = data[field].values

        findata['sort_mv'] = np.argsort(np.argsort(findata["HKStkMV"].values))
        findata.loc[np.isnan(findata["HKStkMV"].values), "HKStkMV"] = len(findata)
        findata['sort_pe'] = np.argsort(np.argsort(findata["PERatio"].values))
        findata.loc[np.isnan(findata["PERatio"].values), "PERatio"] = len(findata)
        findata['sort_pb'] = np.argsort(np.argsort(findata["PB"].values))
        findata.loc[np.isnan(findata["PB"].values), "PB"] = len(findata)

        findata['sort_mv_pe_pb'] = findata['sort_mv'].values + findata['sort_pe'].values + findata['sort_pb'].values

        findata = findata.fillna(0)

        screen = pd.DataFrame(data=None, index=secucode)

        screen["score1"] = findata['earn2019'].values > 0
        screen["score2"] = findata['cash2019'].values > 0
        screen["score3"] = findata['longdebttodebt2018'].values < findata['longdebttodebt2019'].values
        screen["score4"] = findata['grossmargin2019'].values > findata['grossmargin2018'].values

        total_scores = screen[['score1', 'score2', 'score3', 'score4']].astype(int)
        screen['total_scores'] = np.sum(total_scores, axis=1)
        screen["index"] = findata["sort_mv_pe_pb"].values

        screen = screen.loc[ (screen['total_scores'] == 4) & (~np.isnan(screen['index'].values)), :].copy()
        screen = screen.sort_values(by=['index'], ascending=True)

        #
        stocks_pool = list(screen.iloc[:self.strategy_max_num].index)
        self.stocks_pool = stocks_pool
        print(self.stocks_pool)
        return self.stocks_pool


    '''
    type: 策略选股; name: 高成长; item: 2; code: gaochengzhang; version: rc20200616;
    '''
    def strategy_api_finx2_gaochengzhang_rc20200616(self):

        print('start strategy computation.')
        tradedays, secucode, selectdate = self.tradedays, self.secucode, self.selectdate

        findata = pd.DataFrame(data=None, index=secucode)

        myfields = ["EarningAfterTax", "NetOperateCashFlow", "OperatingIncome"]
        data = self.GR.get_secucode_financeindex_byfields(myfields, (2019, 3))

        findata['earn2019'] = data['EarningAfterTax'].values
        findata['sales2019'] = data["OperatingIncome"].values
        findata['cash2019'] = data["NetOperateCashFlow"].values

        myfields = ["EarningAfterTax", "NetOperateCashFlow", "OperatingIncome"]
        data = self.GR.get_secucode_financeindex_byfields(myfields, (2018, 3))

        findata['earn2018'] = data['EarningAfterTax'].values
        findata['sales2018'] = data["OperatingIncome"].values
        findata['cash2018'] = data["NetOperateCashFlow"].values

        findata = findata.fillna(0)

        screen = pd.DataFrame(data=None, index=secucode)

        screen['score1'] = findata['earn2019'].values > findata['earn2018'].values
        screen['score2'] = findata['sales2019'].values > findata['sales2018'].values
        screen['score3'] = findata['cash2018'].values > 0
        screen['score4'] = findata['cash2019'].values > 0
        screen['score5'] = findata['earn2019'].values > 0

        total_scores = screen[['score1', 'score2', 'score3', 'score4', 'score5']].astype(int)
        screen['total_scores'] = np.sum(total_scores, axis=1)

        quote_fields = ['HKStkMV', 'PERatio', 'PB']
        data = self.GR.get_secucode_quotedata_byfields(quote_fields, selectdate)
        for field in quote_fields:
            findata[field] = data[field].values

        screen["index"] = findata["PB"].values

        screen = screen.loc[(screen['total_scores'] == 5) & ~np.isnan(screen['index'].values), :].copy()
        screen = screen.sort_values(by=['index'], ascending=True, na_position='last')

        #
        stocks_pool = list(screen.iloc[:self.strategy_max_num].index)
        self.stocks_pool = stocks_pool
        print(self.stocks_pool)
        return self.stocks_pool

    '''
    type: 策略选股; name: 现金为王; item: 3; code: xianjinweiwang; version: rc20200616; 
    '''
    def strategy_api_finx3_xianjinweiwang_rc20200616(self):

        print('start strategy computation.')
        tradedays, secucode, selectdate = self.tradedays, self.secucode, self.selectdate

        # many fields:
        # eps2019>0, eps2018>0, eps2017>0
        # eps2019>eps2018
        # sales2019>sales2018
        # sales2018>sales2017
        # netcashflow of 2019, 2018, 2017>0

        # using pe as indicator;

        findata = pd.DataFrame(data=None, index=secucode)

        myfields = ["EarningAfterTax", "NetOperateCashFlow", "OperatingIncome"]
        data = self.GR.get_secucode_financeindex_byfields(myfields, (2019, 3))
        findata['earn2019'] = data['EarningAfterTax'].values
        findata['sales2019'] = data["OperatingIncome"].values
        findata['cash2019'] = data["NetOperateCashFlow"].values

        myfields = ["EarningAfterTax", "NetOperateCashFlow", "OperatingIncome"]
        data = self.GR.get_secucode_financeindex_byfields(myfields, (2018, 3))
        findata['earn2018'] = data['EarningAfterTax'].values
        findata['sales2018'] = data["OperatingIncome"].values
        findata['cash2018'] = data["NetOperateCashFlow"].values

        myfields = ["EarningAfterTax", "NetOperateCashFlow", "OperatingIncome"]
        data = self.GR.get_secucode_financeindex_byfields(myfields, (2017, 3))
        findata['earn2017'] = data['EarningAfterTax'].values
        findata['sales2017'] = data["OperatingIncome"].values
        findata['cash2017'] = data["NetOperateCashFlow"].values

        findata = findata.fillna(0)

        screen = pd.DataFrame(data=None, index=secucode)
        screen['score1'] = findata['earn2019'].values > 0
        screen['score2'] = findata['earn2018'].values > 0
        screen['score3'] = findata['earn2017'].values > 0
        screen['score4'] = (findata['earn2019'].values > findata['earn2018'].values) & (findata['earn2019'].values>0)
        screen['score5'] = (findata['sales2019'].values > findata['sales2018'].values * 1.1) & (findata['sales2018'].values>0)
        screen['score6'] = (findata['sales2018'].values > findata['sales2017'].values * 1.1) & (findata['sales2017'].values>0)
        screen['score7'] = findata['cash2019'].values > 0
        screen['score8'] = findata['cash2018'].values > 0
        screen['score9'] = findata['cash2017'].values > 0

        ##
        total_scores = screen[['score1', 'score2', 'score3',
                               'score4', 'score5', 'score6',
                               'score7', 'score8', 'score9']].astype(int)
        screen['total_scores'] = np.sum(total_scores, axis=1)

        quote_fields = ['HKStkMV', 'PERatio', 'PB']
        data = self.GR.get_secucode_quotedata_byfields(quote_fields, selectdate)
        for field in quote_fields:
            findata[field] = data[field].values

        screen["index"] = findata["PERatio"].values
        screen.loc[screen["index"]<=0,"index"] = np.nan

        screen = screen.loc[(screen['total_scores'].values == 9) & ~np.isnan(screen['index'].values), :].copy()
        screen = screen.sort_values(by=['index'], ascending=True, na_position='last')

        #
        stocks_pool = list(screen.iloc[:self.strategy_max_num].index)
        self.stocks_pool = stocks_pool
        return self.stocks_pool

    '''
    type: 策略选股; name: 高股息价值; item: 4; code: gaoguxijiazhitouzi; version: rc20200616; 
    '''
    def strategy_api_finx4_gaoguxijiazhitouzi_rc20200616(self):

        print('start strategy computation.')
        tradedays, secucode, selectdate = self.tradedays, self.secucode, self.selectdate

        findata = pd.DataFrame(data=None, index=secucode)

        myfields = ["EarningAfterTax", "NetOperateCashFlow", "OperatingIncome", "Dividend"]
        data = self.GR.get_secucode_financeindex_byfields(myfields, (2019, 3))
        findata['earn2019'] = data['EarningAfterTax'].values
        findata['sales2019'] = data["OperatingIncome"].values
        findata['cash2019'] = data["NetOperateCashFlow"].values
        findata['div2019'] = data["Dividend"].values

        myfields = ["EarningAfterTax", "NetOperateCashFlow", "OperatingIncome", "Dividend"]
        data = self.GR.get_secucode_financeindex_byfields(myfields, (2018, 3))
        findata['earn2018'] = data['EarningAfterTax'].values
        findata['sales2018'] = data["OperatingIncome"].values
        findata['cash2018'] = data["NetOperateCashFlow"].values
        findata['div2018'] = data["Dividend"].values

        myfields = ["EarningAfterTax", "NetOperateCashFlow", "OperatingIncome", "Dividend"]
        data = self.GR.get_secucode_financeindex_byfields(myfields, (2017, 3))
        findata['earn2017'] = data['EarningAfterTax'].values
        findata['sales2017'] = data["OperatingIncome"].values
        findata['cash2017'] = data["NetOperateCashFlow"].values
        findata['div2017'] = data["Dividend"].values

        findata = findata.fillna(0)

        screen = pd.DataFrame(data=None, index=secucode)
        screen['score1'] = findata['earn2019'].values > 0
        screen['score2'] = findata['earn2018'].values > 0
        screen['score3'] = findata['earn2017'].values > 0
        screen['score4'] = findata['earn2019'].values > findata['earn2017'].values * 1.21
        screen['score5'] = findata['div2019'].values > 0
        screen['score6'] = findata['div2018'].values > 0
        screen['score7'] = findata['div2017'].values > 0

        quote_fields = ['HKStkMV', 'PERatio', 'PB', 'DividendRatioFY', 'DividendRatioRW']
        data = self.GR.get_secucode_quotedata_byfields(quote_fields, selectdate)
        for field in quote_fields:
            findata[field] = data[field].values

        findata = findata.fillna(0)

        screen['score8'] = (findata['PERatio'].values < 17) & (findata['PERatio'].values > 0)
        screen['score9'] = (findata['PERatio'].values * findata['PB'].values < 25.5) & (findata['PERatio'].values > 0)
        screen["index_mv"] = findata["HKStkMV"].values
        screen['index_dividend'] = findata["DividendRatioFY"].values

        ##
        total_scores = screen[['score1', 'score2', 'score3',
                               'score4', 'score5', 'score6',
                               'score7', 'score8', 'score9']].astype(int)

        screen['total_scores'] = np.sum(total_scores, axis=1)
        screen = screen.sort_values(by=['index_mv'], ascending=False, na_position='last')
        screen = screen.iloc[:300].copy()
        screen = screen.loc[screen['total_scores'] >= 6].copy()
        screen = screen.sort_values(by=['index_dividend'], ascending=False, na_position='last')

        #
        stocks_pool = list(screen.iloc[:self.strategy_max_num].index)
        self.stocks_pool = stocks_pool
        print(self.stocks_pool)
        return self.stocks_pool

    '''
    type: 策略选股; name: 高股息价值; item: 4; code: gaoguxijiazhi; version: rc20200616; 
    '''
    def strategy_api_finx4_gaoguxijiazhi_rc20200701(self):

        jd = self

        print('start strategy computation.')
        tradedays, secucode, selectdate = jd.tradedays, jd.secucode, jd.selectdate

        print("from {} to {} ".format(tradedays[0], tradedays[-1]))

        pooldata = pd.DataFrame(data=None, index=secucode)
        quote_fields = ['HKStkMV', 'PERatio', 'DividendRatioFY', 'DividendRatioRW']

        data = jd.GR.get_secucode_quotedata_byfields(quote_fields, 20171229)
        for field in quote_fields:
            pooldata[field + "2017"] = data[field].values

        data = jd.GR.get_secucode_quotedata_byfields(quote_fields, 20181231)
        for field in quote_fields:
            pooldata[field + "2018"] = data[field].values

        data = jd.GR.get_secucode_quotedata_byfields(quote_fields, 20191231)
        for field in quote_fields:
            pooldata[field + "2019"] = data[field].values

        data = jd.GR.get_secucode_quotedata_byfields(quote_fields, selectdate)
        for field in quote_fields:
            pooldata[field + "2020"] = data[field].values

        for thisyear in ["2017", "2018", "2019", "2020"]:
            pooldata["payout" + thisyear] = pooldata["PERatio" + thisyear].values * pooldata[
                "DividendRatioRW" + thisyear] / 100

        quote_fields = ['DividendRatioRW', 'DividendRatioFY']
        data = jd.GR.get_secucode_quotedata_byfields(quote_fields, selectdate)
        for field in quote_fields:
            pooldata[field] = data[field].values

        thisfield = "EarningAfterTax"
        for thisyear in [2017, 2018, 2019]:
            data = jd.GR.get_secucode_financeindex_byfields([thisfield], (thisyear, 3))
            pooldata[thisfield + str(thisyear)] = data[thisfield].values

        pooldata = pooldata.fillna(0)

        screen = pd.DataFrame(data=None, index=secucode)
        screen['score1'] = pooldata['EarningAfterTax2019'].values > 0
        screen['score2'] = pooldata['EarningAfterTax2018'].values > 0
        screen['score3'] = pooldata['EarningAfterTax2017'].values > 0
        screen['score4'] = pooldata['payout2020'].values > 0.3
        screen['score5'] = pooldata['payout2019'].values > 0.3
        screen['score6'] = pooldata['payout2018'].values > 0.3
        screen['score7'] = pooldata['DividendRatioFY'].values > 5
        screen['index_dividend_ratio'] = pooldata['DividendRatioFY'].values

        screen['total_scores'] = \
            np.sum(
                screen[['score1', 'score2', 'score3',
                        'score4', 'score5', 'score6',
                        'score7']].astype(int),
                axis=1)

        screen = screen.loc[screen['total_scores'] >= 7].copy()
        screen = screen.sort_values(by=['index_dividend_ratio'], ascending=False, na_position='last')
        # screen.to_csv("screen_data.csv")
        #
        stocks_pool = list(screen.iloc[:self.strategy_max_num].index)
        self.stocks_pool = stocks_pool
        print(self.stocks_pool)
        return self.stocks_pool


