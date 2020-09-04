
# import MySQLdb

import os
import sys

import numpy as np
import pandas as pd

import datetime
import time

from Utils.PyTime import PyDating
from Utils.PyFile import PyFile

import uuid

from Utils.MySQLConn import MySQLConn


class GildataDb():
    '''
    Connection to GilDataDb
    '''
    def __init__(self):
        self.db = MySQLConn()
        self.limit_lstdate = None

    def set_limit_lstdate(self, lstdate):
        self.limit_lstdate = lstdate

    def close(self):
        self.db.close()

    def read_tradedays(self, days=250):
        command = '''
        select TradingDay from QT_HKDailyQuote group by TradingDay order by TradingDay
        '''
        fdata, nlines = self.db.read(command)
        fdata = [PyDating.dt_to_ymd_int(item[0]) for item in fdata]
        fdata = fdata[-days:]

        if self.limit_lstdate is not None:
            fdata = [x for x in fdata if x<=self.limit_lstdate]

        return fdata

    def read_stocklist(self):
        '''读取stocklist'''
        command = '''
                SELECT InnerCode, CompanyCode, SecuCode, ChiNameAbbr, SecuMarket, SecuCategory, ListedDate, ListedState, InduChS, HSIndexMark  
                FROM HK_SecuCodeTable 
                where (SecuCategory = '港股' or SecuCategory = 'H股') 
        '''
        columns = "InnerCode, CompanyCode, SecuCode, " \
                  "ChiNameAbbr, SecuMarket, SecuCategory, ListedDate, ListedState, InduChS, HSIndexMark"
        columns = columns.replace(" ","")
        columns_list = columns.split(",")

        fdata, nlines = self.db.read(command)
        dfdata = pd.DataFrame(data=fdata,columns=columns_list)
        return dfdata

    def read_trading_data(self, mydate):

        mydate_str = PyDating.dt_to_string(PyDating.ymd_int_to_dt(mydate), format='%Y-%m-%d')
        command = '''
            select codetable.SecuCode, t.ClosePrice, t.OpenPrice, t.HighPrice, t.LowPrice, t.TurnoverVolume, t.TurnoverValue
            from QT_HKDailyQuote t left join HK_SecuCodeTable codetable on t.InnerCode = codetable.InnerCode 
            where (t.TradingDay = "{}") and (codetable.SecuCode is not null) and (t.ClosePrice>0)
        '''.format(mydate_str)

        dfdata, nlines = self.db.read(command)
        columns = "SecuCode,ClosePrice,OpenPrice,HighPrice,LowPrice,VolumeValue,AmountValue"
        columns_list = columns.split(",")
        dfdata = pd.DataFrame(data=dfdata, columns=columns_list)

        return dfdata

    def read_QT_HKBefRehDQuote_data(self,target,mydate):
        mydate_str = PyDating.dt_to_string(PyDating.ymd_int_to_dt(mydate), format='%Y-%m-%d')
        command = '''
            select codetable.SecuCode,t.TradingDay,t.OpenPrice
            from QT_HKBefRehDQuote t left join HK_SecuCodeTable codetable on t.InnerCode = codetable.InnerCode
            where  (codetable.SecuCategory = "港股" or codetable.SecuCategory = "H股") and codetable.SecuCode="{company_code}" and t.TradingDay ="{TradingDay}"
        '''.format(company_code=target,TradingDay=mydate_str)

        dfdata, nlines = self.db.read(command)
        columns = "SecuCode,TradingDay,OpenPrice"
        columns_list = columns.split(",")
        dfdata = pd.DataFrame(data=dfdata, columns=columns_list)
        return dfdata


    def read_QT_HKDailyQuoteIndex_data(self,target,mydate):
        '''
        数据类型解析::
        HKStkShares 	港股股数(股)
        HKStkMV 	港股市值(元)
        TurnoverRate 	换手率
        PERatio 	市盈率
        PS 	市销率
        PB 	市净率
        PCF 	市现率
        DividendRatioFY 	股息率(报告期)(%)
        DividendRatioRW 	股息率(近12个月)(%)
        '''

        mydate_str = PyDating.dt_to_string(PyDating.ymd_int_to_dt(mydate), format='%Y-%m-%d')
        command = '''
            select codetable.SecuCode,t.TradingDay,t.InsertTime,t.UpdateTime, t.HKStkShares, t.HKStkMV, t.TurnoverRate, t.PERatio,t.PETTM, t.PS, t.PB, t.PCF, t.DividendRatioFY, t.DividendRatioRW
            from QT_HKDailyQuoteIndex t left join HK_SecuCodeTable codetable on t.InnerCode = codetable.InnerCode
            where  (codetable.SecuCategory = "港股" or codetable.SecuCategory = "H股") and codetable.SecuCode="{company_code}" and t.TradingDay ="{TradingDay}"
        '''.format(company_code=target,TradingDay=mydate_str)

        dfdata, nlines = self.db.read(command)
        columns = "SecuCode,TradingDay,InsertTime,UpdateTime,HKStkShares,HKStkMV,TurnoverRate,PERatio,PETTM,PS,PB,PCF,DividendRatioFY,DividendRatioRW"
        columns_list = columns.split(",")
        dfdata = pd.DataFrame(data=dfdata, columns=columns_list)
        return dfdata


    def read_HK_MainIndex_data(self,target,mydate):

        mydate_str = PyDating.dt_to_string(PyDating.ymd_int_to_dt(mydate), format='%Y-%m-%d')
        command = '''
            select codetable.SecuCode,t.BeginDate,t.EndDate,t.PeriodMark,t.FinancialYear,t.ROEWeighted,t.DividendRatio,t.CurrentRatio,t.BasicEPS
            from HK_MainIndex t left join HK_SecuCodeTable codetable on t.CompanyCode = codetable.CompanyCode
            where (codetable.SecuCategory = "港股" or codetable.SecuCategory = "H股")  and t.PeriodMark = '12' and codetable.SecuCode="{company_code}" and t.EndDate <="{TradingDay}"
            '''.format(company_code=target, TradingDay=mydate_str)

        dfdata, nlines = self.db.read(command)
        columns = "SecuCode,BeginDate,EndDate,PeriodMark,FinancialYear,ROEWeighted,DividendRatio,CurrentRatio,BasicEPS"
        columns_list = columns.split(",")
        dfdata = pd.DataFrame(data=dfdata, columns=columns_list)
        return dfdata

    def HK_FinStatsDirectable(self,mydate):

        mydate_str = PyDating.dt_to_string(PyDating.ymd_int_to_dt(mydate), format='%Y-%m-%d')
        command = '''
            select codetable.SecuCode,t.EndDate,t.DateTypeCode,t.InfoPublDateBS
            from HK_FinStatsDirectable t left join HK_SecuCodeTable codetable on t.CompanyCode = codetable.CompanyCode
            where (codetable.SecuCategory = "港股" or codetable.SecuCategory = "H股")  and codetable.SecuCode="{company_code}" and t.EndDate <="{TradingDay}"
            '''.format(company_code='03998', TradingDay=mydate_str)
        dfdata, nlines = self.db.read(command)
        columns = "SecuCode,EndDate,DateTypeCode,InfoPublDateBS"
        columns_list = columns.split(",")
        dfdata = pd.DataFrame(data=dfdata, columns=columns_list)
        return dfdata

    def read_financeindex_data(self, target, mydate):
        finance_q = 3
        mydate_str = PyDating.dt_to_string(PyDating.ymd_int_to_dt(mydate), format='%Y-%m-%d')
        this_qter = "第一季报,中期报告,第三季报,年度报告".split(",")[finance_q%4]
        command = '''
        select codetable.SecuCode, t.InfoPublDate,t.PerformancePublDate,t.PeriodicReportPublDate,t.PeriodMark,t.FinancialYear,t.TotalAssets,t.EPSBasic,t.EarningAfterTax,t.FinancialExpense,t.TotalShareholderEquity
        from HK_FinancialIndex t left join HK_SecuCodeTable codetable on t.CompanyCode = codetable.CompanyCode 
        where  (codetable.SecuCategory = "港股" or codetable.SecuCategory = "H股")
        and (InfoSource="{}") and  codetable.SecuCode="{company_code}" and t.InfoPublDate <="{TradingDay}"
        '''.format(this_qter,company_code=target,TradingDay=mydate_str)
        dfdata, nlines = self.db.read(command)
        columns = "SecuCode,InfoPublDate,PerformancePublDate,PeriodicReportPublDate,PeriodMark,FinancialYear,TotalAssets,EPSBasic,EarningAfterTax,FinancialExpense,TotalShareholderEquity"
        columns_list = columns.split(",")
        dfdata = pd.DataFrame(data=dfdata, columns=columns_list)
        return dfdata

    def read_QT_HKMulCycleQuote_data(self,mydate):

        mydate_str = PyDating.dt_to_string(PyDating.ymd_int_to_dt(mydate), format='%Y-%m-%d')
        command = '''
            select codetable.SecuCode,t.Cycle,t.BeginDate,t.OpenPrice
            from QT_HKMulCycleQuote t left join HK_SecuCodeTable codetable on t.InnerCode= codetable.InnerCode
            where (codetable.SecuCategory = "港股" or codetable.SecuCategory = "H股")  and t.Cycle= '4' and codetable.SecuCode="{company_code}" and t.BeginDate <="{TradingDay}"
            '''.format(company_code='03998', TradingDay=mydate_str)

        dfdata, nlines = self.db.read(command)
        columns = "SecuCode,Cycle,BeginDate,OpenPrice"
        columns_list = columns.split(",")
        dfdata = pd.DataFrame(data=dfdata, columns=columns_list)
        return dfdata




class GildataRem():

    def __init__(self, db):
        self.db = db
        self.tradedays = None
        self.stocklist = None

    def set_limit_lstdate(self, lstdate):
        # self.lstdate = lstdate
        self.db.set_limit_lstdate(lstdate)

    def get_today(self):
        today1 = PyDating.get_today()
        return today1

    def get_lastupdatedate(self):
        days = self.get_tradedays()
        lastdate = np.array(days).max()
        return lastdate

    def get_stocklist(self):

        if self.stocklist:
            data = self.stocklist
        else:
            data = self.db.read_stocklist()

        stockinfo = data.loc[data['ListedState'] == '正常交易'].copy()
        SecuCode = list(data["SecuCode"].values)

        # SecuInfo.to_csv("secu_info.csv", encoding='utf-8-sig')

        return SecuCode, stockinfo

    def get_tradedays(self):

        if self.tradedays:
            return self.tradedays
        else:
            return self.db.read_tradedays()

    def load_data(self):
        '''
        load data...
        :return:
        '''
        self._load_trading_data()
        self._load_quoting_data()

    def _load_trading_data(self):
        '''
        load_trading_data
        :return:
        '''

        print("start producing trading-data.")
        tradedays = self.get_tradedays()
        # PyFile.GenPath("../GILDATA/TRADEDATA")
        PyFile.GenPath("/GILDATA/TRADEDATA")
        maxdate_ = 0

        for mydate_ in tradedays:
            filepath = "GILDATA/TRADEDATA/{}.csv".format(mydate_)
            if os.path.exists(filepath):
                maxdate_ = max(maxdate_, mydate_)
                continue
            mydata_ = self.db.read_trading_data(mydate_)
            if len(mydata_)==0:
                print("tradedata {} has not come.".format(mydate_))
                continue
            mydata_.to_csv(filepath)
            maxdate_ = max(maxdate_, mydate_)
            print("load tradedata of {}".format(mydate_))
        return maxdate_


    def _load_quoting_data(self):
        '''
        PB,PE估值数据等
        :return:
        '''
        PyFile.GenPath("/GILDATA/QUOTEDATA")

        print("start producing quoting-data.")

        maxdate_ = 0

        tradedays = self.db.read_tradedays()
        for mydate_ in tradedays:
            filepath = "GILDATA/QUOTEDATA/{}.csv".format(mydate_)
            if os.path.exists(filepath):
                maxdate_ = max(maxdate_, mydate_)
                continue
            mydata_ = self.db.read_quoting_data(mydate_)
            if len(mydata_)==0:
                print("quotedata {} has not come.".format(mydate_))
                continue
            mydata_.to_csv(filepath)
            maxdate_ = max(maxdate_, mydate_)
            print('load quotedata of {}'.format(mydate_))
        return maxdate_

    def get_trading_data(self, mydate):

        filepath = "GILDATA/TRADEDATA/{}.csv".format(mydate)
        if not os.path.exists(filepath):
            return None
        data = pd.read_csv(filepath, index_col=0)
        return data

    def get_quoting_data(self, mydate):

        filepath = "GILDATA/QUOTEDATA/{}.csv".format(mydate)
        if not os.path.exists(filepath):
            return None
        data = pd.read_csv(filepath, index_col=0)
        return data

    def set_secucode(self, secucode):
        self.secucode = [int(item) for item in secucode]

    def get_secucode_tradedata(self, field: str, date):
        '''
        读取tradedata
        :param field: str, [ClosePrice, HighPrice, LowPrice, VolumeValue];
        :param date: int or list[int]
        :return: pd.DataFrame
        '''
        if not isinstance(date, list):
            date = [date]
        manydates = date
        rawdata = pd.DataFrame(data=None, index=self.secucode)

        for date in manydates:
            appdata = self.get_trading_data(date)[['SecuCode',field]]
            appdata = pd.DataFrame(data=appdata[field].values, columns=[date], index=appdata['SecuCode'].values.astype(int))
            rawdata = rawdata.join(appdata, how='left')
        rawdata = rawdata.fillna(np.nan).astype(float)
        return rawdata

    def get_secucode_quotedata(self, field: str, date):
        '''
        读取quotedata
        :param field: str
        :param date: int or list[int]
        :return: pd.DataFrame
        '''
        if not isinstance(date, list):
            date = [date]
        manydates = date
        rawdata = pd.DataFrame(data=None, index=self.secucode)

        for date in manydates:
            appdata = self.get_quoting_data(date)[['SecuCode',field]]
            appdata = pd.DataFrame(data=appdata[field].values, columns=[date], index=appdata['SecuCode'].values.astype(int))
            rawdata = rawdata.join(appdata, how='left')
        rawdata = rawdata.fillna(np.nan).astype(float)
        return rawdata

    # def get_secucode_quotedata

    def get_secucode_quotedata_byfields(self, field, date: int):

        if not isinstance(field, list):
            field = [field]
        manyfields = field
        rawdata = pd.DataFrame(data=None, index=self.secucode)

        for field in manyfields:
            appdata = self.get_quoting_data(date)[['SecuCode',field]]
            # appdata = self.get_quoting_data(date)[[field]]
            appdata = pd.DataFrame(data=appdata[field].values, columns=[field], index=appdata['SecuCode'].values.astype(int))
            rawdata = rawdata.join(appdata, how='left')
        rawdata = rawdata.fillna(np.nan).astype(float)
        return rawdata

    def get_secucode_financeindex(self, field: str, date):
        '''
        读取financeindex
        :param field: str
        :param date: (2019,3) or list[(int, int)]
        :return:
        '''
        if not isinstance(date, list):
            date = [date]
        manydates = date
        rawdata = pd.DataFrame(data=None, index=self.secucode)

        for iy, iq in manydates:
            # appdata = self.get_quoting_data(date)[['SecuCode',field]]
            appdata = self.get_finance_data(field=field, finance_y=iy, finance_q=iq)
            dataname = "{}-{}".format(iy,iq)
            appdata = pd.DataFrame(data=appdata[field].values, columns=[dataname], index=appdata['SecuCode'].values.astype(int))
            rawdata = rawdata.join(appdata, how='left')
        rawdata = rawdata.fillna(np.nan).astype(float)
        return rawdata


    def get_secucode_financeindex_byfields(self, field: str, date):
        '''
        读取finance-index;
        '''
        if not isinstance(field, list):
            field = [field]
        manyfields = field
        rawdata = pd.DataFrame(data=None, index=self.secucode)

        iy, iq = date
        for field in manyfields:
            # appdata = self.get_finance_data(field=field, finance_y=iy, finance_q=iq)
            appdata = self.db.read_financeindex_data(field, iy, iq)
            # appdata.to_csv("{}-data.csv".format(field))
            appdata = pd.DataFrame(data=appdata[field].values, columns=[field], index=appdata['SecuCode'].values.astype(int))
            rawdata = rawdata.join(appdata, how='left')

        rawdata = rawdata.fillna(np.nan).astype(float)
        return rawdata

    def get_secucode_financemain_byfields(self, field: str, date):
        '''
        读取finance-main;
        '''
        if not isinstance(field, list):
            field = [field]
        manyfields = field
        rawdata = pd.DataFrame(data=None, index=self.secucode)

        iy, iq = date
        for field in manyfields:
            appdata = self.db.read_financemain_data(field, iy, iq)
            appdata = pd.DataFrame(data=appdata[field].values, columns=[field], index=appdata['SecuCode'].values.astype(int))
            rawdata = rawdata.join(appdata, how='left')
        rawdata = rawdata.fillna(np.nan).astype(float)
        return rawdata

