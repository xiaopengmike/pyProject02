from Core.dataload import GildataDb
import  pandas as pd
pd.set_option('display.max_columns',30) #给最大列设置为10列
pd.set_option('display.max_rows',3000)#设置最大可见100行

def get_quater_factordata(date):
    Basedata=GildataDb()
    df1=Basedata.read_financeindex_data(date)###
    df3=Basedata.read_HK_MainIndex_data('20201220')
    df4=pd.merge(df1,df3,on=['SecuCode','PeriodMark','FinancialYear'])
    # 部分因子的合成和计算
    df4['ROIC'] = (df4['EarningAfterTax'] + df4['FinancialExpense']) / (df4['FinancialExpense'] * 10 + df4['TotalShareholderEquity'])
    return df4

def  get_daily_factordata(date):
    Basedata = GildataDb()
    df5=Basedata.read_QT_HKDailyQuoteIndex_data(date)
    return df5

#初始化
holdingstockdata=pd.DataFrame(columns=['开仓日期','开仓标的','开仓价格','平仓日期','平仓价格'])
aus=0

#获取交易日期
traderdays=pd.read_csv('hsindex.csv')

#策略主体
for j in range(31,len(traderdays)-1):
    date = traderdays.iloc[j,0].replace('/','').lstrip(' ') #字符串'2010/01/30'
    date2 = traderdays.iloc[j+1,0].replace('/','').lstrip(' ') #字符串'2010/01/30'
    print(j,date)
    #获取该交易日的成分股标的
    constituent_intervel=['20091231','20100630','20101231','20110630','20111231','20120630','20121231','20130630','20131231','20140630','20141231','20150630','20151231','20160630','20161231','20170630','20171231','20180630','20181231','20190630','20191231','20200630','20201231']
    for m in range(1,len(constituent_intervel)):
        if constituent_intervel[m-1]<=date and date<=constituent_intervel[m]:
            alltarget=pd.read_csv(constituent_intervel[m-1]+'.csv',encoding='gb18030').iloc[:,[0,2]]

#高股息策略和巴菲特风格策略
#    targetdata = pd.DataFrame(columns=['日期','标的','市值','股息率TTM','PETTM','RoeT-1','RoeT-2','RoeT-3'])
#神奇公式
#    targetdata = pd.DataFrame(columns=['日期', '标的','行业编码', '市值', '股息率TTM', 'PETTM', 'ROICT-1', 'ROICT-2', 'ROICT-3'])
#行业龙头
#    targetdata = pd.DataFrame(columns=['日期', '标的', '行业编码', '市值', '股息率TTM', 'PETTM'])
#精选银行
#    targetdata = pd.DataFrame(columns=['日期', '标的', '行业编码', '市值', '股息率TTM', 'PETTM','PB'])
#德瑞曼型风格  三一选股法
#    targetdata = pd.DataFrame(columns=['日期', '标的', '行业编码', '市值', '股息率TTM', 'PETTM', 'PB','PCF'])
# 迈克尔普莱斯低估价值选股法  萝卜瑞克
#    targetdata = pd.DataFrame(columns=['日期', '标的', '行业编码', '市值', '股息率TTM', 'PETTM', 'PB', 'PCF','DebtAssetsRatio'])
#迈克尔喜伟
#    targetdata = pd.DataFrame(columns=['日期', '标的', '行业编码', '市值', '股息率TTM', 'PETTM', 'PB', 'PCF', 'DebtAssetsRatio', 'DebtEquityRatio', 'NPParentCompanyGR1Y', 'CurrentRatio'])

#PB-ROE价值投资（CZ
#    targetdata = pd.DataFrame(columns=['日期', '标的', '行业编码', '市值', '股息率TTM', 'PETTM', 'PB','RoeT-1','RoeT-2','RoeT-3','OperProfitGR1Y','OperatingRevenueGR1Y'])
#小盘价值投资
#    targetdata = pd.DataFrame(columns=['日期', '标的', '行业编码', '市值', '股息率TTM', 'PETTM', 'PB', 'EarningAfterTax', 'NetOperateCashFlow', GrossIncomeGR1Y''])
    targetdata = pd.DataFrame(columns=['日期', '标的', '行业编码', '市值', '股息率TTM', 'PETTM', 'PERatio', 'PB', 'PS', 'GrossIncomeGR1Y','EarningAfterTax', 'OperatingRevenueGR3Y', 'NPParentCompanyGR1Y', 'NPParentCompanyGR3Y','NetOperateCashFlow', 'OperatingRevenueGR1Y'])
#成长投资
#    targetdata = pd.DataFrame(columns=['日期', '标的', '行业编码', '市值', '股息率TTM', 'PETTM', 'PERatio','PB', 'PS','BasicEPS1Y', 'NetProfitRatio','ROA','RoeT-1','DebtAssetsRatio','NetOperateCashFlow','OperatingRevenueGR1Y'])
#现金为王投资
    # targetdata = pd.DataFrame(columns=['日期', '标的', '行业编码', '市值', '股息率TTM', 'PETTM', 'PERatio', 'PB', 'PS', 'OperatingIncome', 'EarningAfterTax', 'OperatingRevenueGR3Y', 'NPParentCompanyGR1Y','NPParentCompanyGR3Y', 'NetOperateCashFlow', 'OperatingRevenueGR1Y'])

#从数据库读取财务数据
    adffda=get_daily_factordata(date)
    fdgsgt=get_quater_factordata(date)
# 从数据库读取行情数据
    Basedata = GildataDb()
    quote = Basedata.read_QT_HKBefRehDQuote_data(date)
    quote1 = Basedata.read_QT_HKBefRehDQuote_data(date2)

    for i in range(len(alltarget)):
        daily_factordata = adffda[adffda['SecuCode']==alltarget.iloc[i,0][:5]]
#        daily_factordata = get_daily_factordata(alltarget[i][:5], date)
        quater_factordata = fdgsgt[fdgsgt['SecuCode']==alltarget.iloc[i,0][:5]]
#高股息策略和巴菲特风格策略
#        if daily_factordata.empty or len(quater_factordata)<3 or quater_factordata['ROEWeighted'].tolist()[-1]==None or quater_factordata['ROEWeighted'].tolist()[-2]==None or quater_factordata['ROEWeighted'].tolist()[-3]==None:
#神奇公式
#        if daily_factordata.empty or len(quater_factordata)<3 or quater_factordata['ROIC'].tolist()[-1]==None or quater_factordata['ROIC'].tolist()[-2]==None or quater_factordata['ROIC'].tolist()[-3]==None:
#行业龙头
#        if daily_factordata.empty:

#麦克瑞普莱斯
#        if daily_factordata.empty or quater_factordata.empty or quater_factordata['DebtAssetsRatio'].tolist()[-1]==None :
#PB-ROE价值投资（CZ）
#        if daily_factordata.empty or len(quater_factordata)<3 or quater_factordata['ROEWeighted'].tolist()[-1]==None or quater_factordata['ROEWeighted'].tolist()[-2]==None or quater_factordata['ROEWeighted'].tolist()[-3]==None or quater_factordata['OperatingRevenueGR1Y'].tolist()[-1]==None or quater_factordata['OperProfitGR1Y'].tolist()[-1]==None:
#小盘价值投资
#        if daily_factordata.empty or quater_factordata.empty or quater_factordata['EarningAfterTax'].tolist()[-1]==None or quater_factordata['NetOperateCashFlow'].tolist()[-1]==None or quater_factordata['GrossIncomeGR1Y'].tolist()[-1]==None:
# 成长投资
#        if daily_factordata.empty or quater_factordata.empty or quater_factordata['BasicEPS1Y'].tolist()[-1]==None or quater_factordata['OperatingRevenueGR1Y'].tolist()[-1]==None or quater_factordata['NetOperateCashFlow'].tolist()[-1] == None or quater_factordata['NetProfitRatio'].tolist()[-1]==None or quater_factordata['ROA'].tolist()[-1]==None or quater_factordata['DebtAssetsRatio'].tolist()[-1]==None or quater_factordata['ROEWeighted'].tolist()[-1]==None:
#现金为王策略
        # kk=daily_factordata.empty
        # oo=quater_factordata.empty
        # aa=quater_factordata['OperatingRevenueGR1Y'].tolist()[-1]==None
        # bb=quater_factordata['OperatingRevenueGR3Y'].tolist()[-1]==None
        # hh=quater_factordata['NPParentCompanyGR1Y'].tolist()[-1]==None
        # htru=quater_factordata['GrossIncomeGR1Y'].tolist()[-1]==None
        # cc=quater_factordata['NPParentCompanyGR3Y'].tolist()[-1]==None
        # yy=quater_factordata['NetOperateCashFlow'].tolist()[-1]==None
        # rr=quater_factordata['EarningAfterTax'].tolist()[-1]==None
        if daily_factordata.empty or quater_factordata.empty or quater_factordata['OperatingRevenueGR1Y'].tolist()[-1]==None or quater_factordata['OperatingRevenueGR3Y'].tolist()[-1]==None or quater_factordata['NPParentCompanyGR1Y'].tolist()[-1]==None or quater_factordata['NPParentCompanyGR3Y'].tolist()[-1]==None or quater_factordata['NetOperateCashFlow'].tolist()[-1]==None or quater_factordata['EarningAfterTax'].tolist()[-1]==None or quater_factordata['GrossIncomeGR1Y'].tolist()[-1]==None :
            continue
        else:
            targetdata.loc[i, '标的'] = alltarget.iloc[i,0][:5]
            targetdata.loc[i, '行业编码'] = alltarget.iloc[i,1]
            targetdata.loc[i, '日期'] = date
            targetdata.loc[i, '市值'] = daily_factordata['HKStkMV'].tolist()[-1]
            targetdata.loc[i, '股息率TTM'] =daily_factordata['DividendRatioRW'].tolist()[-1]
            targetdata.loc[i, 'PETTM'] = daily_factordata['PETTM'].tolist()[-1]
            targetdata.loc[i, 'PB'] = daily_factordata['PB'].tolist()[-1]
            targetdata.loc[i, 'PCF'] = daily_factordata['PCF'].tolist()[-1]
            targetdata.loc[i, 'PERatio'] = daily_factordata['PERatio'].tolist()[-1]
            targetdata.loc[i, 'PS'] = daily_factordata['PS'].tolist()[-1]
#高股息策略和巴菲特风格策略
#            targetdata.loc[i, 'RoeT-1'] = quater_factordata['ROEWeighted'].tolist()[-1]
#            targetdata.loc[i, 'RoeT-2'] = quater_factordata['ROEWeighted'].tolist()[-2]
#            targetdata.loc[i, 'RoeT-3'] = quater_factordata['ROEWeighted'].tolist()[-3]
# 神奇公式
#            targetdata.loc[i, 'ROICT-1'] = quater_factordata['ROIC'].tolist()[-1]
#            targetdata.loc[i, 'ROICT-2'] = quater_factordata['ROIC'].tolist()[-2]
#            targetdata.loc[i, 'ROICT-3'] = quater_factordata['ROIC'].tolist()[-3]

#迈克尔普莱斯低估价值选股法  萝卜瑞克
            # targetdata.loc[i, 'NPParentCompanyGR1Y'] = quater_factordata['NPParentCompanyGR1Y'].tolist()[-1]
            # targetdata.loc[i, 'DebtEquityRatio'] = quater_factordata['DebtEquityRatio'].tolist()[-1]
            # targetdata.loc[i, 'CurrentRatio'] = quater_factordata['CurrentRatio'].tolist()[-1]
#PB-ROE价值投资（CZ）
            # targetdata.loc[i, 'RoeT-1'] = quater_factordata['ROEWeighted'].tolist()[-1]
            # targetdata.loc[i, 'RoeT-2'] = quater_factordata['ROEWeighted'].tolist()[-2]
            # targetdata.loc[i, 'RoeT-3'] = quater_factordata['ROEWeighted'].tolist()[-3]
            # targetdata.loc[i, 'OperatingRevenueGR1Y'] = quater_factordata['OperatingRevenueGR1Y'].tolist()[-1]
            # targetdata.loc[i, 'OperProfitGR1Y'] = quater_factordata['OperProfitGR1Y'].tolist()[-1]
#小盘价值投资
            targetdata.loc[i, 'EarningAfterTax'] = quater_factordata['EarningAfterTax'].tolist()[-1]
            targetdata.loc[i, 'NetOperateCashFlow'] = quater_factordata['NetOperateCashFlow'].tolist()[-1]
            targetdata.loc[i, 'GrossIncomeGR1Y'] = quater_factordata['GrossIncomeGR1Y'].tolist()[-1]
            targetdata.loc[i, 'OperatingRevenueGR1Y'] = quater_factordata['OperatingRevenueGR1Y'].tolist()[-1]
            targetdata.loc[i, 'OperatingRevenueGR3Y'] = quater_factordata['OperatingRevenueGR3Y'].tolist()[-1]
            targetdata.loc[i, 'NPParentCompanyGR1Y'] = quater_factordata['NPParentCompanyGR1Y'].tolist()[-1]
            targetdata.loc[i, 'NPParentCompanyGR3Y'] = quater_factordata['NPParentCompanyGR3Y'].tolist()[-1]
#成长投资
            # targetdata.loc[i, 'BasicEPS1Y'] = quater_factordata['BasicEPS1Y'].tolist()[-1]
            # targetdata.loc[i, 'OperatingRevenueGR1Y'] = quater_factordata['OperatingRevenueGR1Y'].tolist()[-1]
            # targetdata.loc[i, 'NetOperateCashFlow'] = quater_factordata['NetOperateCashFlow'].tolist()[-1]
            # targetdata.loc[i, 'NetProfitRatio'] = quater_factordata['NetProfitRatio'].tolist()[-1]
            # targetdata.loc[i, 'ROA'] = quater_factordata['ROA'].tolist()[-1]
            # targetdata.loc[i, 'DebtAssetsRatio'] = quater_factordata['DebtAssetsRatio'].tolist()[-1]
            # targetdata.loc[i, 'RoeT-1'] = quater_factordata['ROEWeighted'].tolist()[-1]
#现金为王投资策略
            # targetdata.loc[i, 'OperatingRevenueGR1Y'] = quater_factordata['OperatingRevenueGR1Y'].tolist()[-1]
            # targetdata.loc[i, 'OperatingRevenueGR3Y'] = quater_factordata['OperatingRevenueGR3Y'].tolist()[-1]
            # targetdata.loc[i, 'NPParentCompanyGR1Y'] = quater_factordata['NPParentCompanyGR1Y'].tolist()[-1]
            # targetdata.loc[i, 'NPParentCompanyGR3Y'] = quater_factordata['NPParentCompanyGR3Y'].tolist()[-1]
            # targetdata.loc[i, 'NetOperateCashFlow'] = quater_factordata['NetOperateCashFlow'].tolist()[-1]
            # targetdata.loc[i, 'OperatingIncome'] = quater_factordata['OperatingIncome'].tolist()[-1]
            # targetdata.loc[i, 'EarningAfterTax'] = quater_factordata['EarningAfterTax'].tolist()[-1]
#高股息价值投资
            # targetdata.loc[i, 'OperatingRevenueGR1Y'] = quater_factordata['OperatingRevenueGR1Y'].tolist()[-1]
            # targetdata.loc[i, 'OperatingRevenueGR3Y'] = quater_factordata['OperatingRevenueGR3Y'].tolist()[-1]
            # targetdata.loc[i, 'NPParentCompanyGR1Y'] = quater_factordata['NPParentCompanyGR1Y'].tolist()[-1]
            # targetdata.loc[i, 'NPParentCompanyGR3Y'] = quater_factordata['NPParentCompanyGR3Y'].tolist()[-1]
            # targetdata.loc[i, 'NetOperateCashFlow'] = quater_factordata['NetOperateCashFlow'].tolist()[-1]
            # targetdata.loc[i, 'OperatingIncome'] = quater_factordata['OperatingIncome'].tolist()[-1]
            # targetdata.loc[i, 'EarningAfterTax'] = quater_factordata['EarningAfterTax'].tolist()[-1]

#选出标的并建仓
#现金为王策略
        # targetdata = targetdata[(targetdata['OperatingRevenueGR1Y'] > 10) & (targetdata['OperatingRevenueGR3Y'] > 10) & (targetdata['NPParentCompanyGR1Y'] > 10) & (targetdata['NPParentCompanyGR3Y'] > 10) & (targetdata['NetOperateCashFlow'] > 0) & (targetdata['OperatingIncome'] > 0) &(targetdata['EarningAfterTax'] > 0)]
        # targetdata = targetdata.sort_values(by='PB').head(10)
# 高股息价值投资
#         targetdata = targetdata[(targetdata['OperatingRevenueGR1Y'] > 10) & (targetdata['OperatingRevenueGR3Y'] > 10) & (targetdata['NPParentCompanyGR1Y'] > 10) & (targetdata['NPParentCompanyGR3Y'] > 10) & (targetdata['NetOperateCashFlow'] > 0) & (targetdata['OperatingIncome'] > 0) &(targetdata['EarningAfterTax'] > 0) & (targetdata['股息率TTM']>5)]
#         targetdata = targetdata.sort_values(by='股息率TTM').tail(10)

# 成长投资
#     target1 = pd.DataFrame()
#     targetdata['BasicEPS1Y'] = targetdata['BasicEPS1Y'].astype(float)
#     idx = targetdata['BasicEPS1Y'].groupby([targetdata['行业编码']]).quantile(0.5)
#     for i in range(len(idx)):
#         df1 = targetdata[targetdata['BasicEPS1Y'] > idx[i]]
#         df2 = df1[df1['行业编码'] == idx.index[i]]
#         target1 = pd.concat([target1, df2])
#
#     target2 = pd.DataFrame()
#     targetdata['OperatingRevenueGR1Y'] = targetdata['OperatingRevenueGR1Y'].astype(float)
#     idx = targetdata['OperatingRevenueGR1Y'].groupby([targetdata['行业编码']]).quantile(0.5)
#     for i in range(len(idx)):
#         df1 = targetdata[targetdata['OperatingRevenueGR1Y'] > idx[i]]
#         df2 = df1[df1['行业编码'] == idx.index[i]]
#         target2 = pd.concat([target2, df2])
#
#     target3 = pd.DataFrame()
#     targetdata['NetOperateCashFlow'] = targetdata['NetOperateCashFlow'].astype(float)
#     idx = targetdata['NetOperateCashFlow'].groupby([targetdata['行业编码']]).quantile(0.5)
#     for i in range(len(idx)):
#         df1 = targetdata[targetdata['NetOperateCashFlow'] > idx[i]]
#         df2 = df1[df1['行业编码'] == idx.index[i]]
#         target3 = pd.concat([target3, df2])
#
#     target4 = pd.DataFrame()
#     targetdata['ROA'] = targetdata['ROA'].astype(float)
#     idx = targetdata['ROA'].groupby([targetdata['行业编码']]).quantile(0.5)
#     for i in range(len(idx)):
#         df1 = targetdata[targetdata['ROA'] > idx[i]]
#         df2 = df1[df1['行业编码'] == idx.index[i]]
#         target4 = pd.concat([target4, df2])
#
#     target5 = pd.DataFrame()
#     targetdata['DebtAssetsRatio'] = targetdata['DebtAssetsRatio'].astype(float)
#     idx = targetdata['DebtAssetsRatio'].groupby([targetdata['行业编码']]).quantile(0.5)
#     for i in range(len(idx)):
#         df1 = targetdata[targetdata['DebtAssetsRatio'] < idx[i]]
#         df2 = df1[df1['行业编码'] == idx.index[i]]
#         target5 = pd.concat([target5, df2])
#
#     target6= pd.DataFrame()
#     targetdata['NetProfitRatio'] = targetdata['NetProfitRatio'].astype(float)
#     idx = targetdata['NetProfitRatio'].groupby([targetdata['行业编码']]).quantile(0.5)
#     for i in range(len(idx)):
#         df1 = targetdata[targetdata['NetProfitRatio'] > idx[i]]
#         df2 = df1[df1['行业编码'] == idx.index[i]]
#         target6 = pd.concat([target6, df2])
#
#     target7= pd.DataFrame()
#     targetdata['RoeT-1'] = targetdata['RoeT-1'].astype(float)
#     idx = targetdata['RoeT-1'].groupby([targetdata['行业编码']]).quantile(0.5)
#     for i in range(len(idx)):
#         df1 = targetdata[targetdata['RoeT-1'] > idx[i]]
#         df2 = df1[df1['行业编码'] == idx.index[i]]
#         target7 = pd.concat([target7, df2])
#
#
#     targetdata_adjust5 = pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(target1, target2, on=['标的']), target3, on=['标的']), target4, on=['标的']), target5,on=['标的']),target6,on=['标的']),target7,on=['标的'])
#     targetdata = targetdata_adjust5[(targetdata_adjust5['BasicEPS1Y'] > 0) & (targetdata_adjust5['OperatingRevenueGR1Y'] > 0) & (targetdata_adjust5['NetOperateCashFlow'] > 0)].sort_values(by='PB').head(10)
#

#PB-ROE价值投资（CZ）
    # targetdata = targetdata[(targetdata['OperatingRevenueGR1Y'] > 10) & (targetdata['RoeT-1'] > 10) & (targetdata['RoeT-2'] > 10) & (targetdata['RoeT-3'] > 10) & (targetdata['OperProfitGR1Y'] > 10) & (targetdata['RoeT-1'] > targetdata['RoeT-2'])]
    # targetdata['ROE_PB'] = targetdata['RoeT-1']/targetdata['PB']
    # targetdata = targetdata.sort_values(by='ROE_PB').tail(10)

#小盘价值投资
    targetdata_adjust1 = targetdata.sort_values(by='PETTM').head(round(len(targetdata) * 0.5))
    targetdata_adjust2 = targetdata.sort_values(by='PB').head(round(len(targetdata) * 0.5))
    targetdata_adjust3 = targetdata.sort_values(by='市值').head(round(len(targetdata) * 0.5))

    targetdata_adjust5 = pd.merge(pd.merge(targetdata_adjust1, targetdata_adjust2, on=['标的']), targetdata_adjust3, on=['标的']) #.iloc[:,0:8]
    targetdata = targetdata_adjust5[(targetdata_adjust5['OperatingRevenueGR1Y'] > 10) & (targetdata_adjust5['OperatingRevenueGR3Y'] > 10) & (targetdata_adjust5['NPParentCompanyGR1Y'] > 10) & (targetdata_adjust5['NPParentCompanyGR3Y'] > 10) & (targetdata_adjust5['NetOperateCashFlow'] > 0) & (targetdata_adjust5['GrossIncomeGR1Y'] > 0) &(targetdata_adjust5['EarningAfterTax'] > 0)]
    # targetdata = targetdata_adjust5[(targetdata_adjust5['EarningAfterTax'] > 0) & (targetdata_adjust5['NetOperateCashFlow'] > 0) & (targetdata_adjust5['GrossIncomeGR1Y'] > 0)].head(10)



# 高股息策略
#    targetdata=targetdata[targetdata['股息率TTM']>5].sort_values(by='股息率TTM').tail(10)
#巴菲特风格策略
#    targetdata = targetdata[(targetdata['PETTM'] > 0) & (targetdata['RoeT-1'] > 15) & (targetdata['RoeT-2'] > 15) & (targetdata['RoeT-3'] > 15)].sort_values(by='PETTM').head(10)
#神奇公式
    # targetdata['ROIC_stanlization'] = (targetdata['ROICT-1'] - decimal.Decimal(targetdata['ROICT-1'][targetdata['ROICT-1'].notnull()].mean())) / decimal.Decimal(targetdata['ROICT-1'][targetdata['ROICT-1'].notnull()].astype('float').std())
    # targetdata['PETTM_stanlization'] = (targetdata['PETTM'] - decimal.Decimal(targetdata['PETTM'][targetdata['PETTM'].notnull()].mean())) / decimal.Decimal(targetdata['PETTM'][targetdata['PETTM'].notnull()].astype('float').std())
    # aus0 = targetdata.loc[:, ['标的', 'ROIC_stanlization']].sort_values(by='ROIC_stanlization', ascending=False)
    # aus0['rank'] = range(len(aus0))
    # targetdata = pd.merge(targetdata, aus0, on=['标的', 'ROIC_stanlization'])
    # aus1 = targetdata.loc[:, ['标的', 'PETTM_stanlization']].sort_values(by='PETTM_stanlization', ascending=True)
    # aus1['rank1'] = range(len(aus1))
    # targetdata=pd.merge(targetdata, aus1, on=['标的', 'PETTM_stanlization'])
    # targetdata['RANK']=targetdata['rank']+targetdata['rank1']
    # targetdata = targetdata[(targetdata['PETTM'] > 0) & (targetdata['ROICT-1'] > 0) & (targetdata['ROICT-2'] > 0) & (targetdata['ROICT-3'] > 0) & (targetdata['市值'] > 10000000000)].sort_values(by='RANK',ascending=True).head(10)
#行业龙头
    # targetdata['市值']=targetdata['市值'].astype(float)
    # idx = targetdata['市值'].groupby(targetdata['行业编码']).idxmax()
    # targetdata = targetdata.loc[idx, :]
#精选银行
#    targetdata = targetdata[(targetdata['行业编码'] == "4,010.0000")].sort_values(by='PB').head(3)

# 德瑞曼型风格
#     targetdata_adjust1 = targetdata.sort_values(by='PETTM').head(round(len(targetdata) * 0.5))
#     targetdata_adjust2 = targetdata.sort_values(by='PB').head(round(len(targetdata) * 0.5))
#     targetdata_adjust3 = targetdata.sort_values(by='PCF').head(round(len(targetdata) * 0.5))
#     targetdata_adjust4 = targetdata.sort_values(by='市值', ascending=False).head(round(len(targetdata) * 0.5))
#
#     targetdata_adjust5 = pd.merge(pd.merge(pd.merge(targetdata_adjust1, targetdata_adjust2, on=['标的']), targetdata_adjust3, on=['标的']),targetdata_adjust4, on=['标的']).iloc[:,0:8]
#     targetdata = targetdata_adjust5[(targetdata_adjust5['PETTM_x'] > 0) & (targetdata_adjust5['PB_x'] > 0) & (targetdata_adjust5['股息率TTM_x'] > 0)].head(10)


    # 三一选股法
    # targetdata_adjust1 = targetdata.sort_values(by='PETTM').head(round(len(targetdata) * 0.3))
    # targetdata_adjust2 = targetdata.sort_values(by='PB').head(round(len(targetdata) * 0.3))
    # targetdata_adjust3 = targetdata.sort_values(by='股息率TTM', ascending=False).head(round(len(targetdata) * 0.3))
    # targetdata_adjust4 = pd.merge(pd.merge(targetdata_adjust1, targetdata_adjust2, on=['标的']), targetdata_adjust3,on=['标的']).iloc[:,0:8]
    # targetdata = targetdata_adjust4[(targetdata_adjust4['PETTM_x'] > 0) & (targetdata_adjust4['PB_x'] > 0)].head(10)

    # 迈克尔•普莱斯低估价值选股法
    # target = pd.DataFrame()
    # targetdata['DebtAssetsRatio']=targetdata['DebtAssetsRatio'].astype(float)
    # idx = targetdata['DebtAssetsRatio'].groupby([targetdata['行业编码']]).quantile(0.5)  # 局部数据就用局部columns
    # for i in range(len(idx)):
    #     ad=i
    #     df1 = targetdata[targetdata['DebtAssetsRatio'] < idx[i]]
    #     df2 = df1[df1['行业编码'] == idx.index[i]]
    #     target = pd.concat([target, df2])
    #
    # targetdata = target[(target['PB'] > 0) & (target['PB'] < 2)]
    # targetdata['市值'] = targetdata['市值'].astype(float)
    # id = targetdata['市值'].groupby([targetdata['行业编码']]).idxmax()  # 局部数据就用局部columns
    # targetdata = targetdata.loc[id, :]

    #萝卜瑞克
    # target1=pd.DataFrame()
    # targetdata['股息率TTM'] = targetdata['股息率TTM'].astype(float)
    # idx =targetdata['股息率TTM'].groupby([targetdata['行业编码']]).quantile(0.5)#局部数据就用局部columns
    #
    # for i in range(len(idx)):
    #     df1=targetdata[targetdata['股息率TTM']>idx[i]]
    #     df2=df1[df1['行业编码']==idx.index[i]]
    #     target1=pd.concat([target1,df2])
    #
    # target2=pd.DataFrame()
    # targetdata['PETTM'] = targetdata['PETTM'].astype(float)
    # idx =targetdata['PETTM'].groupby([targetdata['行业编码']]).quantile(0.5)#局部数据就用局部columns
    #
    # for i in range(len(idx)):
    #     df1=targetdata[targetdata['PETTM']<idx[i]]
    #     df2=df1[df1['行业编码']==idx.index[i]]
    #     target2=pd.concat([target2,df2])
    #
    # target3=pd.DataFrame()
    # targetdata['PCF'] = targetdata['PCF'].astype(float)
    # idx =targetdata['PCF'].groupby([targetdata['行业编码']]).mean()#局部数据就用局部columns
    #
    # for i in range(len(idx)):
    #     df1=targetdata[targetdata['PCF']<idx[i]*0.8]
    #     df2=df1[df1['行业编码']==idx.index[i]]
    #     target3=pd.concat([target3,df2])
    #
    # targetdata_adjust5 = pd.merge(pd.merge(target1, target2, on=['标的']),target3,on=['标的'])
    # targetdata = targetdata_adjust5[(targetdata_adjust5['DebtAssetsRatio'] < 33) & (targetdata_adjust5['PB'] > 0) & (targetdata_adjust5['PB'] < 3)]
    # 迈克尔喜伟
    # target1 = pd.DataFrame()
    # targetdata['股息率TTM'] = targetdata['股息率TTM'].astype(float)
    # idx = targetdata['股息率TTM'].groupby([targetdata['行业编码']]).quantile(0.5)
    # for i in range(len(idx)):
    #     df1 = targetdata[targetdata['股息率TTM'] > idx[i]]
    #     df2 = df1[df1['行业编码'] == idx.index[i]]
    #     target1 = pd.concat([target1, df2])
    #
    # target2 = pd.DataFrame()
    # targetdata['PETTM'] = targetdata['PETTM'].astype(float)
    # idx = targetdata['PETTM'].groupby([targetdata['行业编码']]).quantile(0.5)
    # for i in range(len(idx)):
    #     df1 = targetdata[targetdata['PETTM'] < idx[i]]
    #     df2 = df1[df1['行业编码'] == idx.index[i]]
    #     target2 = pd.concat([target2, df2])
    #
    # target3 = pd.DataFrame()
    # targetdata['NPParentCompanyGR1Y'] = targetdata['NPParentCompanyGR1Y'].astype(float)
    # idx = targetdata['NPParentCompanyGR1Y'].groupby([targetdata['行业编码']]).quantile(0.5)
    # for i in range(len(idx)):
    #     df1 = targetdata[targetdata['NPParentCompanyGR1Y'] > idx[i]]
    #     df2 = df1[df1['行业编码'] == idx.index[i]]
    #     target3 = pd.concat([target3, df2])
    #
    # target4 = pd.DataFrame()
    # targetdata['DebtEquityRatio'] = targetdata['DebtEquityRatio'].astype(float)
    # idx = targetdata['DebtEquityRatio'].groupby([targetdata['行业编码']]).quantile(0.5)
    # for i in range(len(idx)):
    #     df1 = targetdata[targetdata['DebtEquityRatio'] < idx[i]]
    #     df2 = df1[df1['行业编码'] == idx.index[i]]
    #     target4 = pd.concat([target4, df2])
    #
    # target5 = pd.DataFrame()
    # targetdata['CurrentRatio'] = targetdata['CurrentRatio'].astype(float)
    # idx = targetdata['CurrentRatio'].groupby([targetdata['行业编码']]).quantile(0.5)
    # for i in range(len(idx)):
    #     df1 = targetdata[targetdata['CurrentRatio'] > idx[i]]
    #     df2 = df1[df1['行业编码'] == idx.index[i]]
    #     target5 = pd.concat([target5, df2])
    #
    # targetdata_adjust5 = pd.merge(pd.merge(pd.merge(pd.merge(target1, target2, on=['标的']), target3, on=['标的']), target4, on=['标的']), target5,on=['标的'])
    # targetdata = targetdata_adjust5[(targetdata_adjust5['PETTM'] > 0)]

    for k in range(len(targetdata)):
        stock=targetdata['标的'].tolist()[k]
        adsf=quote[quote['SecuCode']==stock]
        adsfadf = quote1[quote1['SecuCode'] == stock]
        holdingstockdata.loc[aus, '开仓日期'] = date
        holdingstockdata.loc[aus, '开仓标的'] = stock
        holdingstockdata.loc[aus, '开仓价格'] = adsf['OpenPrice'].tolist()[-1]
        holdingstockdata.loc[aus, '平仓日期'] = date2
        holdingstockdata.loc[aus, '平仓价格'] = adsfadf['OpenPrice'].tolist()[-1]
        aus+=1
    if j==30:
        holdingstockdata.to_csv('xiapan'+str(j)+'.csv')
holdingstockdata.to_csv('xiapan.csv')

#净值计算
import  pandas as pd
gaoguxi=pd.read_csv('xiapan30策略.csv',encoding='gb18030')
initial_cash = gaoguxi['开仓标的'].groupby([gaoguxi['开仓时间']]).count().quantile(0.5)*1000000
#去掉开平仓价格为零的交易
gaoguxi=gaoguxi[(gaoguxi['开仓价格']>0) & (gaoguxi['平仓价格']>0)]
#计算每笔交易的盈亏
gaoguxi['盈亏']=(gaoguxi['平仓价格']-gaoguxi['开仓价格'])*1000000/gaoguxi['开仓价格']
#计算每月累计盈亏
dailyearn=gaoguxi['盈亏'].groupby([gaoguxi['开仓时间']]).sum().cumsum()
#计算账户净值
mon_netwet=((dailyearn+initial_cash)/initial_cash).reset_index()
mon_netwet.columns=['time','newet_stock']
#计算指数每月净值
index_quoat=pd.read_csv('hsindex.csv')
index_quoat['netwet']=index_quoat.iloc[:,1]/float(index_quoat.columns[1])
index_netwet=index_quoat.iloc[:-1,[0,3]]
index_netwet.columns=['time','newet_index']

net_constract=pd.merge(mon_netwet,index_netwet,left_index=True, right_index=True, how='left').iloc[:,[0,1,3]]
net_constract.to_csv('xiapan30策略净值.csv')
