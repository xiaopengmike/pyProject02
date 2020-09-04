from Utils.MySQLConn import MySQLConn
import  pandas as pd
import pymysql
command = '''
    select codetable.SecuCode,t.BeginDate,t.EndDate,t.FinancialYear,t.DividendRatio
    from HK_MainIndex t left join HK_SecuCodeTable codetable on t.CompanyCode = codetable.InnerCode
    where (codetable.SecuCategory = "港股" or codetable.SecuCategory = "H股")  and codetable.SecuCode="{company_code}"
    '''.format(company_code='00008')

dfdata, nlines = MySQLConn.read(command)
columns = "SecuCode,BeginDate,EndDate,FinancialYear,DividendRatio"
columns_list = columns.split(",")
dfdata = pd.DataFrame(data=dfdata, columns=columns_list)
print(dfdata)

host = 'rm-wz9s90lao15s6j4v2ro.mysql.rds.aliyuncs.com'
port: 3306
user = 'jydb'
password = 'G2W9iPwpAqF4R#202'
db_name = 'jydb'
charset = "utf8"

connection = pymysql.connect(
host=host,
user=user,
password=password,
charset='utf8',
db=db_name
)


sql= '''
    select t.*
    from HK_ExgIndustry t left join HK_SecuCodeTable codetable on t.CompanyCode = codetable.CompanyCode
    where (codetable.SecuCategory = "港股" or codetable.SecuCategory = "H股")  and codetable.SecuCode="{company_code}"
    '''.format(company_code='00007')

sql= '''
    select A.HolderName, B.Code as "HolderNatureCode", C.MS as "HolderNature"
from MF_TopTenHolder A 
left join MF_TopTenHolder_SE B on A.ID = B.ID and TypeCode = 1 
left join CT_SystemConst C on C.DM = B.Code and C.LB = 1026
cursor = connection.cursor(cursor=pymysql.cursors.DictCursor)
    '''

sql= '''
    select A.CompanyCode,A.IndustryNum,A.ExcuteDate,A.CancelDate, B.IndustryName,B.Classification, C.MS as "HolderNature"
from HK_ExgIndustry A 
left join CT_SystemConst C on C.DM = A.Standard and C.LB = 1081
left join HK_IndustryCategory B on A.IndustryNum = B.IndustryNum 
where A.CompanyCode='1000007'
    '''
sql= '''
    select FiscalYear,EarningAfterTax from HK_IncomeStatementGEHK A left join CT_SystemConst C on C.DM = A.AccountingStandards and C.LB = 1357 where A.CompanyCode='1000007' and A.PeriodMark='12' and A.AccountingStandards='110'
    '''
cursor = connection.cursor(cursor=pymysql.cursors.DictCursor)

cursor.execute(sql)
newsResultList=cursor.fetchall()
print(newsResultList)

#行业龙头组合策略
idx = targetdata['市值'].groupby([targetdata['行业编码']]).idxmax()#局部数据就用局部columns
targetdata=targetdata.loc[idx,:]

#精选银行
targetdata = targetdata[(targetdata['行业编码'] == "4010.0000")].sort_values(by='PB').head(3)

#德瑞曼型风格
targetdata_adjust1 = targetdata.sort_values(by='PETTM').head(round(len(targetdata)*0.3))
targetdata_adjust2 = targetdata.sort_values(by='PB').head(round(len(targetdata)*0.3))
targetdata_adjust3 = targetdata.sort_values(by='PCF').head(round(len(targetdata)*0.3))
targetdata_adjust4 = targetdata.sort_values(by='市值',ascending=False).head(round(len(targetdata)*0.3))

targetdata_adjust5 = pd.merge(pd.merge(pd.merge(targetdata_adjust1, targetdata_adjust2, on=['标的']),targetdata_adjust3,on=['标的']),targetdata_adjust4,on=['标的'])

targetdata = targetdata_adjust5[(targetdata_adjust5['PETTM'] > 0) & (targetdata_adjust5['PB'] > 0) & (targetdata_adjust5['股息率TTM'] > 30)].head(10)

#三一选股法
targetdata_adjust1 = targetdata.sort_values(by='PETTM').head(round(len(targetdata)*0.2))
targetdata_adjust2 = targetdata.sort_values(by='PB').head(round(len(targetdata)*0.2))
targetdata_adjust3 = targetdata.sort_values(by='股息率TTM',ascending=False).head(round(len(targetdata)*0.2))
targetdata_adjust4 = pd.merge(pd.merge(targetdata_adjust1, targetdata_adjust2, on=['标的']), targetdata_adjust3, on=['标的'])
targetdata = targetdata_adjust5[(targetdata_adjust4['PETTM'] > 0) & (targetdata_adjust4['PB'] > 0)].head(10)

#迈克尔•普莱斯低估价值选股法
target=pd.DataFrame()
idx =targetdata['DebtAssetsRatio'].groupby([targetdata['行业编码']]).quantile(0.5)#局部数据就用局部columns

for i in range(len(idx)):
    df1=targetdata[targetdata['DebtAssetsRatio']<idx[i]]
    df2=df1[df1['行业编码']==idx.index[i]]
    target=pd.concat([target,df2])

targetdata = target[(target['PB'] > 0) & (target['PB'] < 2)]
targetdata['市值']=targetdata['市值'].astype(float)
id = targetdata['市值'].groupby([targetdata['行业编码']]).idxmax()#局部数据就用局部columns
targetdata=targetdata.loc[id,:]

#萝卜瑞克
target1=pd.DataFrame()
idx =targetdata['股息率TTM'].groupby([targetdata['行业编码']]).quantile(0.5)#局部数据就用局部columns

for i in range(len(idx)):
    df1=targetdata[targetdata['股息率TTM']>idx[i]]
    df2=df1[df1['行业编码']==idx.index[i]]
    target1=pd.concat([target1,df2])

target2=pd.DataFrame()
idx =targetdata['PETTM'].groupby([targetdata['行业编码']]).quantile(0.5)#局部数据就用局部columns

for i in range(len(idx)):
    df1=targetdata[targetdata['PETTM']<idx[i]]
    df2=df1[df1['行业编码']==idx.index[i]]
    target2=pd.concat([target2,df2])

target3=pd.DataFrame()
idx =targetdata['PCF'].groupby([targetdata['行业编码']]).mean()#局部数据就用局部columns

for i in range(len(idx)):
    df1=targetdata[targetdata['PCF']<idx[i]*0.8]
    df2=df1[df1['行业编码']==idx.index[i]]
    target3=pd.concat([target3,df2])

targetdata_adjust5 = pd.merge(pd.merge(pd.merge(target1, target2, on=['标的']),target3,on=['标的']))
targetdata = targetdata_adjust5[(targetdata_adjust4['DebtAssetsRatio'] < 33) & (targetdata_adjust4['PB'] > 0) & (targetdata_adjust4['PB'] < 3)]


#迈克尔喜伟
target1 = pd.DataFrame()
targetdata['股息率TTM'] = targetdata['股息率TTM'].astype(float)
idx = targetdata['股息率TTM'].groupby([targetdata['行业编码']]).quantile(0.5)  # 局部数据就用局部columns

for i in range(len(idx)):
    df1 = targetdata[targetdata['股息率TTM'] > idx[i]]
    df2 = df1[df1['行业编码'] == idx.index[i]]
    target1 = pd.concat([target1, df2])

target2 = pd.DataFrame()
targetdata['PETTM'] = targetdata['PETTM'].astype(float)
idx = targetdata['PETTM'].groupby([targetdata['行业编码']]).quantile(0.5)  # 局部数据就用局部columns

for i in range(len(idx)):
    df1 = targetdata[targetdata['PETTM'] < idx[i]]
    df2 = df1[df1['行业编码'] == idx.index[i]]
    target2 = pd.concat([target2, df2])


target3 = pd.DataFrame()
targetdata['NPParentCompanyGR1Y'] = targetdata['NPParentCompanyGR1Y'].astype(float)
idx = targetdata['NPParentCompanyGR1Y'].groupby([targetdata['行业编码']]).quantile(0.5)  # 局部数据就用局部columns

for i in range(len(idx)):
    df1 = targetdata[targetdata['NPParentCompanyGR1Y'] > idx[i]]
    df2 = df1[df1['行业编码'] == idx.index[i]]
    target3 = pd.concat([target3, df2])

target4 = pd.DataFrame()
targetdata['DebtEquityRatio'] = targetdata['DebtEquityRatio'].astype(float)
idx = targetdata['DebtEquityRatio'].groupby([targetdata['行业编码']]).quantile(0.5)  # 局部数据就用局部columns

for i in range(len(idx)):
    df1 = targetdata[targetdata['DebtEquityRatio'] < idx[i]]
    df2 = df1[df1['行业编码'] == idx.index[i]]
    target4 = pd.concat([target4, df2])


target5 = pd.DataFrame()
targetdata['CurrentRatio'] = targetdata['CurrentRatio'].astype(float)
idx = targetdata['CurrentRatio'].groupby([targetdata['行业编码']]).quantile(0.5)  # 局部数据就用局部columns

for i in range(len(idx)):
    df1 = targetdata[targetdata['CurrentRatio'] > idx[i]]
    df2 = df1[df1['行业编码'] == idx.index[i]]
    target5 = pd.concat([target5, df2])


targetdata_adjust5 = pd.merge(pd.merge(pd.merge(pd.merge(target1, target2, on=['标的']), target3, on=['标的']), target4, on=['标的']),target5, on=['标的'])
targetdata = targetdata_adjust5[(targetdata_adjust5['PETTM'] > 0)]