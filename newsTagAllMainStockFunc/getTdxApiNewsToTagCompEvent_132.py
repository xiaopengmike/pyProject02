# 获取通达信7*24， 标出股票
import pymysql
import requests
import json
import uuid
from time import gmtime, strftime
import config

tdxMarketCode = '132'

apiIPAdress = config.apiIPAdress['local']
print('apiIPAdress:'+apiIPAdress)


def getTdxStockMarketNews(marketIndex):
    url = 'http://8.129.11.22:7619/TQLEX?Entry=CWServ.mzx_yw'
    postJson = json.dumps({"Params": [marketIndex, "", "1", "9999"]})
    tdxResponse = requests.post(url, data=postJson).text
    tdxResponse = json.loads(tdxResponse)
    return tdxResponse


tdxResponseData = getTdxStockMarketNews(tdxMarketCode)

tdxNewsLi = tdxResponseData['ResultSets'][0]['Content']
tdxNewsDictLi = []
tdxNewsDict = {}
for tdxNewsItem in tdxNewsLi:
    tdxNewsDict = {}
    try:
        tdxNewsDict['title'] = tdxNewsItem[0]
        tdxNewsDict['content'] = tdxNewsItem[2]
        tdxNewsDict['publish_time'] = tdxNewsItem[1]
        # 在这里造数据，测试特殊情况
        # tdxNewsDict['title'] = '中信证券研报称，中国石化中报基本符合预期02'
        # tdxNewsDict['content'] = '中信证券研报称，中国石化中报基本符合预期02'
        tdxNewsDictLi.append(tdxNewsDict)
    except:
        continue

# tdxNewsDictLi调接口标出股票

# 循环发请求，插入目标mysql数据库
host02 = '47.107.33.27'
port02: 3306
user02 = 'root'
password02 = 'kaisa!'
db_name02 = 'kgl_user_center'
charset02 = "utf8"

connection02 = pymysql.connect(
    host=host02,
    user=user02,
    password=password02,
    charset='utf8',
    db=db_name02
)
cursor02 = connection02.cursor(cursor=pymysql.cursors.DictCursor)


# 循环发form请求
def itemApiResIntoDb(newsResult, tdxMarketCode):
    # 需区分tdxMarketCode
    tittle = newsResult['title']
    content = newsResult['content']
    time = newsResult['publish_time']

    url = "http://"+apiIPAdress+":8891/allMainStockTagSearch"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    form_data = {
        "tdxMarketCode": tdxMarketCode,
        "tittle": tittle,
        "content": content,
        "time": time,
    }

    response = requests.post(url, data=form_data, headers=headers).text
    print('response')
    response = json.loads(response)
    res_content = response['content']
    res_tittle = response['tittle']
    res_code = response['code']
    res_time = response['time']
    res_stockName = response['stockName']
    res_market = response['market']
    res_contentStockCountDictLi = response['contentStockCountDictLi']
    # res_contentStockCountDictLi = json.dumps(res_contentStockCountDictLi,ensure_ascii=False)
    created_by = 'kaisa.xp06'
    last_modified_by = 'kaisa.xp06'
    gmt_create = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    gmt_modified = strftime("%Y-%m-%d %H:%M:%S", gmtime())


    print(res_stockName)
    print(res_code)
    print('res_Type')
    print('type')
    print('-----------------------')

    #如果标到了股票,每个股票+此资讯单独入库
    newsItemWithOneStockDic = {}
    if len(res_contentStockCountDictLi)>0:
        for stockCountDict in res_contentStockCountDictLi:
            id = str(uuid.uuid4()).replace("-", "")[:18]
            newsItemWithOneStockDic['name'] = stockCountDict['name']
            newsItemWithOneStockDic['market'] = stockCountDict['market']
            newsItemWithOneStockDic['SecuCode'] = stockCountDict['SecuCode']
            newsItemWithOneStockDic['content'] = res_content
            newsItemWithOneStockDic['title'] = res_tittle
            newsItemWithOneStockDic['time'] = res_time

            # 如果有按股票重复，则用ON DUPLICATE KEY UPDATE更新那条数据。
            sql = "INSERT INTO app_config_shares_news_info_get_all_shares (id,content,title,code,publish_time,stock_name,market,created_by,last_modified_by,gmt_create,gmt_modified) VALUE ('{id}','{title}','{content}','{code}','{publish_time}','{stock_name}','{market}','{created_by}','{last_modified_by}','{gmt_create}','{gmt_modified}') ON DUPLICATE KEY UPDATE id='{id}',content='{content}',title='{title}',code='{code}',publish_time='{publish_time}',stock_name='{stock_name}',market='{market}',created_by='{created_by}',last_modified_by='{last_modified_by}',gmt_create='{gmt_create}',gmt_modified='{gmt_modified}'".format(
                id=id,
                content=res_content,
                title=res_tittle,
                publish_time=res_time,
                code=newsItemWithOneStockDic['SecuCode'],
                stock_name=newsItemWithOneStockDic['name'],
                market=newsItemWithOneStockDic['market'],
                created_by=created_by,
                last_modified_by=last_modified_by,
                gmt_create=gmt_create,
                gmt_modified=gmt_modified,
            )

            cursor02.execute(sql)

            connection02.commit()

    # if (res_code):
    #         sql = "INSERT INTO app_config_shares_news_info_get_all_shares (id,content,title,code,time,stock_name,market,created_by,last_nmodified_by,gmt_create,gmt_modified) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s') ON duplicate KEY UPDATE title = title " % (
    #         id, res_content, res_tittle, res_code, res_time, res_stockName, res_market,created_by, last_modified_by,gmt_create, gmt_modified)


for newsResult in tdxNewsDictLi:
    try:
        itemApiResIntoDb(newsResult, tdxMarketCode)
    except:
        continue
