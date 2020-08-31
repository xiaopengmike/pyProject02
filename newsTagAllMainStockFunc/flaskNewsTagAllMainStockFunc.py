# api输入文章tile,content，
# 返回content，market，code, allCodeInfoDict
# coding=utf-8
import flask
import json
from flask import request
import dataSource

# flask： web框架，通过flask提供的装饰器@server.route()将普通函数转换为服务
server = flask.Flask(__name__)


@server.route('/allMainStockTagSearch', methods=['post'])
def stockTagSearch():
    # 获取到post的数据
    tdxMarketCode = request.form.get('tdxMarketCode')
    # print('tdxMarketCode' + ':' + tdxMarketCode)

    content = request.form.get('content')
    tittle = request.form.get('tittle')
    time = request.form.get('time')
    newsContentDict = {
        'content': content,
        'tittle': tittle,
        'time': time,
    }
    # print(newsContentDict)

    # 统计股票 公司预警事件出现次数
    # HK、HS、US股票数据分开，或合并
    if (tdxMarketCode == '131'):
        stockSearchWordLi = dataSource.hsStockSearchWordLi
    if (tdxMarketCode == '132'):
        stockSearchWordLi = dataSource.hkStockSearchWordLi

    for item in stockSearchWordLi:
        if item.__contains__('_id'):
            item = item.pop('_id')

    tagSearchWordLi = dataSource.tagSearchWordLi

    def strStockTagCount(responseDict):
        # 标题、正文一起分析
        titleStr = responseDict['tittle']
        contentStr = responseDict['content']
        combineStr = titleStr + contentStr
        print('combineStr')
        print(combineStr)
        # 循环插入搜索关键词，如果有count，就放到dict里
        contentStockCountDictLi = []
        contentStockCountDict = {}
        for stockSearchWordDic in stockSearchWordLi:
            if combineStr.count(stockSearchWordDic['name']):
                print(stockSearchWordDic)
                contentStockCountDict = stockSearchWordDic
                contentStockCountDict['count'] = combineStr.count(stockSearchWordDic['name'])
                contentStockCountDictLi.append(contentStockCountDict)
        # combineStr循环结束后排序
        contentStockCountDictLi.sort(key=lambda k: (k.get('count', 0)), reverse=True)
        print(contentStockCountDictLi)

        if len(contentStockCountDictLi):
            responseDict['stockName'] = contentStockCountDictLi[0]['name']
            responseDict['market'] = contentStockCountDictLi[0]['market']
            responseDict['code'] = contentStockCountDictLi[0]['SecuCode']
        else:
            responseDict['stockName'] = ""
            responseDict['market'] = ""
            responseDict['code'] = ""

        if len(contentStockCountDictLi):
            responseDict['contentStockCountDictLi'] = contentStockCountDictLi

        return responseDict

    response = strStockTagCount(newsContentDict)
    return json.dumps(response, ensure_ascii=False)


if __name__ == '__main__':
    server.run(port=8891, host='0.0.0.0')  # 指定端口、host,0.0.0.0代表不管几个网卡，任何ip都可以访问
