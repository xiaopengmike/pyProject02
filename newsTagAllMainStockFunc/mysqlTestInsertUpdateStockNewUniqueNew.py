import pymysql
import requests
import json
import uuid
from time import gmtime, strftime
import config

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
all_stock_dict_li = [
]

# all_stock_dict_li='中文编码测试'

all_stock_dict_li=json.dumps(all_stock_dict_li,ensure_ascii=False)

stock_name='万科01'
title='万科新闻01'
#
stock_name='万科'

# sql = "INSERT INTO app_config_shares_news_info_get_all_shares (stock_name) VALUES ('{stock_name01}')" .format(stock_name01=stock_name)

realVar = '7'
#
sql = "INSERT INTO app_config_shares_news_info_get_all_shares (stock_name,market) VALUE('万科','{midVar}') ON DUPLICATE KEY UPDATE market= '{midVar}'".format(midVar=realVar)

# sql = "INSERT INTO app_config_shares_news_info_get_all_shares (stock_name,market) VALUE('万科02','{midVar}')".format(midVar=realVar)

realVar = '10'

sql = "INSERT INTO app_config_shares_news_info_get_all_shares (stock_name) VALUE ('{midVar}')".format(
                midVar=realVar)


cursor02.execute(sql)
connection02.commit()