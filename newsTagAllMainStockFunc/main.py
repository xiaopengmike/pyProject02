# 获取mysql数据
import pymysql
import requests
import json

host01 = '47.107.33.27'
port01: 3306
user01 = 'root'
password01 = 'kaisa!'
db_name01 = 'kgl_user_center'
charset01 = "utf8"

connection01 = pymysql.connect(
    host=host01,
    user=user01,
    password=password01,
    charset='utf8',
    db=db_name01
)
cursor01 = connection01.cursor(cursor=pymysql.cursors.DictCursor)

sql = "SELECT eventName,tagEvent,type FROM app_config_shares_com_event_ori_tags"
cursor01.execute(sql)
# 获取compEventTagDictLi
compEventTagDictLi = cursor01.fetchall()


#拿Tdx最新资讯数据打预警标签