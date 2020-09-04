
'''
日期类函数
'''

import datetime
import time

class PyDating():

    @staticmethod
    def dt_to_ymd_int(mydate, format='%Y%m%d'):
        '''datetime.datetime to ymd int'''
        num = int(mydate.strftime(format))
        return num

    @staticmethod
    def dt_to_string(mydate, format='%Y%m%d'):
        str_: str = mydate.strftime(format)
        return str_

    @staticmethod
    def ymd_int_to_dt(num, format='%Y%m%d'):
        '''ymd int to datetime.datetime'''
        mydate = datetime.datetime.strptime(str(num), format)
        return mydate

    @staticmethod
    def get_today():
        '''get today'''
        now1 = datetime.datetime.now()
        now1 = int(now1.strftime('%Y%m%d'))
        return now1

if __name__=='__main__':

    print("start.")
    item = PyDating.ymd_int_to_dt(20190101)
    print(item)
    print(type(item))
    item = PyDating.dt_to_ymd_int(item)
    print(item)
    print(type(item))
