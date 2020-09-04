# import MySQLdb

import os
import sys

import numpy as np
import pandas as pd

import datetime
import time

from Utils.PyTime import PyDating
from Utils.PyFile import PyFile
from Utils.DbUtils import get_env_db_config

import uuid
import pymysql

class MySQLConn:
    '''
    MySQLConn
    connection to MySQL;
    '''
    def __init__(self, name="gil"):

        if name=='gil':

            host_server = 'rm-wz9s90lao15s6j4v2.mysql.rds.aliyuncs.com'
            host_office = 'rm-wz9s90lao15s6j4v2ro.mysql.rds.aliyuncs.com'
            host = host_office if ('win' in sys.platform) else host_server

            print("using gildata MySQLdb...")
            # rm-wz9s90lao15s6j4v2.mysql.rds.aliyuncs.com
            self.conn = pymysql.connect(
            host = host,
            port = 3306,
            user = 'jydb',
            passwd = 'G2W9iPwpAqF4R#202',
            db = 'jydb',
            charset = "utf8"
            )

        elif name=='dev':

            config_db = get_env_db_config()
            print(config_db)

            self.conn = pymysql.connect(
                host = config_db["host"],
                port = config_db['port'],
                user = config_db['user'],
                passwd = config_db['passwd'],
                db = config_db['db'],
                charset = config_db['charset']
            )

        else:
            self.conn = None
        self.cur = None

    def read(self, command: str):
        self.cur = self.conn.cursor()
        nlines = self.cur.execute(command)
        fetchdata = self.cur.fetchall()
        if self.cur:
            self.cur.close()

        return fetchdata, nlines

    def commit(self, command: str):

        print("MySQL excute: \n{}".format(command))

        self.cur = self.conn.cursor()
        self.cur.execute(command)
        self.conn.commit()

    def close(self):
        self.conn.commit()
        self.conn.close()
