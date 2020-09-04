
'''
rev. on MACD computation.
edit. 2020.6.20

智能投顾：
1.形态选股+策略选股；
形态选股有5个策略，策略选股也有5个策略
2. 数据从jildata MySqlDb提取；
3. signals会上传到dev MySqlDb中; 另会写到本地文件Jsignals;

liuyi09@kaisagroup.com
edit. 2020.6.18
'''

import os
import sys
import time
import datetime

import numpy as np
import pandas as pd
import json

import traceback

from Core.StrategyConnRC0617 import JstrategyBase
from Core.AllStrategiesRC0617 import Jstrategy

from Utils.PyFile import PyFile
from Utils.log import setup_custom_logger_rotating as setup_logger

PyFile.ChDir(sys.argv[0])

logger = setup_logger('Trader')
logger.info("Hello, World.")

import config

print("project config:")
[print(item) for item in config.AllConfig]
print("strategy config:")
[print(item) for item in config.StrategyConfig]

global production
production = config.AllConfig['production']

if production:
    print("it is production.")
else:
    print("it is dev. en.")

def StrategyTraderOneJ(strategy_no=0):

    jstrategy = Jstrategy(loaddata=False, selectdate=20200616)

    if not jstrategy.status:
        print("status error of jstrategy.")
    else:
        jstrategy.print_info()

        # strategy_no = 3
        strategy_info = config.StrategyConfig[strategy_no]
        print(strategy_info)

        jstrategy.get_strategy_api_name(**strategy_info)

        jstrategy.set_strategy_info(**strategy_info)

        strategy_max_num = config.AllConfig["strategy_max_num"]
        jstrategy.set_strategy_max_num(strategy_max_num)
        jstrategy.run_strategy_api()
        jstrategy.run_strategy_update()

def StrategyTraderJJ():

    run_complete_flag = False

    while (not run_complete_flag):

        logger.info("strategy run.")

        run_complete_flag = True
        for strategy_no, strategy_info in enumerate(config.StrategyConfig):

            wday = int(datetime.datetime.now().strftime("%w"))

            strategy_type = strategy_info['type']
            if strategy_type=='SHAPEX':
                # SHAPEX;
                if wday not in [1,2,3,4,5]:
                    continue
            elif strategy_type=='FINX':
                # FINX;
                wday = int(datetime.datetime.now().strftime("%w"))
                if wday not in [1]:
                    continue
            else:
                continue

            print("\n"+"*"*64)
            print("*"*64)
            print("*"*64+"\n")
            try:
                jstrategy = Jstrategy(loaddata=(strategy_no==0))
            except Exception:
                logger.error("except")
                exstr = traceback.format_exc()
                logger.error(exstr)

            if not jstrategy.status:
                run_complete_flag = False
                logger.info("status error of jstrategy. does not exists.")
                # send me information.
                logger.info("retry again later")
                for _ in range(5):
                    print("waiting for one min.")
                    time.sleep(60)
                break;

            jstrategy.print_info()
            jstrategy.get_strategy_api_name(**strategy_info)
            jstrategy.set_strategy_info(**strategy_info)

            jstrategy.set_strategy_max_num(config.AllConfig["strategy_max_num"])
            jstrategy.run_strategy_api()
            jstrategy.run_strategy_update()

    return None

'''
MAIN RUN.
time plan at 17:00 everyday.
'''
if __name__=='__main__':

    try:
        env = sys.argv[1]
    except:
        env = "dev"
    if env not in ["dev","sit","uat","pro"]:
        env = "dev"
    try:
        server = sys.argv[2]
    except:
        server = "office"
    if server not in ["server","office"]:
        server = "server"


    print("*"*32)
    print("this env. is {}-{} ".format(env, server))
    print("*"*32)

    with open("env.json","w") as f:
        json.dump({"env":env,"server":server}, f)

    logger.info("main run.")

    # StrategyTraderOneJ(strategy_no=0)

    try:

        StrategyTraderJJ()

    except Exception:

        logger.error("except")
        exstr = traceback.format_exc()
        logger.error(exstr)

    finally:

        logger.info("complete")


