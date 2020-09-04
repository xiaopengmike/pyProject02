
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import time

def setup_custom_logger(name, log_level=logging.INFO):
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.addHandler(handler)
    return logger

'''
import logging
from log import setup_custom_logger_rotating as setup_logger

logger = setup_logger('LOGGER')
'''

def setup_custom_logger_rotating(name="LOGGER", log_level=logging.INFO):

    ## logging设置;

    if not os.path.exists("log"):
        os.mkdir("log")

    # logger = logging.getLogger(__name__)
    logger = logging.getLogger(name)

    logger.setLevel(level=logging.INFO)
    # logger.setLevel(level=logging.DEBUG)
    rf_handler = RotatingFileHandler("log/log{}.txt".format(time.strftime('_%y%m%d', time.localtime(time.time()))),
                                     encoding='utf-8', maxBytes=1024*1024, backupCount=1024)

    rf_handler.setLevel(logging.INFO)
    # rhandler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    rf_handler.setFormatter(formatter)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    logger.addHandler(rf_handler)
    logger.addHandler(console)

    return logger

