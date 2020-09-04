#!/bin/bash  
#日志目录
today1=$(date +%Y%m%d_%H%M)
/usr/bin/python3 -u /opt/intellegent_investment_signals/StrategyTrader_RC0618.py  >/logs/intellegent_investment_signals-${today1}.log 2>&1 &
