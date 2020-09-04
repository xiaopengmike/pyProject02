
#### project: intellegent_investment_signals_v1.0.0
##### desc: production of two type strategies of `SHAPEX` and `FINX`.

#### centos安装依赖 

##### 1. install python
`yum install python3-devel, python3`

##### 2. install package
`pip install numpy, pandas, pymysql`

`python -m pip install --upgrade pip`

`python -m pip install --upgrade numpy`

`python -m pip install --upgrade pandas`

##### 3. linux任务
cd /opt/smart_investment_signals
10 17 * * *  /usr/bin/python3 -u /opt/smart_investment_signals/StrategyTrader_RC0618.py  >/logs/smart_investment_signals.log 2>&1 &
  
##### 4. 环境配置

python x.py dev office
python x.py uat server
python x.py pro server

`
02 17 * * * cd /opt/intellegent_investment_signals && /usr/bin/python3 -u StrategyTrader_RC0618.py sit server >/logs/fucking.log 2>&1 &
pwd
`
