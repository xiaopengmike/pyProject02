

AllConfig = {
    "server": False,
    "production": True,
    "strategy_max_num": 10
}

StrategyConfig = [
    ## SHAPEX;
    {"type": "SHAPEX", "name": "上涨趋势跟踪", "item": "0", "code": "shangzhangqushi", "version": "rc20200615"},
    {"type": "SHAPEX", "name": "新高突破策略", "item": "1", "code": "xingaotupo", "version": "rc20200615"},
    {"type": "SHAPEX", "name": "长下影", "item": "2", "code": "changxiaying", "version": "rc20200615"},
    {"type": "SHAPEX", "name": "成交量上扬", "item": "3", "code": "chengjiaoliangshangyang", "version": "rc20200615"},
    {"type": "SHAPEX", "name": "MACD低位金叉", "item": "4", "code": "macddiweijincha", "version": "rc20200615"},

    ## FINX;
    {"type": "FINX", "name": "PB-ROE价值投资", "item": "0", "code": "pbroejiazhitouzi", "version": "rc20200616"},
    {"type": "FINX", "name": "小盘价值投资", "item": "1", "code": "xiaopanjiazhitouzi", "version": "rc20200616"},
    {"type": "FINX", "name": "成长投资", "item": "2", "code": "chengzhangtouzi", "version": "rc20200616"},
    {"type": "FINX", "name": "现金为王投资", "item": "3", "code": "xianjinweiwangtouzi", "version": "rc20200616"},
    {"type": "FINX", "name": "高股息价值投资", "item": "4", "code": "gaoguxijiazhitouzi", "version": "rc20200616"}
]

if __name__=='__main__':
    from StrategyTrader import Jstrategy
    for item in StrategyConfig:
        api_name = Jstrategy.get_strategy_api_name(**item)


'''
strategy_api_name: strategy_api_shapex_0_shangzhangqushi_rc20200615
strategy_api_name: strategy_api_shapex_1_xingaotupo_rc20200615
strategy_api_name: strategy_api_shapex_2_changxiaying_rc20200615
strategy_api_name: strategy_api_shapex_3_chengjiaoliangshangyang_rc20200615
strategy_api_name: strategy_api_shapex_4_macddiweijincha_rc20200615
strategy_api_name: strategy_api_finx_0_peroejiazhitouzi_rc20200616
strategy_api_name: strategy_api_finx_1_xiaopanjiazhitouzi_rc20200616
strategy_api_name: strategy_api_finx_2_chengzhangtouzi_rc20200616
strategy_api_name: strategy_api_finx_3_xianjinweiwangtouzi_rc20200616
strategy_api_name: strategy_api_finx_4_gaoguxijiazhitouzi_rc20200616
'''

