
import numpy as np
import pandas as pd

def EMA(data, timeperiod=20, nonan=False):
    # check.
    # edit. 2020.6.20

    # for i in range(len(data)):
    #     if (i<timeperiod-1):
    #         if (nonan):
    #             emas[i] = np.mean(data[:i + 1])
    #         else:
    #             pass
    #     elif (i==timeperiod-1):
    #         emas[i] = np.mean(data[:i+1])
    #     else:
    #         if not np.isnan(emas[i-1]):
    #             emas[i] = emas[i - 1] * ((timeperiod - 1) / (timeperiod + 1)) + data[i] * (2 / (timeperiod + 1))
    #         else:
    #             emas[i] = np.mean(data[i + 1 - timeperiod:i + 1])

    emas = np.full(data.shape, np.nan)
    emadatalen = 0
    for i in range(len(data)):
        if not np.isnan(data[i]):
            emadatalen += 1
        if emadatalen==0:
            pass
        elif emadatalen<timeperiod:
            if nonan:
                emas[i] = np.mean(data[i+1-emadatalen:i+1])
        elif emadatalen==timeperiod:
            emas[i] = np.mean(data[i+1-emadatalen:i+1])
        else:
            emas[i] = emas[i - 1] * ((timeperiod - 1) / (timeperiod + 1)) + data[i] * (2 / (timeperiod + 1))
    return emas


def MACD(data, fastperiod=12, slowperiod=26, signalperiod=9, nonan=False):
    # check.
    # edit. 2020.6.20

    ema12 = EMA(data, timeperiod=12, nonan=nonan)
    ema26 = EMA(data, timeperiod=26, nonan=nonan)

    difs = ema12 - ema26
    deas = EMA(difs, timeperiod=9, nonan=nonan)
    macds = 2 * (difs - deas)
    return difs, deas, macds
