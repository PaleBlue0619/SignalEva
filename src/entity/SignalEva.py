import os, sys, tqdm
import pandas as pd
import dolphindb as ddb
from typing import List, Dict
from src.entity.Source import Source
from src.entity.Eva import Eva
from src.entity.Result import Result,Stats
from src.utils.utils import split_list, get_splitTradeTime, get_dateDictFromDF
pd.set_option('display.max_columns', None)

class SignalEva(Eva, Stats):
    def __init__(self, session: ddb.session):
        super().__init__(session)

    @staticmethod
    def run(session: ddb.session, cfg: Dict[str, str], signalList: List[str], dropDB: bool = False, window: int = 60,
            simpleEva: bool = False):
        """
        运行评价函数
        dropDB: 是否删除原始数据库
        dropOri: 是否删除原始结果
        """
        # 0.配置
        SigObj = SignalEva(session)
        SigObj.init(factorDict=cfg["factor"],
                    labelDict=cfg["label"],
                    resultDict=cfg["result"])
        SigObj.setConfig(config=cfg["config"])
        SigObj.initResDB(dropDB=dropDB)
        SigObj.initDef()  # 初始化函数定义

        if isinstance(signalList, list) and len(signalList) == 0:
            signalList = SigObj.getAllFactor()  # 获取因子库中的所有因子(上游)
        elif isinstance(signalList, bool) and not signalList:
            signalList = SigObj.getAllFactor()
        oldSignalDict: Dict[int, List[str]] = {}
        newSignalDict: Dict[int, List[str]] = {}
        for period in tqdm.tqdm(SigObj.callBackDays, desc="getting signalList byPeriods..."):
            oldSignalDict[period] = SigObj.getSignalListByPeriod(period=period)
            newSignalDict[period] = [signal for signal in signalList if signal not in oldSignalDict[period]]  # 本次需要新加入的信号
        # if not dropDB:
        #     SigObj.deleteSignalRes(signalList=signalList) # 这里上游信号不太可能发生变动, 所以下游也不需要变动

        # 1. 对于老信号 -> 一一计算
        for period, oldSignals in oldSignalDict.items():
            afterStatDays = SigObj.afterStatDays[SigObj.callBackDays.index(period)]
            if oldSignals:
                dateDF = SigObj.getDateRangeByFactor(factorList=oldSignals, period=period)   # 获取当前结果库中的时间范围
                dateDict = get_dateDictFromDF(dateDF=dateDF)    # Dict(uniqueMaxDate, List[signal])
                for date, signalWithMaxDate in tqdm.tqdm(dateDict.items(), total=len(dateDict)):
                    startDate = date + pd.Timedelta(1, "D")
                    endDate = SigObj.endDate
                    if startDate > endDate:  # 说明已经满足了endDate的最新要求
                        continue
                    signalList_nested = split_list(signalWithMaxDate, k=30)
                    for signalList in tqdm.tqdm(signalList_nested, desc=f"old signals' eva under period={period}..."):
                        SigObj.eva(startDate=startDate, endDate=endDate, signalList=signalList,
                                    callBackDays=period, afterStatDays=afterStatDays, simpleEva=simpleEva)

        # 2.批量计算新信号 -> 插入至数据库
        for period, newSignals in newSignalDict.items():
            afterStatDays = SigObj.afterStatDays[SigObj.callBackDays.index(period)]
            if newSignals:
                signalList_nested = split_list(l=newSignals, k=30)
                dateList_nested = get_splitTradeTime(session=session, startDate=SigObj.startDate, endDate=SigObj.endDate, window=window)
                for signalList in tqdm.tqdm(signalList_nested, desc=f"new signals' eva under period={period}..."):
                    # 按照时间进行分割
                    for dateList in dateList_nested:
                        startDate, endDate = dateList[0], dateList[-1]
                        # 对该信号列表 + 该时间段的信号数据进行评价 -> 写入评价结果数据库
                        SigObj.eva(startDate=startDate, endDate=endDate, signalList=signalList,
                                    callBackDays=period, afterStatDays=afterStatDays, simpleEva=simpleEva)
    @staticmethod
    def givenPeriodAndSignalPlot(cfg: Dict[str, str]):
        SigObj = FactorSignal(session)
        SigObj.init(factorDict=cfg["factor"],
                    labelDict=cfg["label"],
                    resultDict=cfg["result"])
        SigObj.setConfig(cfg["config"])
        SigObj.Plot_(panelName="A")

    @staticmethod
    def givenPeriodAndSymbolPlot(cfg: Dict[str, str]):
        SigObj = FactorSignal(session)
        SigObj.init(factorDict=cfg["factor"],
                    labelDict=cfg["label"],
                    resultDict=cfg["result"])
        SigObj.setConfig(cfg["config"])
        SigObj.Plot_(panelName="B")