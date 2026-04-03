import os, sys, json, json5, tqdm
import pandas as pd
import dolphindb as ddb
from typing import List, Dict
# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from src.entity.Source import Source
from src.entity.Eva import Eva
from src.entity.Result import Result,Stats
from src.utils.utils import split_list, get_splitTradeTime
pd.set_option('display.max_columns', None)

class FactorSignal(Eva, Stats):
    def __init__(self, session: ddb.session):
        super().__init__(session)

    @staticmethod
    def run(cfg: Dict[str, str], signalList: List[str], dropDB: bool = False, window: int = 60):
        """
        运行评价函数
        dropDB: 是否删除原始数据库
        dropOri: 是否删除原始结果
        """
        SigObj = FactorSignal(session)
        SigObj.init(factorDict=cfg["factor"],
                    labelDict=cfg["label"],
                    resultDict=cfg["result"])
        SigObj.initDef()  # 初始化函数定义
        if not signalList:
            signalList = SigObj.getFactorList()
        signalList_nested = split_list(l=signalList, k=100)
        SigObj.setConfig(config=cfg["config"])
        SigObj.initResDB(dropDB=dropDB)
        # if not dropDB:
        #     SigObj.deleteSignalRes(signalList=signalList) # 这里上游信号不太可能发生变动, 所以下游也不需要变动
        dateList_nested = get_splitTradeTime(session=session, startDate=SigObj.startDate, endDate=SigObj.endDate, window=window)
        for signalList in tqdm.tqdm(signalList_nested, desc="signal eva..."):
            # 按照时间进行分割
            for dateList in dateList_nested:
                startDate, endDate = dateList[0], dateList[-1]
                # 对该信号列表 + 该时间段的信号数据进行评价 -> 写入评价结果数据库
                for callBackDays, afterStatDays in zip(SigObj.callBackDays, SigObj.afterStatDays):
                    SigObj.eva(startDate=startDate, endDate=endDate, signalList=signalList,
                               callBackDays=callBackDays, afterStatDays=afterStatDays)

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

if __name__ == "__main__":
    session = ddb.session("localhost", 8848, "admin", "123456")
    with open(r"D:\DolphinDB\Project\FactorSignal\src\cons\signalCons.json5", "r", encoding="utf-8") as f:
        sigCfg = json5.load(f)
    FactorSignal.run(cfg=sigCfg, signalList=None, dropDB=True, window=1)
    # FactorSignal.givenPeriodAndSignalPlot(cfg=sigCfg)
    # FactorSignal.givenPeriodAndSymbolPlot(cfg=sigCfg)