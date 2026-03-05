import os, sys, json, json5, tqdm
import pandas as pd
import dolphindb as ddb
from typing import List, Dict
# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from src.entity.Source import Source
from src.entity.Result import Result,Stats
from src.utils.utils import split_list

class FactorSignal(Stats):
    def __init__(self, session: ddb.session):
        super().__init__(session)

    @staticmethod
    def run(cfg: Dict[str, str], signalList: List[str], dropDB: bool = False):
        """
        运行评价函数
        """
        SigObj = FactorSignal(session)
        SigObj.init(factorDict=cfg["factor"],
                    labelDict=cfg["label"],
                    resultDict=cfg["result"])
        if not signalList:
            signalList = SigObj.getFactorList()
        signalList_nested = split_list(l=signalList, k=10)
        SigObj.setConfig(config=cfg["config"])
        SigObj.initResDB(dropDB=dropDB)
        if not dropDB:
            SigObj.deleteSignalRes(signalList=signalList)
        for signalList in tqdm.tqdm(signalList_nested):
            print(signalList)

if __name__ == "__main__":
    session = ddb.session("localhost", 8848, "admin", "123456")
    with open(r".\cons\signalCons.json5", "r", encoding="utf-8") as f:
        sigCfg = json5.load(f)
    FactorSignal.run(cfg=sigCfg, signalList=None, dropDB=False)