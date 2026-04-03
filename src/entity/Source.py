import numpy as np
import pandas as pd
import dolphindb as ddb
from typing import List, Dict

class Source:
    def __init__(self, session: ddb.session):
        self.session: ddb.session = session
        self.factorDateCol: str = ""
        self.labelDateCol: str = ""
        self.factorSymbolCol: str = ""
        self.labelSymbolCol: str = ""
        self.factorDBName: str = ""
        self.labelDBName: str = ""
        self.factorTBName: str = ""
        self.labelTBName: str = ""
        self.factorIndicatorCol: str = ""
        self.labelIndicatorCol: str = ""
        self.factorSymbolCol: str = ""
        self.labelSymbolCol: str = ""
        self.factorValueCol: str = ""
        self.labelValueCol: str = ""
        self.factorCondition: str = ""
        self.labelCondition: str = ""
        self.dataDateCol: str = "tradeDate"
        self.dataSymbolCol: str = "symbol"
        self.dataObjName: str = "dataObj_"
        self.resultDBName: str = ""
        self.resultTBName: str = ""

    def init(self, factorDict: Dict[str, str], labelDict: Dict[str, str], resultDict: Dict[str, str]):
        self.factorDBName = factorDict["dbName"]
        self.factorTBName = factorDict["tbName"]
        self.factorDateCol = factorDict["dateCol"]
        self.factorSymbolCol = factorDict["symbolCol"]
        self.factorIndicatorCol = factorDict["indicatorCol"]
        self.factorValueCol = factorDict["valueCol"]
        self.factorCondition = factorDict["condition"]
        self.labelDBName = labelDict["dbName"]
        self.labelTBName = labelDict["tbName"]
        self.labelDateCol = labelDict["dateCol"]
        self.labelSymbolCol = labelDict["symbolCol"]
        self.labelIndicatorCol = labelDict["indicatorCol"]
        self.labelValueCol = labelDict["valueCol"]
        self.labelCondition = labelDict["condition"]
        self.resultDBName = resultDict["dbName"]
        self.resultTBName = resultDict["tbName"]

    def getDateList(self) -> List[pd.Timestamp]:
        """
        获取当前库内所有时间, 返回列表
        """
        if self.factorCondition not in ["", None]:
            factorDF = self.session.run(f"""
                select count(*) from loadTable("{self.factorDBName}", "{self.factorTBName}")
                    where {self.factorCondition}
                    group by {self.factorDateCol} as tradeDate 
            """)
        else:
            factorDF = self.session.run(f"""
                select count(*) from loadTable("{self.factorDBName}", "{self.factorTBName}")
                    group by {self.factorDateCol} as tradeDate
            """)
        dateList = [pd.Timestamp(i) for i in factorDF["tradeDate"].tolist()]
        return dateList

    def deleteByDateAndFactorList(self, startDate: str, endDate: str, factorList: List[str]):
        startDate = pd.Timestamp(startDate).strftime("%Y.%m.%d")
        endDate = pd.Timestamp(endDate).strftime("%Y.%m.%d")
        self.session.upload({"factorList": factorList})
        self.session.run(f"""
        startDate = {startDate}
        endDate = {endDate}
        delete from loadTable("{self.resultDBName}", "{self.resultTBName}") 
        where signal in factorList and (tradeDate between startDate and endDate)
        """)

    def getFactorList(self) -> List[str]:
        """
        获取当前库内所有因子, 返回列表
        """
        if self.factorCondition not in ["", None]:
            factorDF = self.session.run(f"""
                select count(*) from loadTable("{self.factorDBName}", "{self.factorTBName}")
                    where {self.factorCondition}
                    group by {self.factorIndicatorCol} as factorName 
            """)
        else:
            factorDF = self.session.run(f"""
                select count(*) from loadTable("{self.factorDBName}", "{self.factorTBName}")
                    group by {self.factorIndicatorCol} as factorName
            """)
        factorList = factorDF["factorName"].tolist()
        return factorList

    def checkFactorList(self, factorList: List[str]) -> List[str]:
        """
        确认输入的因子列表是否都在库内->返回在库内的因子列表
        """
        return [i for i in factorList if i in self.getFactorList()]