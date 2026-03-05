import pandas as pd
import dolphindb as ddb
import streamlit as st
from functools import lru_cache
from typing import Dict, List
from src.entity.Source import Source

class Result(Source):
    def __init__(self, session: ddb.session):
        super().__init__(session)
        self.startDate: pd.Timestamp = None
        self.endDate: pd.Timestamp = None
        self.callBackDays: List[int] = []
        self.afterStatDays: List[List[int]] = []
        self.barRetLabelName: str = ""

    def setConfig(self, config: Dict):
        """初始化结果配置项"""
        self.startDate = pd.Timestamp(config["startDate"]) if config["startDate"] is not None else pd.Timestamp("20200101")
        self.endDate = pd.Timestamp(config["endDate"]) if config["endDate"] is not None else pd.Timestamp.now().date()
        self.callBackDays = config["callBackDays"]
        self.afterStatDays = config["afterStatDays"]
        self.barRetLabelName = config["barRetLabelName"]

    def deleteSignalRes(self, signalList: List[str]) -> None:
        """
        删除结果数据库中对应信号的结果
        """
        self.session.upload({"deleteSignals": signalList})
        self.session.run(f"""
        delete from loadTable("{self.resultDBName}", "{self.resultTBName}") where signal in deleteSignals;
        """)

    def initResDB(self, dropDB: bool = False) -> None:
        """
        创建结果数据库
        """
        if dropDB and self.session.existsDatabase(self.resultDBName):
            self.session.dropDatabase(self.resultDBName)
        if not self.session.existsTable(dbUrl=self.resultDBName, tableName=self.resultTBName):
            colName = ["signal","symbol","tradeDate","period","after","indicator","value"]
            colType = ["SYMBOL","SYMBOL","DATE","INT","INT","SYMBOL","DOUBLE"]
            self.session.run(f"""
            db=database("{self.resultDBName}",RANGE,2010.01M+(0..30)*12,engine="OLAP")
            schemaTb=table(1:0,{colName}, {colType});
            t=db.createDimensionTable(table=schemaTb, tableName="{self.resultTBName}")
            """)  # DolphinDB 维度表 - 信号结果数据库

class Stats(Result):    # for SignalPlot
    def __init__(self, session: ddb.session):
        super().__init__(session)