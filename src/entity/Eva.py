import pandas as pd
import dolphindb as ddb
from typing import Dict, List
from src.entity.Result import Result

class Eva(Result):
    def __init__(self, session: ddb.session):
        super().__init__(session)

    def eva(self, signalList: List[str]):
        self.session.upload({"factorList": signalList})
        """初始化定义"""
        startDate = pd.Timestamp(self.startDate).strftime("%Y.%m.%d")
        endDate = pd.Timestamp(self.endDate).strftime("%Y.%m.%d")
        self.session.run(rf"""
        // 参数配置
        startDate = {self.startDate}
        endDate = {self.endDate}
        callBackDays = 120
        afterStatDays = [3,4]
        barRetLabelName = "{self.barRetLabelName}"
        realStartDate = temporalAdd(startDate, -callBackDays, "d")
        realEndDate = endDate
        factorDB = "{self.factorDBName}"
        factorTB = "{self.factorTBName}"
        labelDB = "{self.labelDBName}"
        labelTB = "{self.labelTBName}"
        
        // 取数
        signalDF = select symbol,tradeDate,factor,value from loadTable(factorDB,factorTB) where factor in factorList and (tradeDate between realStartDate and realEndDate) 
        labelDF = select cont as symbol,tradeDate,value as ret from loadTable(labelDB,labelTB) where label == barRetLabelName and (tradeDate between realStartDate and realEndDate)
        signalDF = lj(signalDF, labelDF, `symbol`tradeDate)
        
        
    summaryStats = select symbol, tradeDate, factor, value, ret,
        callBackDays as `period, 0 as `after,
        mcount(iif(value == 1, 1, NULL), callBackDays) as posNum,
        mcount(iif(value == -1, 1, NULL), callBackDays) as negNum,
        mcount(iif(value == 0, 1, NULL), callBackDays) as zeroNum,
        condUp1D = iif(move(ret,-1)>0, 1, 0),
        condUp2D = iif((move(ret,-1)>0 and move(ret,-2)>0), 1, 0),
        condUp3D = iif((move(ret,-1)>0 and move(ret,-2)>0 and move(ret,-3)>0), 1, 0),
        condDown1D = iif(move(ret,-1)<0, 1, 0),
        condDown2D = iif((move(ret,-1)<0 and move(ret,-2)<0), 1, 0),
        condDown3D = iif((move(ret,-1)<0 and move(ret,-2)<0 and move(ret,-3)<0), 1, 0)
        from signalDF context by factor, symbol
        order by symbol, tradeDate
     
    
    // pos
    update summaryStats set consUp1DNumPos = nullFill(msum(iif(condUp1D == 1 and value == 1, 1, 0), callBackDays)-iif(condUp1D == 1 and value == 1, 1, 0),0) context by factor, symbol
    update summaryStats set consUp1DRatePos = nullFill(consUp1DNumPos\posNum,0.0)
    update summaryStats set consUp2DNumPos = nullFill(msum(iif(condUp2D == 1 and value == 1, 1, 0), callBackDays)-msum(iif(condUp2D == 1 and value == 1, 1, 0), 2),0) context by factor, symbol
    update summaryStats set consUp2DRatePos = nullFill(consUp2DNumPos\posNum,0.0)
    update summaryStats set consUp3DNumPos = nullFill(msum(iif(condUp3D == 1 and value == 1, 1, 0), callBackDays)-msum(iif(condUp3D == 1 and value == 1, 1, 0), 3),0) context by factor, symbol
    update summaryStats set consUp3DRatePos = nullFill(consUp3DNumPos\posNum,0.0)
    
    update summaryStats set consDown1DNumPos = nullFill(msum(iif(condDown1D == 1 and value == 1, 1, 0), callBackDays)-iif(condDown1D == 1 and value == 1, 1, 0),0) context by factor, symbol
    update summaryStats set consDown1DRatePos = nullFill(consDown1DNumPos\posNum,0.0)
    update summaryStats set consDown2DNumPos = nullFill(msum(iif(condDown2D == 1 and value == 1, 1, 0), callBackDays)-msum(iif(condDown2D == 1 and value == 1, 1, 0), 2),0) context by factor, symbol
    update summaryStats set consDown2DRatePos = nullFill(consDown2DNumPos\posNum,0.0)
    update summaryStats set consDown3DNumPos = nullFill(msum(iif(condDown3D == 1 and value == 1, 1, 0), callBackDays)-msum(iif(condDown3D == 1 and value == 1, 1, 0), 3),0) context by factor, symbol
    update summaryStats set consDown3DRatePos = nullFill(consDown3DNumPos\posNum,0.0)
    
    // neg
    update summaryStats set consUp1DNumNeg = nullFill(msum(iif(condUp1D == 1 and value == -1, 1, 0), callBackDays)-iif(condUp1D == 1 and value == -1, 1, 0),0) context by factor, symbol
    update summaryStats set consUp1DRateNeg = nullFill(consUp1DNumNeg\negNum,0.0)
    update summaryStats set consUp2DNumNeg = nullFill(msum(iif(condUp2D == 1 and value == -1, 1, 0), callBackDays)-msum(iif(condUp2D == 1 and value == -1, 1, 0), 2),0) context by factor, symbol
    update summaryStats set consUp2DRateNeg = nullFill(consUp2DNumNeg\negNum,0.0)
    update summaryStats set consUp3DNumNeg = nullFill(msum(iif(condUp3D == 1 and value == -1, 1, 0), callBackDays)-msum(iif(condUp3D == 1 and value == -1, 1, 0), 3),0) context by factor, symbol
    update summaryStats set consUp3DRateNeg = nullFill(consUp3DNumNeg\negNum,0.0)
    
    update summaryStats set consDown1DNumNeg = nullFill(msum(iif(condDown1D == 1 and value == -1, 1, 0), callBackDays)-iif(condDown1D == 1 and value == -1, 1, 0),0) context by factor, symbol
    update summaryStats set consDown1DRateNeg = nullFill(consDown1DNumNeg\negNum,0.0)
    update summaryStats set consDown2DNumNeg = nullFill(msum(iif(condDown2D == 1 and value == -1, 1, 0), callBackDays)-msum(iif(condDown2D == 1 and value == -1, 1, 0), 2),0) context by factor, symbol
    update summaryStats set consDown2DRateNeg = nullFill(consDown2DNumNeg\negNum,0.0)
    update summaryStats set consDown3DNumNeg = nullFill(msum(iif(condDown3D == 1 and value == -1, 1, 0), callBackDays)-msum(iif(condDown3D == 1 and value == -1, 1, 0), 3),0) context by factor, symbol
    update summaryStats set consDown3DRateNeg = nullFill(consDown3DNumNeg\negNum,0.0)
    
    // clear draftCols
    dropColumns!(summaryStats, `condUp2D`condUp3
        """)