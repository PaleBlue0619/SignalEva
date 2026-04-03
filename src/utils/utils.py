import pandas as pd
import dolphindb as ddb
from typing import List

def split_list(l: List, k: int = 5) -> List[List]:
    """将列表进行切片, 长度不满的直接输出, 最终输出嵌套列表"""
    return [l[i:i + k] for i in range(0, len(l), k)]

def get_splitTradeTime(session: ddb.session,
                       startDate: pd.Timestamp,
                       endDate: pd.Timestamp,
                       window: int = 20) -> List[List[pd.Timestamp]]:
    startDate = pd.Timestamp(startDate).strftime("%Y.%m.%d")
    endDate = pd.Timestamp(endDate).strftime("%Y.%m.%d")
    dateList: List[pd.Timestamp] = session.run(f"""
    getMarketCalendar("XSHG",{startDate},{endDate})
    """)
    return split_list(l=dateList, k=window)