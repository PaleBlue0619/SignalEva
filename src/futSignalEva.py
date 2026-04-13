import os,sys,json,json5
import dolphindb as ddb
from src.entity.SignalEva import SignalEva
# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

if __name__ == "__main__":
    session = ddb.session("localhost", 8848, "admin", "123456")
    with open(r"E:\Quant\SignalEva\src\cons\futSignalEva.json5", "r", encoding="utf-8") as f:
        sigCfg = json5.load(f)
    signalList = session.run("""
    exec factor from loadTable("dfs://daySignalFut", "info");
    """)
    SignalEva.run(session=session, cfg=sigCfg, signalList=signalList, dropDB=False, window=30, simpleEva=True)