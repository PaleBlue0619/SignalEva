import pandas as pd
import dolphindb as ddb
from typing import Dict, List

class Stats:
    def __init__(self, session: ddb.session):
        self.session = session

