
from FinMind.data import DataLoader
import pandas as pd
import os
from pathlib import Path

global_path = Path(__file__).parent
os.chdir(global_path)

from get_price_data import main as get_price_data
from get_other_data import main as get_feature_data

price = get_price_data('2018-06-01', '2023-06-30')
feature = get_feature_data('2018-06-01', '2023-06-30')

class Strategy:
    def __init__(self, paras) -> None:
        self.paras = paras
        
        self.price_data = price
        self.feature_data = feature
        
    def calculate_indicator(self):
        pass
    
    def strategy_signal(self):
        pass
    
    def KPI(self):
        pass