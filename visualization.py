
import os
import sys
from pathlib import Path

g_path = Path(__file__).parent
if str(g_path) not in sys.path:
    sys.path.append(str(g_path))
    

from strategy_class import futures_Strategy, get_all_data
import pandas as pd
from datetime import datetime

price_data, feature_data = get_all_data('2018-06-05',
                                        '2023-06-30',
                                        'price_data.csv',
                                        'other_data.csv')
train_trade = futures_Strategy(price_data, feature_data)

today = datetime.now().strftime("%Y-%m-%d")
price_data_test, feature_data_test = get_all_data('2023-07-03',
                                                    today,
                                                    'price_data_test.csv',
                                                    'other_data_test.csv')
test_trade = futures_Strategy(price_data_test, feature_data_test)

#TODO:
xi = []
para = {}

for i in [train_trade, test_trade]:
    i.calculate_indicator(para)
    i.strategy_signal()
    
    
    i.signal.plot(kind='line', x='date', y='total_cash')
    print('finished')