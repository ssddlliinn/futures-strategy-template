# output a dataframe with date as datetime object and others with correct timing

from FinMind.data import DataLoader
import pandas as pd
import os
from pathlib import Path
from datetime import datetime

def main(start_date, end_date, store_file):
    #記得挑一個有交易的起始與終點日
    #從Finmind取得資料或讀csv
    data_path = Path(__file__).parent.parent / 'other_data'
    if store_file in os.listdir(data_path):
        data = pd.read_csv(data_path / store_file)
        if data is not None:
            data['date'] = pd.to_datetime(data['date'])
            data = data.set_index(['date'])
            return data[start_date:end_date]
    
    #TODO:從finmind讀取資料並作完整處理，留下必要資料(必須要有data column in object format)
    
    
    
    
    data.to_csv(data_path / store_file, index=False)
    data['date'] = pd.to_datetime(data['date'])
    data = data.set_index(['date'])
    return data

if __name__ == '__main__':
    data = main('2018-06-05', '2023-06-30', 'foreign_OI.csv')
    print(data.dtypes)
    print(data.head(10))
    print(data.tail(10))