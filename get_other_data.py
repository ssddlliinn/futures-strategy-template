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
            dt_start = datetime.strptime(start_date, '%Y-%m-%d')
            dt_end = datetime.strptime(end_date, '%Y-%m-%d')
            data['date'] = pd.to_datetime(data['date'])
            # if (start_date == data.iloc[0].date) and (end_date == data.iloc[-1].date):
            #     return data
            if (dt_start >= data.iloc[0].date) and(dt_end <= data.iloc[-1].date):
                return_df = data[(data['date'] >= dt_start) & (data['date'] <= dt_end)]
                return return_df
    
    #TODO:從finmind讀取資料並作完整處理，留下必要資料
    
    
    
    
    data.to_csv(data_path / store_file, index=False)
    data['date'] = pd.to_datetime(data['date'])
    return data

if __name__ == '__main__':
    data = main('2018-06-05', '2023-06-30', 'foreign_OI.csv')
    print(data.dtypes)
    print(data.head(10))
    print(data.tail(10))