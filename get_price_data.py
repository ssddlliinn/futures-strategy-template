
from FinMind.data import DataLoader
import pandas as pd
import os
from pathlib import Path

def get_open(row, original):
    date = row['date']
    miss_cont = row['contract_next']
    
    next_open = original[(original['date'] == date) & (original['contract_date'] == miss_cont)]['open']
    
    row['open_far'] = next_open.values[0]
    
    return row

def get_contract_diff(whole_day_data, original):
    contract = whole_day_data.copy()
    contract = contract[['date', 'contract_date_night', 'open_night']]
    contract.columns = ['date', 'contract', 'open_near']
    contract['contract_next'] = contract['contract'].shift(-1)

    contract = contract[contract['contract'] != contract['contract_next']]
    contract = contract.dropna()

    contract = contract.apply(get_open, axis=1, original=original)
    contract['diff'] = contract['open_near'] - contract['open_far']
    
    whole_day_data = pd.merge(whole_day_data, contract[['date', 'diff']], how='outer', on='date')
    whole_day_data['diff'] = whole_day_data['diff'].fillna(0)
    return whole_day_data


def main(start_date, end_date, store_file):
    #記得挑一個有交易的起始與終點日
    #從Finmind取得資料或讀csv
    if store_file in os.listdir(Path(__file__).parent):
        all_day = pd.read_csv(Path(__file__).parent / store_file)
        if all_day is not None:
            if (start_date == all_day.iloc[0].date) and (end_date == all_day.iloc[-1].date):
                all_day['date'] = pd.to_datetime(all_day['date'])
                return all_day

    api = DataLoader()
    df = api.taiwan_futures_daily(
        futures_id='TX',
        start_date=start_date,
        end_date=end_date
    )

    #去除期貨類別
    df = df.drop(['futures_id'], axis=1)

    #期貨資料中去除顯示近遠月價差的資料
    df = df[~(df['contract_date'].str.contains("/"))]

    #分出日夜盤資料並分開儲存，去除不必要資料
    night = df[df['trading_session'] == 'after_market']
    daily = df[df['trading_session'] == 'position']
    
    night = night.drop(['trading_session'], axis=1)
    daily = daily.drop(['trading_session'], axis=1)
    
    night['contract_date'] = night['contract_date'].astype(int)
    daily['contract_date'] = daily['contract_date'].astype(int)

    #近月合約資料
    maturity = daily.groupby('date')['contract_date'].apply(lambda x: x.unique()[0])
    maturity = pd.DataFrame(maturity)
    maturity = maturity.reset_index()
        
    #####################第一步處理，將資料變成可用資料
    #取得全契約未平倉數
    daily_oi = daily.groupby('date')[['open_interest']].sum()
    daily_oi = daily_oi.rename(columns={'open_interest':'all_oi'})
    daily_oi = daily_oi.reset_index()
    
    #####################進階處理，將資料合併
    #合併近月與日資料，取得近月資料
    daily_near = pd.merge(daily, maturity, how='inner', on=['date', 'contract_date'])
    night_near = pd.merge(night, maturity, how='inner', on=['date', 'contract_date'])
    
    #合併日夜資料，並以時間順序往列方向排列
    all_day = pd.merge(night_near, daily_near, 
                        how='outer', 
                        on='date', 
                        suffixes=('_night', '_day'))
    
    #刪除不必要之欄位
    for i in ['max', 'min', 'spread', 'settlement', 'open_interest_night', 'maturity_night']:
        remove_col = [col for col in all_day.columns if i in col]
        if remove_col != []:
            all_day = all_day.drop(remove_col, axis=1)
    
    ##################去除資料中的空值或錯誤
    #合併全契約未平倉資料
    all_day = pd.merge(all_day, daily_oi, how='outer', on='date')
    
    #特定欄位重新命名
    all_day = all_day.rename(columns={'open_interest_day':'near_oi'})

    #空值問題
    if all_day['open_night'].isnull().sum() != 0:
        all_day['contract_date_night'] = all_day['contract_date_night'].ffill()
        nulls = all_day['volume_night'].isnull()
        
        all_day.loc[nulls, 'volume_night'] = 0
        all_day.loc[nulls, 'close_night'] = all_day.loc[nulls, 'open_day']
        all_day.loc[nulls, 'open_night'] = all_day['close_day'].shift(1).loc[nulls]
    
    #加上交易點價格
    all_day['trade_price'] = all_day['open_night'].shift(-1)
    cond = all_day['trade_price'].isnull()
    all_day.loc[cond, 'trade_price'] = all_day.loc[cond, 'close_day']
    
    #加上轉約價差
    if len(maturity['contract_date'].unique()) == 1:
        pass
    else:
        all_day = get_contract_diff(all_day, original=night)
        
    all_day.to_csv(Path(__file__).parent / store_file, index=False)
    all_day['date'] = pd.to_datetime(all_day['date'])

    return all_day

if __name__ == '__main__':
    data = main('2018-06-01','2023-06-30')
    print(data.dtypes)
    