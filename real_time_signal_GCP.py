# -*- coding: utf-8 -*-

import pandas as pd
import requests
from datetime import datetime as dt
from FinMind.data import DataLoader

check_today = False

hold_data = {
    'date':'',
    'signal':0,
    'price':0,
    'adjust_cost':0,
    'start':False,
    'min':0,
    'max':0}

# xi = [1700,-2700,-2100,1500,6,24,3800,-3700,0.05,0.09,0.8]
xi = [1600,-2700,-2100,1800,8,23,4000,-4000,0.05,0.09,0.8,1000]
# xi = [1600,-4000,-5200,3800,8,23,3800,-4000,0.05,0.09,0.016]
strategy_paras = {
    'cash':1200000,
    'increase_vol':xi[0],
    'decrease_vol':xi[1],
    'offset_de_vol':xi[2],
    'offset_in_vol':xi[3],
    'short_days':xi[4],
    'long_days':xi[5],
    'long_large_vol':xi[6],
    'short_large_vol':xi[7],
    'max_loss':xi[8],
    'start_profit':xi[9],
    'dropdown':xi[10],
    'dropdown_limit':xi[11]
    }

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
    return whole_day_data

def get_institution():
    global check_today
    today = dt.now().date()
    
    start_date = '2023-07-01'
    end_date = f'{today.year}-{today.month:0>2}-{today.day:0>2}'
        
    api = DataLoader()
    df = api.taiwan_futures_institutional_investors(
        data_id='TX',
        start_date=start_date,
        end_date=end_date
    )
    
    last_date = str(df['date'].iloc[-1])
    if (last_date != end_date) and check_today:
        # line_print("Today's data has not been updated yet. Please try later.")
        return None

    df = df.drop(['futures_id'], axis=1)

    local = df[df['institutional_investors'] != '外資']
    foreign = df[df['institutional_investors'] == '外資']

    local_sum = local.groupby(by=['date']).sum()
    foreign_sum = foreign.set_index(['date'])
    
    local_sum = local_sum.drop(['institutional_investors'], axis=1)
    foreign_sum = foreign_sum.drop(['institutional_investors'], axis=1)

    new_df = pd.merge(foreign_sum, local_sum, how='inner', on='date', suffixes=('_foreign', '_local'))
    new_df = new_df.reset_index()
    return new_df

def get_daily_info():
    today = dt.now().date()
    
    start_date = '2023-07-01'
    end_date = f'{today.year}-{today.month:0>2}-{today.day:0>2}'
    
    api = DataLoader()
    df = api.taiwan_futures_daily(
        futures_id='TX',
        start_date=start_date,
        end_date=end_date
    )

    df = df.drop(['futures_id'], axis=1)

    #期貨資料中去除顯示近遠月價差的資料
    df = df[~(df['contract_date'].str.contains("/"))]

    #分出日夜盤資料並分開儲存
    new_night = df[df['trading_session'] == 'after_market']
    new_daily = df[df['trading_session'] == 'position']
    
    new_night = new_night.drop(['trading_session'], axis=1)
    new_daily = new_daily.drop(['trading_session'], axis=1)
    
    new_night['contract_date'] = new_night['contract_date'].astype(int)
    new_daily['contract_date'] = new_daily['contract_date'].astype(int)
    return new_daily, new_night


def get_data():
    #################取得資料
    #期貨內外資資料
    institution = get_institution()
    if institution is None:
        return None
    #期貨資料
    daily, night = get_daily_info()

    #到期日與近月合約資料
    maturity = daily.groupby('date')['contract_date'].apply(lambda x: x.unique()[0])
    maturity = pd.DataFrame(maturity)
    maturity = maturity.reset_index()

    
    
        
    #####################第一步處理，將資料變成可用資料
    #取得全契約未平倉數
    daily_oi = daily.groupby('date')[['open_interest']].sum()
    daily_oi = daily_oi.rename(columns={'open_interest':'all_oi'})
    daily_oi = daily_oi.reset_index()
    
    #取未平倉資料
    insti_flat = institution[['date', 
                              'long_open_interest_balance_volume_foreign', 
                              'short_open_interest_balance_volume_foreign', 
                              'long_open_interest_balance_volume_local', 
                              'short_open_interest_balance_volume_local']]
    
    insti_flat.columns = ['date', 
                          'foreign_long_oi_vol',
                          'foreign_short_oi_vol',
                          'local_long_oi_vol',
                          'local_short_oi_vol']
    
    #####################進階處理，將資料合併
    #合併到期日與日資料，取得目標契約資料
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
    #合併全契約未平倉資料與法人未平倉資料
    all_day = pd.merge(all_day, daily_oi, how='outer', on='date')
    all_day = pd.merge(all_day, insti_flat, how='outer', on='date')

    #空值問題
    if all_day['open_night'].isnull().sum() != 0:
        all_day['contract_date_night'] = all_day['contract_date_night'].ffill()
        nulls = all_day['volume_night'].isnull()
        
        all_day.loc[nulls, 'volume_night'] = 0
        all_day.loc[nulls, 'close_night'] = all_day.loc[nulls, 'open_day']
        all_day.loc[nulls, 'open_night'] = all_day['close_day'].shift(1).loc[nulls]
    
    #加上交易點價格
    all_day['trade_price'] = all_day['open_night'].shift(-1)
    
    
    #加上轉約價差
    if len(maturity['contract_date'].unique()) == 1:
        pass
    else:
        all_day = get_contract_diff(all_day, original=night)
    
    return all_day

def renew(date, signal, price):
    global hold_data
    hold_data['date'] = date
    hold_data['signal'] = signal
    hold_data['price'] = price
    hold_data['adjust_cost'] = price
    hold_data['max'] = price
    hold_data['min'] = price

def set_signal_2(row):
    global hold_data, strategy_paras
    
    long_large_vol = strategy_paras['long_large_vol']
    short_large_vol = strategy_paras['short_large_vol']
    
    max_loss = strategy_paras['max_loss']
    
    start_profit = strategy_paras['start_profit']
    dropdown = strategy_paras['dropdown']
    dropdown_limit = strategy_paras['dropdown_limit']
    
    if hold_data['signal'] == 0:
        ####檢視是否符合策略一或二
        #策略二
        if row['net_change_MA'] >= long_large_vol:
            renew(row['date'], 2, row['trade_price'])
            row['signal2'] = 2
        elif row['net_change_MA'] <= short_large_vol:
            renew(row['date'], -2, row['trade_price'])
            row['signal2'] = -2
        else:
            row['signal2'] = 0
            
    #看二是否做多平倉
    elif hold_data['signal'] == 2:
        hold_data['adjust_cost'] -= row['diff']
        cost = hold_data['adjust_cost']
        # hold_data['diff'] += row['diff']
        trade_price = row['trade_price']
        hold_data['min'] = min(hold_data['min'], trade_price)
        hold_data['max'] = max(hold_data['max'], trade_price)
        
        cond1 = row['net_change_MA'] <= short_large_vol
        cond2 = trade_price < (cost * (1-max_loss))
        
        if not hold_data['start']:
            start_profit_point = cost*(1+start_profit)
            # start_profit_point = min(cost*(1+start_profit), cost+start_profit_limit)
            if trade_price > start_profit_point:
                hold_data['start'] = True
            cond3 = False
        else:
            max_profit = hold_data['max'] - cost
            max_dropdown = max(max_profit*dropdown, max_profit-dropdown_limit)
            cond3 = (trade_price-cost) < max_dropdown
        
        if any([cond1, cond2, cond3]):
            row['pl2'] = trade_price - cost
            row['MDD2'] = hold_data['min'] - cost
            row['comment2'] = (hold_data['max'] if cond3 else ('1' if cond1 else '2'))
            row['signal2'] = 0
            
            hold_data = {
                'date':'',
                'signal':0,
                'price':0,
                'adjust_cost':0,
                'start':False,
                'min':0,
                'max':0}

            
        else:
            row['signal2'] = hold_data['signal']
            row['pl2'] = trade_price - cost
            row['MDD2'] = hold_data['min'] - cost
            row['comment2'] = ''
        
        
        
    #看二是否做空平倉
    elif hold_data['signal'] == -2:
        hold_data['adjust_cost'] -= row['diff']
        cost = hold_data['adjust_cost']
        trade_price = row['trade_price']
        hold_data['min'] = min(hold_data['min'], trade_price)
        hold_data['max'] = max(hold_data['max'], trade_price)
        
        cond1 = row['net_change_MA'] >= long_large_vol
        cond2 = trade_price > (cost * (1+max_loss))
        
        if not hold_data['start']:
            start_profit_point = cost*(1-start_profit)
            if trade_price < start_profit_point:
                hold_data['start'] = True
            cond3 = False
        else:
            max_profit = cost - hold_data['min']
            max_dropdown = max(max_profit*dropdown, max_profit-dropdown_limit)
            cond3 = (cost-trade_price) < max_dropdown
            
        if any([cond1, cond2, cond3]):
            row['pl2'] = cost - trade_price
            row['MDD2'] = cost - hold_data['max']
            row['comment2'] = (hold_data['max'] if cond3 else ('1' if cond1 else '2'))
            row['signal2'] = 0
            
            hold_data = {
                'date':'',
                'signal':0,
                'price':0,
                'adjust_cost':0,
                'start':False,
                'min':0,
                'max':0}

            
            
        else:
            row['signal2'] = hold_data['signal']
            row['pl2'] = cost - trade_price
            row['MDD2'] = cost - hold_data['max']
            row['comment2'] = ''
    
    if 'pl2' in row.index:
        return row[['date', 'signal2', 'pl2', 'MDD2', 'comment2']]
    else:
        return row[['date', 'signal2']]

def set_signal_1(row):
    global hold_data, strategy_paras
    increase_vol = strategy_paras['increase_vol']
    decrease_vol = strategy_paras['decrease_vol']
    
    offset_de_vol = strategy_paras['offset_de_vol']
    offset_in_vol = strategy_paras['offset_in_vol']
    
    if hold_data['signal'] == 0:
        ####檢視是否符合策略一或二
        #策略二
        if (row['net_change_MA'] >= increase_vol) and (row['short_ma'] > row['long_ma']):
            renew(row['date'], 1, row['trade_price'])
            row['signal1'] = 1
        elif (row['net_change_MA'] <= decrease_vol) and (row['short_ma'] < row['long_ma']):
            renew(row['date'], -1, row['trade_price'])
            row['signal1'] = -1
        else:
            row['signal1'] = 0
            
    #看二是否做多平倉
    elif hold_data['signal'] == 1:
        hold_data['adjust_cost'] -= row['diff']
        cost = hold_data['adjust_cost']
        # hold_data['diff'] += row['diff']
        trade_price = row['trade_price']
        hold_data['min'] = min(hold_data['min'], trade_price)
        hold_data['max'] = max(hold_data['max'], trade_price)
        
        cond1 = row['short_ma'] < row['long_ma']
        cond2 = row['net_change_MA'] < offset_de_vol
        
        if any([cond1, cond2]):
            row['pl1'] = trade_price - cost
            row['MDD1'] = hold_data['min'] - cost
            row['signal1'] = 0
            
            hold_data = {
                'date':'',
                'signal':0,
                'price':0,
                'adjust_cost':0,
                'start':False,
                'min':0,
                'max':0}

            
        else:
            row['signal1'] = hold_data['signal']
            row['pl1'] = trade_price - cost
            row['MDD1'] = hold_data['min'] - cost
        
        
    #看二是否做空平倉
    elif hold_data['signal'] == -1:
        hold_data['adjust_cost'] -= row['diff']
        cost = hold_data['adjust_cost']
        # hold_data['diff'] += row['diff']
        trade_price = row['trade_price']
        hold_data['min'] = min(hold_data['min'], trade_price)
        hold_data['max'] = max(hold_data['max'], trade_price)
            
        cond1 = row['short_ma'] > row['long_ma']
        cond2 = row['net_change_MA'] > offset_in_vol
        
        if any([cond1, cond2]):
            row['pl1'] = cost - trade_price
            row['MDD1'] = cost - hold_data['max']
            row['signal1'] = 0
            
            hold_data = {
                'date':'',
                'signal':0,
                'price':0,
                'adjust_cost':0,
                'start':False,
                'min':0,
                'max':0}

            
            
        else:
            row['signal1'] = hold_data['signal']
            row['pl1'] = cost - trade_price
            row['MDD1'] = cost - hold_data['max']
    
    if 'pl1' in row.index:
        return row[['date', 'signal1', 'pl1', 'MDD1']]
    else:
        return row[['date', 'signal1']]

def get_status(prev, curr):
    if prev == 0 and curr > 0:
        status = '判斷外資開始做多'
        code = 0
    elif prev > 0 and curr > 0:
        status = '判斷外資持續做多'
        code = 1
    elif prev == 0 and curr < 0:
        status = '判斷外資開始做空'
        code = 0
    elif prev < 0 and curr < 0:
        status = '判斷外資持續做空'
        code = -1
    elif prev == 0 and curr == 0:
        status = '外資無明顯方向'
        code = 0
    elif prev > 0 and curr == 0:
        status = '判斷外資停止做多，應平倉'
        code = 0
    elif prev < 0 and curr == 0:
        status = '判斷外資停止做空，應平倉'
        code = 0
        
    return status, code


def line_print(message):
    url = 'https://notify-api.line.me/api/notify'
    token = 'xiDevD33i73VUwBlUp2IAfkKV7sZhs47PBeTE4OjH5v'
    headers = {
        'Authorization': 'Bearer ' + token    # 設定權杖
    }
    data = {
        'message':message     # 設定要發送的訊息
    }
    data = requests.post(url, headers=headers, data=data)

def main():
    global hold_data, strategy_paras
    
    data = get_data()
    if data is None:
        return None
    
    data = data.drop_duplicates(['date'])
    
    data['short_ma'] = data['close_day'].rolling(strategy_paras['short_days']).mean()
    data['long_ma'] = data['close_day'].rolling(strategy_paras['long_days']).mean()
    
    data['foreign_net'] = data['foreign_long_oi_vol'] - data['foreign_short_oi_vol']
    data['foreign_net_change'] = data['foreign_net'].diff()
    
    data['net_change_MA'] = data['foreign_net_change'].rolling(5).mean()
    
    data = data[data['long_ma'].notna()]
    data['diff'] = data['diff'].fillna(0)
    cond1 = data['trade_price'].isna()
    data.loc[cond1, 'trade_price'] = data[cond1]['close_day']
    
    strategy1 = data.apply(set_signal_1, axis=1)

    # line_print(f"""
    # 資料最後更新日：{strategy1['date'].iloc[-1]}
    # """)

    prev_signal = strategy1['signal1'].iloc[-2]
    curr_signal = strategy1['signal1'].iloc[-1]

    status, code = get_status(prev_signal, curr_signal)
    pl = (data['close_day'].iloc[-1] - hold_data["adjust_cost"]) * code

    # line_print(f"""
    # 策略1(未平倉量+均線策略)
    # 目前狀態：{status}
    # {(f'進場日期：{hold_data["date"]}') if code!=0 else ''}
    # {(f'調整後持有成本：{hold_data["adjust_cost"]}') if code!=0 else ''}
    # {(f'調整後當前獲利：{pl}') if code!=0 else ''}
    # """)

    hold_data = {
        'date':'',
        'signal':0,
        'price':0,
        'adjust_cost':0,
        'start':False,
        'min':0,
        'max':0}


    strategy2 = data.apply(set_signal_2, axis=1)

    prev_signal = strategy2['signal2'].iloc[-2]
    curr_signal = strategy2['signal2'].iloc[-1]

    status, code = get_status(prev_signal, curr_signal)
    pl = (data['close_day'].iloc[-1] - hold_data["adjust_cost"]) * code
    max_profit = max((hold_data['max']-hold_data['adjust_cost'])*code, 
                     (hold_data['min']-hold_data['adjust_cost'])*code)

    # line_print(f"""
    # 策略2(大額未平倉量策略)
    # 目前狀態：{status}
    # {(f'進場日期：{hold_data["date"]}') if code!=0 else ''}
    # {(f'調整後持有成本：{hold_data["adjust_cost"]}') if code!=0 else ''}
    # {(f'調整後當前獲利：{pl}') if code!=0 else ''}
    # {(f'調整後該交易歷史最大獲利：{max_profit}') if code!=0 else ''}
    # """)

    all_data = pd.merge(data, strategy1, how='outer', on='date')
    all_data = pd.merge(all_data, strategy2, how='outer', on='date')

    all_data['ddate'] = all_data['date']
    
    return all_data

if __name__ == '__main__':
    check_today = False
    all_data = main()
