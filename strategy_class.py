
from FinMind.data import DataLoader
import pandas as pd
import os
from pathlib import Path

global_path = Path(__file__).parent
os.chdir(global_path)

from get_price_data import main as get_price_data
from get_other_data import main as get_feature_data

# price必須要有date(datetime)、trade_price(float)、div(float)欄位
# 以及其他要計算indicator要用的欄位
price = get_price_data('2018-06-01', '2023-06-30')
# feature 必須要有date(datetime)以及其他indicator欄位
feature = get_feature_data('2018-06-01', '2023-06-30')

class futures_Strategy:
    def __init__(self) -> None: 
        self.price_data = price
        self.feature_data = feature
        self.signal_list = []
        
        self.original_cash = 1200000
        self.all_fee = 1
        self.point_value = 200
        self.margin = 500000
        self.hold_data = {
                'date':'',
                'signal':0,
                'price':0,
                'adjust_cost':0,
                'start':False,
                'max':0}
        
    def calculate_indicator(self, paras):
        self.indicator = self.price_data[['date']].copy()
        self.paras = paras
        #建立所有指標在self.indicator上，使用paras dict裡面的參數
        
        
        self.indicator = self.indicator.dropna()
        
    #要套用到indicator df的函數
    def set_signal(self, row):
        pass
        #取出self.paras的必要參數
        
        #根據hold_data內的資訊，建立策略機制
        
    
    def strategy_signal(self):
        #平倉時再紀錄 #再決定訊號函數內，平倉時新增資料
        #必備欄位in_date、out_date、signal、volume、cost、cover
        self.record = pd.DataFrame(columns=['in_date', 'out_date', 'signal', 'volume', 'cost', 'cover'])
        
        #每天都要記錄 #把indicator套入決定訊號函數 
        #必備欄位date、signal、PL、volume、total_cash
        self.signal = self.indicator.apply(self.set_signal, axis=1)
        
    
    def KPI(self):
        if self.record.shape[0] == 0:
            output_KPI = {
                        'cum_return':-999,
                        'avg_return':-999,
                        'MDD':999,
                        'std_return':999,
                        'profit_factor':-999,
                        'win_lose_ratio':-999,
                        'expectation':-999,
                        'sharpe':-999,
                        'trade_times':0,
                        'win_rate':-999,
                        'max_continuous_wins':-999,
                        'max_continuous_loses':999,
                        }
        
            return output_KPI

        #(尚未確定)調整價格(額外費用)
        
        #計算return(報酬率)、return_cash(每筆獲利金額)欄位，加到record上面
        cost_per_lot = int(self.margin / self.point_value)
        self.record['return'] = ((self.record['cover'] - self.record['cost'])*self.record['signal']) / cost_per_lot
        self.record['return_cash'] = self.record['return'] * self.record['volume'] * cost_per_lot
        
        
        #計算平均獲利百分比、獲利標準差與夏普比率
        avg_return = round(self.record['return'].mean(), 4)
        std_return = round(self.record['return'].std(), 4)
        sharpe = round((avg_return - 0.01565) / std_return, 4)
        
        #計算交易次數與勝率
        trade_times = len(self.record)
        win_rate = round((self.record['return']).sum() / trade_times, 2)
        
        #計算累積獲利、MDD
        cum_return = self.signal.iloc[-1]['total_cash'] / self.original_cash
        mdd_df = pd.DataFrame()
        mdd_df['total_cash'] = self.signal['total_cash']
        mdd_df['historic_max'] = mdd_df['total_cash'].cummax()
        mdd_df['dropdown'] = round((mdd_df['total_cash'] - mdd_df['historic_max']) / mdd_df['historic_max'], 4)
        MDD = mdd_df['dropdown'].min()
        
        #計算獲利因子、賺賠比、期望值
        win_data = self.record[self.record['return'] > 0]
        lose_data = self.record[self.record['return'] <= 0]
        
        if len(lose_data) > 0:
            profit_factor = win_data['return_cash'].sum() / lose_data['return_cash'].sum()
            profit_factor = round(abs(profit_factor), 2)
            
            win_lose_ratio = win_data['return_cash'].mean() / lose_data['return_cash'].mean()
            win_lose_ratio = round(abs(win_lose_ratio), 2)
        else:
            profit_factor = '勝率100%'
            win_lose_ratio = '勝率100%'
        
        expectation = win_data['return_cash'].mean() * win_rate + lose_data['return_cash'].mean() * (1-win_rate)
        
        #計算最大連續獲利/虧損次數
        max_continuous_wins = 0
        max_continuous_loses = 0
        times = 0
        
        for p in self.record['return_cash']:
            if p > 0 and times >= 0:
                times += 1
            elif p <= 0 and times <= 0:
                times -= 1
            elif p > 0 and times < 0:
                times = 1
            elif p < 0 and times > 0:
                times = -1
                
            if times > max_continuous_wins:
                max_continuous_wins = times
            elif times < max_continuous_loses:
                max_continuous_loses = times
                
        max_continuous_loses = abs(max_continuous_loses)
        
        output_KPI = {
        'cum_return':cum_return,
        'avg_return':avg_return,
        'MDD':MDD,
        'std_return':std_return,
        'profit_factor':profit_factor,
        'win_lose_ratio':win_lose_ratio,
        'expectation':expectation,
        'sharpe':sharpe,
        'trade_times':trade_times,
        'win_rate':win_rate,
        'max_continuous_wins':max_continuous_wins,
        'max_continuous_loses':max_continuous_loses,
        }
    
        return output_KPI
    
    def set_hold_data(self, ):
    
    def Recording(self, in_date, out_date, signal, volume, cost, cover):
        if self.record.empty:
            index_count = 0
        else:
            index_count = self.record.index[-1] + 1
        
        self.record.at[index_count, 'in_date'] = in_date
        self.record.at[index_count, 'out_date'] = out_date
        self.record.at[index_count, 'signal'] = signal
        self.record.at[index_count, 'volume'] = volume
        self.record.at[index_count, 'cost'] = cost
        self.record.at[index_count, 'cover'] = cover