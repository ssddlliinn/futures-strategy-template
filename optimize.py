from strategy_class import Strategy

obj = Strategy({'default':1})
print(obj.price_data.head(5))
print(obj.price_data.dtypes)