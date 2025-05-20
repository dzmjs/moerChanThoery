from conf import engine
import pandas as pd

def backtest(focus_date, period, target_length):
    sql = f'select * from focus_on where \"period\" = \'{period}\' and \"focus_time\" = \'{focus_date}\' '
    df = pd.read_sql(sql, engine)
    df['up'] = None
    for _ind, row in df.iterrows():
        code = row['code']
        sql = f'select * from stock_{period} where \"date\" >= \'{focus_date}\' and \"code\" = \'{code}\' order by \"date\" asc limit {target_length}'
        stock_price = pd.read_sql(sql, engine)
        sell = stock_price['close'].max()
        buy = stock_price.iloc[0]['close']
        df.loc[_ind, 'up'] = (sell-buy)/buy
    print(df)


if __name__ == '__main__':
    focus_date = '2025-04-25'
    target_length = 15
    period = 'd'
    backtest(focus_date, period, target_length)