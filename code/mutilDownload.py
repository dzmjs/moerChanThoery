import yfinance as yf
import pandas as pd
import time
from InsertOrUpdate import InsertOrUpdate
from conf import engine

pd.options.mode.chained_assignment = None
def mutilDownload(tickers, interval='1d', period='2d', sleep=0):
    cc = [
        {'name': 'Datetime', 'cType': 'datetime'},
        {'name': 'Close', 'cType': 'float'},
        {'name': 'High', 'cType': 'float'},
        {'name': 'Low', 'cType': 'float'},
        {'name': 'Open', 'cType': 'float'},
        {'name': 'Volume', 'cType': 'int'},
        {'name': 'Ticker', 'cType': 'string'},
    ]

    # 下载数据
    data = yf.download(tickers, period=period, interval=interval)
    # 分割成不同股票的数据
    stocks_data = {ticker: data.xs(ticker, level=1, axis=1) for ticker in tickers}

    # 显示数据
    for ticker_code, df in stocks_data.items():
        df = df.dropna(subset=['Close'])
        df.reset_index(inplace=True)
        if df.shape[0] == 0:
            continue
        df['Ticker'] = ticker_code  # 添加ticker列

        # 写入数据库，表名叫 stock_5min
        # df.to_sql(f'stock_{interv}', engine, if_exists='append', index=False)
        flag = "Datetime"
        if flag not in df.columns:
            flag = "Date"
        if flag not in df.columns:
            flag = "index"
        df['Datetime'] = df[flag].dt.strftime('%Y-%m-%d %H:%M:%S')
        iu = InsertOrUpdate(engine, f'stock_{interval}', ['Datetime', 'Ticker'], cc, df)
        iu.insert_or_update()

        print(f'{ticker_code}-{interval}-数据写入完成！')

    # 显示数据
    # print(data)
    time.sleep(sleep)

def downloadManager(interval='1d', period='5d'):
    index = 0
    page_size = 1000
    sleep = 35
    while True:
        off_set = index * page_size
        sql = f'select sl.\"Symbol\" from stock_list sl where sl."CodeType" =\'stock\' order by sl.\"Symbol\" asc offset {off_set} limit {page_size}'
        symbol_list = pd.read_sql(sql, engine)
        index += 1
        if symbol_list.shape[0] == 0:
            return 0
        # 转换 'Ticker' 列为列表
        ticker_list = symbol_list['Symbol'].tolist()
        mutilDownload(ticker_list, interval=interval, period=period, sleep=sleep)

if __name__ == '__main__':
    # 股票代码列表
    tickers = ["AACB", "AAPL", "MSFT", "GOOGL"]  # 你可以用你自己的股票代码列表
    # interval = '1d'
    period = '21d'
    # sleep = 2
    mutilDownload(tickers, interval='4h', period=period, sleep=0)
    # downloadManager(interval='4h', period=period)