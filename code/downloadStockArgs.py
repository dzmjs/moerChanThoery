import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from InsertOrUpdate import InsertOrUpdate

# 替换为你的PostgreSQL连结
# 例如: 'postgresql://用户名:密码@主机:端口/数据库名'
engine = create_engine('postgresql://postgres:dzm@localhost:5432/stock')

def downloadStock(stockCode="AAPL", interv="5m", period="1d"):
    # 下载AAPL 5分钟线数据
    df = yf.download(stockCode, interval=interv, period=period)
    if df.columns.nlevels > 1:
        df.columns = df.columns.droplevel(1)
    df.reset_index(inplace=True)
    df['Ticker'] = stockCode  # 添加ticker列

    # 写入数据库，表名叫 stock_5min
    # df.to_sql(f'stock_{interv}', engine, if_exists='append', index=False)
    cc = [
        {'name': 'Datetime', 'cType': 'datetime'},
        {'name': 'Close', 'cType': 'float'},
        {'name': 'High', 'cType': 'float'},
        {'name': 'Low', 'cType': 'float'},
        {'name': 'Open', 'cType': 'float'},
        {'name': 'Volume', 'cType': 'int'},
        {'name': 'Ticker', 'cType': 'string'},
    ]
    flag = "Datetime"
    if flag not in df.columns:
        flag = "Date"
    df['Datetime'] = df[flag].dt.strftime('%Y-%m-%d %H:%M:%S')
    iu = InsertOrUpdate(engine, f'stock_{interv}', ['Datetime', 'Ticker'], cc, df)
    iu.insert_or_update()

    print(f'{stockCode}-{interv}-数据写入完成！')