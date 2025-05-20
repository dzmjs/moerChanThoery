import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from InsertOrUpdate import InsertOrUpdate
import os

# yf.set_config(proxy="http://127.0.0.1:7890")
# 设置HTTP和HTTPS代理
# ip = "109.123.238.230"
# port = 14602
# os.environ['HTTP_PROXY'] = f'http://{ip}:{port}'
# os.environ['HTTPS_PROXY'] = f'http://{ip}:{port}'

# 替换为你的PostgreSQL连结
# 例如: 'postgresql://用户名:密码@主机:端口/数据库名'
engine = create_engine('postgresql://postgres:dzm@localhost:5432/stock')
# stockCode = '^GSPC'
stockCode = '^IXIC'
interv = ['1wk', '1d', '4h', '30m', '5m']
period = '9d'
for inv in interv:
    # interv = '4h'
    # period = '720d'
    df = yf.download(stockCode, interval=inv, period=period, timeout=60)
    if df.columns.nlevels > 1:
        df.columns = df.columns.droplevel(1)
    df.reset_index(inplace=True)
    df['Ticker'] = stockCode  # 添加ticker列
    flag = "Datetime"
    if flag not in df.columns:
        flag = "Date"

    # 写入数据库，表名叫 stock_5min
    # df.to_sql(f'stock_{interv}', engine, if_exists='append', index=False)
    # tableName, key, columns, df
    cc = [
        {'name': 'Datetime', 'cType': 'datetime'},
        {'name': 'Close', 'cType': 'float'},
        {'name': 'High', 'cType': 'float'},
        {'name': 'Low', 'cType': 'float'},
        {'name': 'Open', 'cType': 'float'},
        {'name': 'Volume', 'cType': 'int'},
        {'name': 'Ticker', 'cType': 'string'},
        ]
    df['Datetime'] = df[flag].dt.strftime('%Y-%m-%d %H:%M:%S')
    iu = InsertOrUpdate(engine, f'stock_{inv}', ['Datetime', 'Ticker'], cc, df)
    iu.insert_or_update()
    print(f'{stockCode}-{inv}-数据写入完成！')