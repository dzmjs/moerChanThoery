from downloadStockArgs import downloadStock
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
import datetime
from conf import engine
import time
from sqlalchemy import text
from mutilDownload import downloadManager

def mutilDownloadMarketData(interval_list=['1wk', '1d', '4h', '30m', '5m'], period='5d'):
    # period = ['1wk', '1d', '4h', '30m', '5m']

    for p in interval_list:
        downloadManager(interval=p, period=period)

def deleteNan():
    stock_list = ['1wk', '1d', '4h', '30m', '5m']
    with engine.connect() as cn:
        for stock in stock_list:
            sql = f'DELETE FROM stock_{stock} WHERE \"Close\" IS NULL'
            delete_statement = text(sql)
            cn.execute(delete_statement)
            cn.commit()

def downloadMarketData():
    # engine = create_engine('postgresql://postgres:dzm@localhost:5432/stock')
    # pattern = 'N%'
    # sql = 'SELECT * FROM stock_list WHERE "Symbol" LIKE %(pattern)s'
    # df = pd.read_sql(sql, engine, params={'pattern': pattern})
    #==================================================================
    df = pd.read_sql("SELECT * FROM stock_list where \"CodeType\" = 'stock' order by \"Symbol\" asc", engine)
    #=====================================================================
    # sql = 'select * from stock_list sl order by sl."Symbol" asc limit 4000 offset 4475' ;
    # df = pd.read_sql(sql, engine)
    # print(df.head())

    # 查询部分数据
    # df2 = pd.read_sql("SELECT li.* FROM stock_list li WHERE li.\"Symbol\"='AAPL'", engine)
    # print(df2)
    begin = datetime.datetime.now()
    print(begin)
    # period = ['4h']
    # period = ['1d', '4h', '30m', '5m']
    period = ['1wk', '1d', '4h', '30m', '5m']
    for p in period:
        for idx, row in df.iterrows():
            # print(row['Symbol'], row['Name'], row['Market Cap'])
            downloadStock(row['Symbol'], p, "5d")
            time.sleep(2)

        end = datetime.datetime.now()
        print(end, begin)
if __name__ == '__main__':
    deleteNan()
    downloadMarketData()