import time

import requests
import pandas as pd
import json
from InsertOrUpdate import InsertOrUpdate
from conf import engine
from datetime import datetime

def check_proxy(ticker, interv, ranget, proxies):
    # 请求的 URL
    url = f'https://query2.finance.yahoo.com/v8/finance/chart/{ticker}?range={ranget}&interval={interv}'

    headersP = {
        'User-Agent': 'PostmanRuntime/7.43.4',
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive"
    }
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "max-age=0",
        "cookie": "A1=d=AQABBM98G2gCEJeBPVbMXZaGFIfAWDE6-XAFEgEBAQHOHGglaNxL0iMA_eMCAA&S=AQAAAmeA19i2_OcaFapNzEHskeY; A3=d=AQABBM98G2gCEJeBPVbMXZaGFIfAWDE6-XAFEgEBAQHOHGglaNxL0iMA_eMCAA&S=AQAAAmeA19i2_OcaFapNzEHskeY; A1S=d=AQABBM98G2gCEJeBPVbMXZaGFIfAWDE6-XAFEgEBAQHOHGglaNxL0iMA_eMCAA&S=AQAAAmeA19i2_OcaFapNzEHskeY",
        "Priority": "u=0, i",
        "Sec-Ch-Ua": "\"Chromium\";v=\"136\", \"Google Chrome\";v=\"136\", \"Not.A/Brand\";v=\"99\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    }
    # 发送 GET 请求
    response = requests.get(url, headers=headersP, proxies=proxies, timeout=25)

    # 检查请求是否成功
    if response.status_code == 200:
        # 解析 JSON 响应
        data = response.json()
        # print(json.dumps(data, indent=4))  # 整齐地打印 JSON 数据

        # 访问特定的数据字段（如果需要）
        try:
            chart_data = data['chart']['result'][0]
            timestamps = chart_data['timestamp']
            indicators = chart_data['indicators']['quote'][0]
            indicators['Timestamp'] = timestamps
            df = pd.DataFrame(indicators)
            if interv.lower() == '1d':
                df['Datetime'] = pd.to_datetime(df['Timestamp'] - 30600, unit='s', utc=True)
            if interv.lower() == '4h':
                df['Datetime'] = pd.to_datetime(df['Timestamp'] + 18000, unit='s', utc=True)
            if interv.lower() == '30m':
                df['Datetime'] = pd.to_datetime(df['Timestamp'] + 18000, unit='s', utc=True)
            if interv.lower() == '5m':
                df['Datetime'] = pd.to_datetime(df['Timestamp'] + 18000, unit='s', utc=True)
            df['Datetime'] = df['Datetime'].dt.tz_convert('America/Chicago')
            df['Open'] = df['open']
            df['Close'] = df['close']
            df['High'] = df['high']
            df['Low'] = df['low']
            df['Volume'] = df['volume']

            # 打印时间戳和收盘价格
            # print("Timestamps:", timestamps)
            return df

        except KeyError as e:
            print(f"Key error: {e}")

    else:
        print(f"请求失败，状态码: {response.status_code}")

if __name__ == '__main__':

    # interv = "1d"
    ranget = '5d'
    # stockCode = '^IXIC'
    ip = "47.236.224.32"
    port = '8080'
    proxy = {
        "https": f"http://{ip}:{port}",
        "http": f"http://{ip}:{port}"
    }
    stock_list = pd.read_sql("SELECT * FROM stock_list where \"CodeType\" = 'stock' order by \"Symbol\" asc offset 3823", engine)
    # =====================================================================
    # sql = 'select * from stock_list sl order by sl."Symbol" asc limit 4000 offset 4475' ;
    # df = pd.read_sql(sql, engine)
    # print(df.head())

    # 查询部分数据
    # df2 = pd.read_sql("SELECT li.* FROM stock_list li WHERE li.\"Symbol\"='AAPL'", engine)
    # print(df2)
    begin = datetime.now()
    print(begin)
    # period = ['4h']
    # period = ['5m']
    period = ['4h', '30m', '5m']
    # period = ['1wk', '1d', '4h', '30m', '5m']
    for interv in period:
        for idx, row in stock_list.iterrows():
            # stockCode = 'AAPL'
            stockCode = row['Symbol']
            df = check_proxy(stockCode, interv, ranget, None)
            if df is None or df.shape[0] == 0:
                continue
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
            df = df[:-1]
            iu = InsertOrUpdate(engine, f'stock_{interv}', ['Datetime', 'Ticker'], cc, df)
            iu.insert_or_update()
            print(f'{stockCode}-{interv}-数据写入完成！')
            time.sleep(3)