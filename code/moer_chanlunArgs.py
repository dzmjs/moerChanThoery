import pandas as pd
from conf import engine
from moer_chanlun import MooreChanLun
from InsertOrUpdate import InsertOrUpdate

def moer_analy():
    # symbol = 'NVDA'
    # period_list = ['1wk', '1d']
    # period_list = ['1d', '4h', '30m', '5m']
    period_list = ['1wk', '1d', '4h', '30m', '5m']
    stock_list_sql = f'select \"Symbol\" from stock_list where \"CodeType\" = \'stock\' order by \"Symbol\" asc'
    stock_list = pd.read_sql(stock_list_sql, engine)
    for index, row in stock_list.iterrows():
        symbol = row['Symbol']
        # symbol = "A"
        for period in period_list:
            sql = f'select * from stock_{period} where \"Ticker\" = \'{symbol}\' order by \"Datetime\" asc'
            df = pd.read_sql(sql, engine)
            df.rename(columns={'High': 'high', 'Low': 'low', 'Open': 'open', 'Close': 'close', 'Datetime': 'date'},
                      inplace=True)
            # 将JSON数据转换为Pandas DataFrame
            # data = pd.DataFrame(data_js)
            data = df
            if data.shape[0] < 3:
                continue

            # 创建摩尔缠论对象并分析数据
            mct = MooreChanLun(data, symbol, period)
            mct.find_begin_point()
            mct.draw_lines()
            print(mct.line_segrement)
            if len(mct.line_segrement) == 0:
                continue
            lins_to_database = []
            for i in mct.line_segrement:
                # 避免延申导致没有起点
                try:
                    start = i['start']
                except Exception:
                    continue
                start = i['start']
                end = i['final_end']
                line_type = i['line_type']
                if line_type == 'down':
                    startItem = df.iloc[start]
                    endItem = df.iloc[end]
                    startDate = startItem['date']
                    endDate = endItem['date']
                    startPrice = startItem['high']
                    endPrice = endItem['low']
                else:
                    startItem = df.iloc[start]
                    endItem = df.iloc[end]
                    startDate = startItem['date']
                    endDate = endItem['date']
                    startPrice = startItem['low']
                    endPrice = endItem['high']
                lins_to_database.append([symbol, line_type, startDate, endDate, startPrice, endPrice])
            print(lins_to_database)
            cl_df = pd.DataFrame(lins_to_database,
                                 columns=['Ticker', 'Line_Type', 'startDate', 'endDate', 'startPrice', 'endPrice'])
            # cl_df.to_sql(f'moer_cl_{period}', engine, if_exists='append', index=False)
            cc = [
                {'name': 'Ticker', 'cType': 'string'},
                {'name': 'Line_Type', 'cType': 'string'},
                {'name': 'startDate', 'cType': 'date'},
                {'name': 'endDate', 'cType': 'date'},
                {'name': 'startPrice', 'cType': 'float'},
                {'name': 'endPrice', 'cType': 'float'},
                {'name': 'class_type', 'cType': 'string'}
            ]
            if cl_df.shape[0] == 0:
                continue
            cl_df['startDate'] = cl_df['startDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
            cl_df['endDate'] = cl_df['endDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
            cl_df['class_type'] = None
            iu = InsertOrUpdate(engine, f'moer_cl_{period}', ['Line_Type', 'Ticker', 'startDate'], cc, cl_df)
            iu.insert_or_update()
            print(f'{symbol}_{period} import successful')
if __name__ == '__main__':
    moer_analy()