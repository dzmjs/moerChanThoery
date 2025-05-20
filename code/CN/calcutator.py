from conf import engine
import pandas as pd
from moer_chanlun import MooreChanLun
from InsertOrUpdate import InsertOrUpdate
from moer_chanlun_third_buy import moer_chanlun_third_buy

def calcutator_single_period(period, stock_length=100):
    sql = "select * from stock_list order by  \"code\" asc"
    df_list = pd.read_sql(sql, engine)
    for ind, row in df_list.iterrows():
        stock = row['code']
        # stock = 'sh.600459'
        stock_name = row['code_name']
        stock_type = row['type']
        calcutator_single(stock, period, stock_type, stock_length)

# 进行单只股票的摩尔缠论分析
def calcutator_single(stock_code, period, stock_type, stock_length=300):
    stock = stock_code
    # stock = 'sh.600459'
    if (period == '30' or period == '5') and stock_type == 'index':
        return None
    stock_table = f'stock_{period}'
    sql = f"select * from {stock_table} where code = '{stock}' order by date desc"
    if stock_length:
        sql += f" limit {stock_length}"
    df = pd.read_sql(sql, engine)
    df = df.iloc[::-1].reset_index(drop=True)
    data = df
    if data.shape[0] < 3:
        return None

    # 创建摩尔缠论对象并分析数据
    mct = MooreChanLun(data, stock, period)
    mct.find_begin_point()
    mct.draw_lines()
    # print(mct.line_segrement)
    if len(mct.line_segrement) == 0:
        return None
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
        lins_to_database.append([stock, line_type, startDate, endDate, startPrice, endPrice])
    # print(lins_to_database)
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
        return None
    # print(cl_df)
    cl_df['startDate'] = cl_df['startDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
    cl_df['endDate'] = cl_df['endDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
    cl_df['class_type'] = None
    iu = InsertOrUpdate(engine, f'moer_cl_{period}', ['Line_Type', 'Ticker', 'startDate'], cc, cl_df)
    iu.insert_or_update()
    print(f'缠论分析: {stock}_{period} import successful')
    return True

def calcutator():
    stock_type_list = ['w', 'd', '30', '5']
    # stock_type_list = ['w', 'd', '30']
    # stock_type_list = ['5']
    # stock_type_list = ['d']
    sql = "select * from stock_list order by  \"code\" asc"
    df_list = pd.read_sql(sql, engine)
    for ind, row in df_list.iterrows():
        stock = row['code']
        # stock = 'sh.600459'
        stock_name = row['code_name']
        stock_index = row['type']
        for stock_type in stock_type_list:
            if (stock_type == '30' or stock_type == '5') and stock_index == 'index':
                continue
            stock_table = f'stock_{stock_type}'
            sql = f"select * from {stock_table} where code = '{stock}' order by date asc"
            df = pd.read_sql(sql, engine)
            data = df
            if data.shape[0] < 3:
                continue

            # 创建摩尔缠论对象并分析数据
            mct = MooreChanLun(data, stock, stock_type)
            mct.find_begin_point()
            mct.draw_lines()
            # print(mct.line_segrement)
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
                lins_to_database.append([stock, line_type, startDate, endDate, startPrice, endPrice])
            # print(lins_to_database)
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
            # print(cl_df)
            cl_df['startDate'] = cl_df['startDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
            cl_df['endDate'] = cl_df['endDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
            cl_df['class_type'] = None
            iu = InsertOrUpdate(engine, f'moer_cl_{stock_type}', ['Line_Type', 'Ticker', 'startDate'], cc, cl_df)
            iu.insert_or_update()
            print(f'No.{ind} _{stock}_{stock_type} import successful')

if __name__ == '__main__':
    calcutator()
    moer_chanlun_third_buy()