import numpy as np
import pandas as pd
from conf import engine
from InsertOrUpdate import InsertOrUpdate
from datetime import datetime

def focusOn(target, period):
    # period_list = ['w', 'd']
    # period_list = ['d']
    stock_list_sql = f'select \"code\", \"code_name\" from stock_list where \"type\"=\'stock\' order by \"code\" asc'
    stock_list = pd.read_sql(stock_list_sql, engine)
    for index, row in stock_list.iterrows():
        symbol = row['code']
        code_name = row['code_name']
        # for period in period_list:
        k_sql = (f'select \"date\", \"code\", \"open\", \"close\", \"high\", \"low\" from stock_{period} '
                 f'where \"code\"=\'{symbol}\' and \"date\" = \'{target}\' ')
        target_k = pd.read_sql(k_sql, engine)
        if target_k.shape[0] == 1:
            target_k = target_k.iloc[0]
        else:
            continue
        sql = f'select * from moer_cl_{period} where \"Ticker\"=\'{symbol}\' and \"endDate\"<\'{target}\' order by \"startDate\" desc limit 10'
        cl_lines = pd.read_sql(sql, engine)
        if cl_lines.shape[0] < 4:
            continue
        cl_lines = cl_lines.iloc[::-1].reset_index(drop=True)
        empty_rows = pd.DataFrame([[None] * cl_lines.shape[1]] * 3, columns=cl_lines.columns)
        # 追加
        cl_lines = pd.concat([cl_lines, empty_rows], ignore_index=True)
        three_lines = cl_lines.iloc[0:3]
        # 重合时间区间
        overlap_start = three_lines['startDate'].min()
        overlap_end = three_lines['endDate'].max()
        # 重合价格区间
        overlap_price_low = three_lines[['startPrice', 'endPrice']].min(axis=1).max()
        overlap_price_high = three_lines[['startPrice', 'endPrice']].max(axis=1).min()

        cur = 3
        last_zs = {
            'starttime': cl_lines.iloc[0]['startDate'],
            'endtime': cl_lines.iloc[2]['endDate'],
            'high': overlap_price_high,
            'low': overlap_price_low
        }
        while cur < cl_lines.shape[0] - 3:
            row = cl_lines.iloc[cur]
            # print(cur)
            # 只要这线段在重叠区间时间内价格都高于overlap_price_high，标记三买
            # 这里用简单判定：整条线段价格高于那个重合区间的高点即可
            seg_min_price = min(row['startPrice'], row['endPrice'])
            seg_max_price = max(row['startPrice'], row['endPrice'])
            if seg_min_price > overlap_price_high or seg_max_price < overlap_price_low:
                three_lines = cl_lines.iloc[cur: cur + 3]
                # 重合时间区间
                overlap_start = three_lines['startDate'].min()
                overlap_end = three_lines['endDate'].max()
                # 重合价格区间
                overlap_price_low = three_lines[['startPrice', 'endPrice']].min(axis=1).max()
                overlap_price_high = three_lines[['startPrice', 'endPrice']].max(axis=1).min()
                last_zs = {
                    'starttime': three_lines.iloc[0]['startDate'],
                    'endtime': three_lines.iloc[2]['endDate'],
                    'high': overlap_price_high,
                    'low': overlap_price_low
                }
                # print(row)
            elif not pd.isna(row['endDate']):
                last_zs['endtime'] = row['endDate']
            cur = cur + 1
        if (last_zs['high'] < target_k['low']
                and (last_zs['high'] * 1.4) > target_k['low']
                and cl_lines.iloc[-4]['Line_Type'] == 'down'
                and (cl_lines.iloc[-4]['endPrice'] > last_zs['high']
                     or cl_lines.iloc[-4]['endPrice'] < last_zs['low'])):
            data = {'code': symbol,
                    'starttime': last_zs['starttime'],
                    'endtime': last_zs['endtime'],
                    'high': last_zs['high'],
                    'low': last_zs['low'],
                    'focus_time': target,
                    'focus_price': target_k['low'],
                    'focus_type': 'Third_Buy_' + period,
                    'period': period
                    }
            df_result = pd.DataFrame([data])
            cc = [
                {'name': 'code', 'cType': 'string'},
                {'name': 'starttime', 'cType': 'date'},
                {'name': 'endtime', 'cType': 'date'},
                {'name': 'high', 'cType': 'float'},
                {'name': 'low', 'cType': 'float'},
                {'name': 'focus_time', 'cType': 'date'},
                {'name': 'focus_price', 'cType': 'float'},
                {'name': 'focus_type', 'cType': 'string'},
                {'name': 'period', 'cType': 'string'}
            ]
            df_result['starttime'] = df_result['starttime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df_result['endtime'] = df_result['endtime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            iu = InsertOrUpdate(engine, f'focus_on', ['code', 'focus_time'], cc, df_result)
            iu.insert_or_update()
            print(f'{symbol}_{period} import successful')

if __name__ == '__main__':
    # nw = datetime.now().strftime('%Y-%m-%d')
    # nw = '2025-05-16'
    # nw = '2024-11-27'
    nw_list = [
        '2025-05-20',
        '2025-05-19',
        '2025-05-16',
        '2025-05-15',
        '2025-05-14',
        '2025-05-13',
        '2025-05-12',
    ]
    for nw in nw_list:
        # nw = '2025-04-25'
        # period_list = ['w', 'd']
        period_list = ['d', '30']
        for period in period_list:
            focusOn(nw, period)






