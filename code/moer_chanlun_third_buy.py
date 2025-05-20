import pandas as pd
from conf import engine
from InsertOrUpdate import InsertOrUpdate

def moer_chanlun_third_buy():
    # period_list = ['1wk', '1d']
    # period_list = ['4h', '30m', '5m']
    period_list = ['1wk', '1d', '4h', '30m', '5m']
    stock_list_sql = f'select \"Symbol\" from stock_list where \"CodeType\" = \'stock\' order by \"Symbol\" asc'
    stock_list = pd.read_sql(stock_list_sql, engine)
    for index, row in stock_list.iterrows():
        symbol = row['Symbol']
        for period in period_list:
            # 假设你的数据已经读入了 DataFrame df
            # df = pd.read_csv('your_data.csv')  # 这里只是示例入口
            # symbol = 'AAPL'
            # period = '5m'
            sql = f'select * from moer_cl_{period} where \"Ticker\" = \'{symbol}\' order by \"startDate\" asc'
            df = pd.read_sql(sql, engine)
            if df.shape[0] < 2:
                continue
            empty_rows = pd.DataFrame([[None]*df.shape[1]]*3, columns=df.columns)
            # 追加
            df = pd.concat([df, empty_rows], ignore_index=True)

            cond_up = df['Line_Type'] == 'down'
            # 1. 找最高点
            df['min_price'] = df[['startPrice', 'endPrice']].max(axis=1)
            # df['min_price'] = df[['startPrice', 'endPrice']].min(axis=1)
            # 找到最低点所在的行
            max_idx = df[cond_up]['min_price'].idxmax()

            # 2. 取三条线段，最高点起始位置及其后两条
            three_lines = df.iloc[max_idx:max_idx+3]
            # 重合时间区间
            overlap_start = three_lines['startDate'].min()
            overlap_end = three_lines['endDate'].max()
            # 重合价格区间
            overlap_price_low = three_lines[['startPrice', 'endPrice']].min(axis=1).max()
            overlap_price_high = three_lines[['startPrice', 'endPrice']].max(axis=1).min()

            # 时间和价格区间合法性判定
            if overlap_start >= overlap_end or overlap_price_low >= overlap_price_high:
                print('三条线段无有效重合区间')
            else:
                # 3. 检查之后每条线段是否“完全高于”这个区间
                result = []
                cur = max_idx + 3
                # for idx, row in df.iloc[max_idx+3:].iterrows():
                while cur <= df.shape[0]-3:
                    row = df.iloc[cur]
                    # print(cur)
                    # 只要这线段在重叠区间时间内价格都高于overlap_price_high，标记三买
                    # 这里用简单判定：整条线段价格高于那个重合区间的高点即可
                    seg_min_price = min(row['startPrice'], row['endPrice'])
                    if seg_min_price > overlap_price_high:
                        result.append(cur)  # 记录为三买线段的索引
                        df.loc[cur, 'class_type'] = 'Third Buy'
                        three_lines = df.iloc[cur - 1:cur + 2]
                        # 重合时间区间
                        overlap_start = three_lines['startDate'].min()
                        overlap_end = three_lines['endDate'].max()
                        # 重合价格区间
                        overlap_price_low = three_lines[['startPrice', 'endPrice']].min(axis=1).max()
                        overlap_price_high = three_lines[['startPrice', 'endPrice']].max(axis=1).min()

                        # print(row)
                    seg_max_price = max(row['startPrice'], row['endPrice'])
                    if seg_max_price < overlap_price_low:
                        result.append(cur)  # 记录为三卖线段的索引
                        df.loc[cur, 'class_type'] = 'Third Sell'
                        three_lines = df.iloc[cur - 1:cur + 2]
                        # 重合时间区间
                        overlap_start = three_lines['startDate'].min()
                        overlap_end = three_lines['endDate'].max()
                        # 重合价格区间
                        overlap_price_low = three_lines[['startPrice', 'endPrice']].min(axis=1).max()
                        overlap_price_high = three_lines[['startPrice', 'endPrice']].max(axis=1).min()

                    cur = cur + 1

                # 打印结果
                # print("三买线段编号：", result)
                # print(df.loc[result])
                cc = [
                    {'name': 'Ticker', 'cType': 'string'},
                    {'name': 'Line_Type', 'cType': 'string'},
                    {'name': 'startDate', 'cType': 'date'},
                    # {'name': 'endDate', 'cType': 'date'},
                    # {'name': 'startPrice', 'cType': 'float'},
                    # {'name': 'endPrice', 'cType': 'float'},
                    {'name': 'class_type', 'cType': 'string'}
                ]
                if df.shape[0] < 4:
                    exit()
                df['startDate'] = df['startDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
                df['endDate'] = df['endDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
                iu = InsertOrUpdate(engine, f'moer_cl_{period}', ['Line_Type', 'Ticker', 'startDate'], cc, df[:-3])
                iu.insert_or_update()
                print(f'{symbol}_{period} import successful')
if __name__ == '__main__':
    moer_chanlun_third_buy()