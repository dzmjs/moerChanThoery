import pandas as pd
from conf import engine
from InsertOrUpdate import InsertOrUpdate
from line_zhongshu import Line_zhongshu

def moer_chanlun_analyse_single(period='30', symbol='sh.600000'):
    sql = f'select * from moer_cl_{period} where \"Ticker\" = \'{symbol}\' order by \"startDate\" asc'
    df = pd.read_sql(sql, engine)
    if df.shape[0] < 2:
        return None
    empty_rows = pd.DataFrame([[None] * df.shape[1]] * 3, columns=df.columns)
    # 追加
    df = pd.concat([df, empty_rows], ignore_index=True)

    cond_up = df['Line_Type'] == 'down'
    # 1. 找最高点
    df['min_price'] = df[['startPrice', 'endPrice']].max(axis=1)
    # df['min_price'] = df[['startPrice', 'endPrice']].min(axis=1)
    # 找到最高点所在的行
    max_idx = df[cond_up]['min_price'].idxmax()

    # 2. 取三条线段，最高点起始位置及其后两条
    three_lines = df.iloc[max_idx + 1:max_idx + 1 + 3]
    # 重合时间区间
    overlap_start = three_lines['startDate'].min()
    overlap_end = three_lines['endDate'].max()
    # 重合价格区间
    # overlap_price_high = three_lines[['startPrice', 'endPrice']].max(axis=1).min()
    overlap_price_high = three_lines[['close_begin', 'close_end']].max(axis=1).min()
    # overlap_price_low = three_lines[['startPrice', 'endPrice']].min(axis=1).max()
    overlap_price_low = three_lines[['close_begin', 'close_end']].min(axis=1).max()
    zs = Line_zhongshu(three_lines, max_idx + 1, overlap_price_high, overlap_price_low)

    # 时间和价格区间合法性判定
    if overlap_start >= overlap_end or overlap_price_low >= overlap_price_high:
        print('三条线段无有效重合区间')
    else:
        # 3. 检查之后每条线段是否“完全高于”这个区间
        result = []
        cur = max_idx + 3 + 1
        # for idx, row in df.iloc[max_idx+3:].iterrows():
        while cur <= df.shape[0] - 3:
            row = df.iloc[cur]
            # print(cur)
            # 只要这线段在重叠区间时间内价格都高于overlap_price_high，标记三买
            # 这里用简单判定：整条线段价格高于那个重合区间的高点即可
            seg_min_price = min(row['startPrice'], row['endPrice'])
            seg_max_price = max(row['startPrice'], row['endPrice'])
            if seg_min_price > overlap_price_high:
                result.append(cur)  # 记录为三买线段的索引
                df.loc[cur, 'class_type'] = 'Third Buy'
                three_lines = df.iloc[cur:cur + 3]
                # 重合时间区间
                overlap_start = three_lines['startDate'].min()
                overlap_end = three_lines['endDate'].max()
                # 重合价格区间
                overlap_price_low = three_lines[['close_begin', 'close_end']].min(axis=1).max()
                overlap_price_high = three_lines[['close_begin', 'close_end']].max(axis=1).min()
                # 新中枢
                if three_lines.shape[0] < 3:
                    break
                zs = Line_zhongshu(three_lines, cur, overlap_price_high, overlap_price_low)
                # print(row)

            elif seg_max_price < overlap_price_low:
                result.append(cur)  # 记录为三卖线段的索引
                df.loc[cur, 'class_type'] = 'Third Sell'
                three_lines = df.iloc[cur:cur + 3]
                # 重合时间区间
                overlap_start = three_lines['startDate'].min()
                overlap_end = three_lines['endDate'].max()
                # 重合价格区间
                overlap_price_low = three_lines[['close_begin', 'close_end']].min(axis=1).max()
                overlap_price_high = three_lines[['close_begin', 'close_end']].max(axis=1).min()
                # 新中枢
                if three_lines.shape[0] < 3:
                    break
                zs = Line_zhongshu(three_lines, cur, overlap_price_high, overlap_price_low)
            else:
                zs.add_to_inside_lines(df.iloc[[cur]])
            if zs.get_inside_lines_num() >= 9:  # 中枢内有9段次级别线段，升级了，需要拆解中枢
                zs.remove_last_line()
                # 拆除1|3|5段，以满足非同级别分解
                zs.same_level_and_cross_level_decomposition()
                first_index = zs.end_num + 1
                if zs.zs_type == 'down':
                    df.loc[first_index, 'class_type'] = 'First Buy'
                else:
                    df.loc[first_index, 'class_type'] = 'First Sell'
                cur = first_index + 1 + 1
                three_lines = df.iloc[cur - 1:cur + 2]
                # 重合时间区间
                overlap_start = three_lines['startDate'].min()
                overlap_end = three_lines['endDate'].max()
                # 重合价格区间
                overlap_price_low = three_lines[['close_begin', 'close_end']].min(axis=1).max()
                overlap_price_high = three_lines[['close_begin', 'close_end']].max(axis=1).min()
                # 新中枢
                if three_lines.shape[0] < 3:
                    break
                zs = Line_zhongshu(three_lines, cur - 1, overlap_price_high, overlap_price_low)
                cur = cur + 1  # 跳转至three_lines的最后一条次级别线段

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

def moer_chanlun_third_buy():
    # period_list = ['1wk', '1d']
    # period_list = ['4h', '30m', '5m']
    period_list = ['w', '5', '30']
    # period_list = ['d']
    stock_list_sql = f'select \"code\", \"type\" from stock_list order by \"code\" asc'
    stock_list = pd.read_sql(stock_list_sql, engine)
    for index, row in stock_list.iterrows():
        symbol = row['code']
        stock_type = row['type']
        for period in period_list:
            if stock_type == 'index' and (period == '30' or period == '5'):
                continue
            # 假设你的数据已经读入了 DataFrame df
            # df = pd.read_csv('your_data.csv')  # 这里只是示例入口
            # symbol = 'AAPL'
            # period = '5m'
            moer_chanlun_analyse_single(period, symbol)



if __name__ == '__main__':
    moer_chanlun_third_buy()
