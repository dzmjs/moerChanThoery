import pandas as pd
from InsertOrUpdate import InsertOrUpdate
import numpy as np
import json
from conf import engine

class zs_line:
    def __init__(self, index, data):
        self.begin = index
        self.high = min(data['high'][index], data['high'][index+1])
        self.low = max(data['low'][index], data['low'][index+1])
        self.cross = [index, index+1]
        self.cross_last = index+1
        self.count = 2
        self.checkCount = 0
class MooreChanLun:
    def __init__(self, data, symbol=None, period=None):
        self.data = data
        self.symbol = symbol
        self.period = period
        self.line_segrement = []
        self.rank_zs_segments = []
        self.begin_no = 1
        self.begin_type = 'down'
    def is_up_point(self, index):
        result = self.data['high'][index] >= self.data['high'][index-1] and self.data['high'][index] >= self.data['high'][index+1]
        return result
    def is_down_point(self, index):
        result = self.data['low'][index] <= self.data['low'][index-1] and self.data['low'][index] <= self.data['low'][index+1]
        return result
    def find_begin_point(self):
        max_row_index = self.data['high'].idxmax()
        min_row_index = self.data['low'].idxmin()
        max_row_index = min(max_row_index, min_row_index)
        if max_row_index == min_row_index:
            self.begin_type = 'up'
        else:
            self.begin_type = 'down'
        # if min_row_index > max_row_index:
        if max_row_index == 0:
            max_row_index = 1
        self.begin_no = max_row_index

    def updateHighLowValue(self, index, xd_obj):
        high_v = self.data['high'][index]
        low_v = self.data['low'][index]
        if xd_obj['zhuanzhe']['low'] > low_v:
            xd_obj['zhuanzhe']['low'] = low_v
        if xd_obj['zhuanzhe']['high'] < high_v:
            xd_obj['zhuanzhe']['high'] = high_v

    def verifyZsLine(self, begin, end, zs_line):
        num = 0
        for i in range(begin, end+1):
            if (self.data['high'][i] < zs_line.low or self.data['low'][i] > zs_line.high):
                pass #print("")
            else:
                num = num + 1
            if (self.data['low'][i] > self.data['high'][i-1] or self.data['high'][i] < self.data['low'][i-1]):
                num = num + 1
        zs_line.checkCount = num
        return num >= 5


    def verifyBetween(self, begin, end, zs_list, tp):
        checkResult = []
        for zs in zs_list:
            if zs.checkCount < 5:
                continue
            if tp == 'up':
                ev = self.data['high'][end]
                if ev < zs.low:
                    checkResult.append(False)
                else:
                    checkResult.append(True)
            if tp == 'down':
                ev = self.data['low'][end]
                if ev > zs.high:
                    checkResult.append(False)
                else:
                    checkResult.append(True)
        return True in checkResult
    def verify2kSingle(self, begin, end, zs_list, tp):
        checkResult = []
        for zs in zs_list:
            if zs.checkCount<5:
                continue
            checkTmp = False
            if tp == "up":
                if (self.data['high'][begin] >= self.data['low'][end]):
                    checkTmp = False
            if tp == "down":
                if (self.data['low'][begin] <= self.data['high'][end]):
                    checkTmp = False
            if tp == "up":
                bv1 = self.data['high'][begin - 1]
                bv  = self.data['high'][begin]
                bv2 = self.data['high'][begin + 1]
                ev1 = self.data['low'][end - 1]
                ev  = self.data['low'][end]
                ev2 = self.data['low'][end + 1]
                zs_high = zs.high
                zs_low = zs.low
                if ((bv < zs_low and bv1 < zs_low) or (bv < zs_low and bv2 < zs_low) or
                    (ev1 > zs_high and ev > zs_high) or (ev2 > zs_high and ev > zs_high)):
                    checkTmp = True
            if tp == "down":
                bv1 = self.data['low'][begin - 1]
                bv  = self.data['low'][begin]
                bv2 = self.data['low'][begin + 1]
                ev1 = self.data['high'][end - 1]
                ev  = self.data['high'][end]
                ev2 = self.data['high'][end + 1]
                zs_high = zs.high
                zs_low = zs.low
                if ((bv > zs_low and bv1 > zs_low) or (bv > zs_low and bv2 > zs_low) or
                    (ev1 < zs_high and ev < zs_high) or (ev2 < zs_high and ev < zs_high)):
                    checkTmp = True
            checkResult.append(checkTmp)
        return True in checkResult
    def verifyKLine(self, begin, end, zs_line):
        zs_check = self.verifyZsLine(zs_line.begin, zs_line.cross[-1], zs_line)
        if not zs_check:
            return False
        if end-begin >= 13 and zs_check:
            return True
        else:
            num = 1
            for i in range(begin+1, end+1):
                if i < zs_line.begin or i > zs_line.cross[-1]:
                    if (self.data['high'][i] < self.data['low'][i-1] or
                        self.data['low'][i] > self.data['high'][i-1]):
                        num = num + 3
                    else:
                        num = num + 1
                else:
                    if (self.data['high'][i] < self.data['low'][i - 1] or
                            self.data['low'][i] > self.data['high'][i - 1]):
                        num = num + 2
                    else:
                        num = num + 1

            if num >= 13:
                return True
            else:
                return False

    def isExtend(self, index, zs_list, begin, tp):

        if tp == 'down':
            if self.data['high'][index] > self.data['high'][begin]:
                return True
            else:
                return False
        if tp == 'up':
            if self.data['low'][index] < self.data['low'][begin]:
                return True
            else:
                return False
    def is_cross_zsh(self, index, zs_line_list, t):
        finished = False
        for zs_line in zs_line_list:
            # self.begin = index
            # self.high = min(data['high'][index], data['high'][index + 1])
            # self.low = max(data['low'][index], data['low'][index + 1])
            # self.cross = [index, index + 1]
            if zs_line.high < self.data['low'][index] or zs_line.low > self.data['high'][index]:
                pass #print("t")
            else:
                # zs_line.cross_last = index
                if index not in zs_line.cross:
                    zs_line.cross.append(index)
                    zs_line.count = zs_line.count + 1
            if (self.data['low'][index] > self.data['high'][index-1]
                or self.data['high'][index] < self.data['low'][index-1]):
                if index not in zs_line.cross:
                    zs_line.cross.append(index)
                    zs_line.count = zs_line.count + 1
            if zs_line.count >= 5:
                finished = True
                t['zs_line_final'] = zs_line
        return finished

    def draw_lines(self):
        start_point = None
        last_line = None
        line_segrement=[]

        for i in range(self.begin_no, len(self.data) - 1):
            if start_point is None:
                start_point = {
                    'start': i,
                    'line_type': self.begin_type,
                    'zs_line': [],
                    'zs_line_final': None,
                    'zhuanzhe': {
                       'high': self.data['low'][i],
                       'low': self.data['high'][i],
                    },
                    'final_temp': None,
                    'final_end': None,

                }
                continue
            up = self.is_up_point(i)  # 是否顶分型
            down = self.is_down_point(i)  # 是否底分型
            # if i == 926:
            #     print("debug")
            if start_point['line_type'] == 'down':
                if start_point['start']+1 == i:
                    self.updateHighLowValue(i, start_point)
                    continue
                #---------延申----
                ex = self.isExtend(i, start_point['zs_line'], start_point['start'], 'down')
                if ex and up:
                    try:
                        line_segrement.pop(-1)
                    except Exception:
                        last_line = {}
                    start_point = last_line
                    start_point['final_end'] = i
                    last_line = start_point
                    line_segrement.append(last_line)
                    start_point = None
                    start_point = {
                        'start': i,
                        'line_type': 'down',
                        'zs_line': [],
                        'zs_line_final': None,
                        'zhuanzhe': {
                            'high': self.data['low'][i],
                            'low': self.data['high'][i],
                        },
                        'final_temp': None,
                        'final_end': None,
                    }
                    continue
                if (self.data['low'][i] < start_point['zhuanzhe']['low']
                        and self.data['low'][i] >= self.data['low'][i-1]):
                    zs_line1 = zs_line(i - 1, self.data)
                    start_point['zs_line'].append(zs_line1)
                if len(start_point['zs_line']) > 0:
                    zs_finished = self.is_cross_zsh(i, start_point['zs_line'], start_point)
                    if zs_finished and down:
                        start_point['final_temp'] = i
                if start_point['final_temp'] is not None:
                    line_ckeck = self.verifyKLine(start_point['start'], i, start_point['zs_line_final'])
                    # 独立2k
                    single = self.verify2kSingle(start_point['start'], i, start_point['zs_line'], start_point['line_type'])
                    #  中枢在中间
                    between = self.verifyBetween(start_point['start'], i, start_point['zs_line'], start_point['line_type'])
                    if line_ckeck and down and single and between:
                        start_point['final_end'] = i
                        last_line = start_point
                        line_segrement.append(last_line)
                        start_point = None
                        start_point = {
                            'start': i,
                            'line_type': 'up',
                            'zs_line': [],
                            'zs_line_final': None,
                            'zhuanzhe': {
                               'high': self.data['low'][i],
                               'low': self.data['high'][i],
                            },
                            'final_temp': None,
                            'final_end': None,
                        }
                        continue
            if start_point['line_type'] == 'up':
                if start_point['start']+1 == i:
                    self.updateHighLowValue(i, start_point)
                    continue
                    #  -------------- 延申 ------
                ex = self.isExtend(i, start_point['zs_line'], start_point['start'], 'up')
                if ex and down:
                    try:
                        line_segrement.pop(-1)
                    except Exception:
                        last_line = {}
                    start_point = last_line
                    start_point['final_end'] = i
                    last_line = start_point
                    line_segrement.append(last_line)
                    start_point = None
                    start_point = {
                        'start': i,
                        'line_type': 'up',
                        'zs_line': [],
                        'zs_line_final': None,
                        'zhuanzhe': {
                            'high': self.data['low'][i],
                            'low': self.data['high'][i],
                        },
                        'final_temp': None,
                        'final_end': None,
                    }
                    continue
                if (self.data['high'][i] > start_point['zhuanzhe']['high']
                        and self.data['high'][i] <= self.data['high'][i-1]):
                    zs_line1 = zs_line(i - 1, self.data)
                    start_point['zs_line'].append(zs_line1)
                if len(start_point['zs_line']) > 0:
                    zs_finished = self.is_cross_zsh(i, start_point['zs_line'], start_point)
                    if zs_finished and up:
                        start_point['final_temp'] = i
                if start_point['final_temp'] is not None:
                    line_ckeck = self.verifyKLine(start_point['start'], i, start_point['zs_line_final'])
                    # 独立2k
                    single = self.verify2kSingle(start_point['start'], i, start_point['zs_line'], start_point['line_type'])
                    #  中枢在中间
                    between = self.verifyBetween(start_point['start'], i, start_point['zs_line'], start_point['line_type'])
                    if line_ckeck and up and single and between:
                        start_point['final_end'] = i
                        last_line = start_point
                        line_segrement.append(last_line)
                        start_point = None
                        start_point = {
                            'start': i,
                            'line_type': 'down',
                            'zs_line': [],
                            'zs_line_final': None,
                            'zhuanzhe': {
                               'high': self.data['low'][i],
                               'low': self.data['high'][i],
                            },
                            'final_temp': None,
                            'final_end': None,
                        }
                        continue
        self.line_segrement = line_segrement




if __name__ == '__main__':
    # with open('data.json', 'r', encoding='utf-8') as file:
    #     data_js = json.load(file)
    # symbol = '^GSPC'
    symbol = 'AAPL'
    period = '30m'
    sql = f'select * from stock_{period} where \"Ticker\" = \'{symbol}\' order by \"Datetime\" asc'
    df = pd.read_sql(sql, engine)
    df.rename(columns={'High': 'high', 'Low': 'low', 'Open': 'open', 'Close': 'close', 'Datetime': 'date'}, inplace=True)
    # 将JSON数据转换为Pandas DataFrame
    # data = pd.DataFrame(data_js)
    data = df
    if data.shape[0] < 12:
        exit()
    # 创建摩尔缠论对象并分析数据
    mct = MooreChanLun(data)
    mct.find_begin_point()
    mct.draw_lines()
    print(mct.line_segrement)
    lins_to_database = []
    for i in mct.line_segrement:
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
    cl_df = pd.DataFrame(lins_to_database, columns=['Ticker', 'Line_Type', 'startDate', 'endDate', 'startPrice', 'endPrice'])
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
    cl_df['startDate'] = cl_df['startDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
    cl_df['endDate'] = cl_df['endDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
    cl_df['class_type'] = None
    iu = InsertOrUpdate(engine, f'moer_cl_{period}', ['Line_Type', 'Ticker', 'startDate'], cc, cl_df)
    iu.insert_or_update()
    print(f'{symbol}_{period} import successful')
