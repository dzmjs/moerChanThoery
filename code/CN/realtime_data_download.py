import time
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import akshare as ak
import pandas as pd
from queue import Queue

from InsertOrUpdate import InsertOrUpdate
from conf import engine
from my_logger import logger
from calcutator import calcutator_single

def get_all_stock_list():
    # 获取当前沪深 A 股的股票列表
    stock_info_a_code_name_df = ak.stock_info_a_code_name()
    print(stock_info_a_code_name_df)
    stock_info_a_code_name_df.to_csv("./akshare_all_stock_list.csv", index=False, encoding='utf-8-sig')

def worker(queue):
    while True:
        data = queue.get()
        if data is None:  # 当获取到None时关闭线程
            break
        # print(f"Processed {data}")
        my_thread_function(data['stock_code'], data['period'], data['stock_type'], data['stock_length'])
        queue.task_done()
def my_thread_function(stock_code, period, stock_type, stock_length=100):
    # print(f"子线程开始，参数：stock_code={stock_code}, period={period}, stock_type={stock_type}")
    # time.sleep(5)
    calcutator_single(stock_code, period, stock_type, stock_length)
    # print("子线程结束")

def download_stock_data_with_chan_analyse(period_list=['30','5']):
    # ---------------子线程调度器--------------
    th_num = 5
    data_queue = Queue(maxsize=th_num)  # 限制队列最大为5
    threads = []
    for _ in range(th_num):
        thread = threading.Thread(target=worker, args=(data_queue,))
        thread.start()
        threads.append(thread)
    #-----------------Queue Init Finished---------------------
    now_shanghai = datetime.now(ZoneInfo("Asia/Shanghai"))
    bg_date = now_shanghai - timedelta(weeks=1)
    now_shanghai = now_shanghai + timedelta(hours=6)
    yestoday_shanghai = now_shanghai - timedelta(days=2)
    now_shanghai = now_shanghai.strftime('%Y-%m-%d %H:%M:%S')
    yestoday_shanghai = yestoday_shanghai.strftime('%Y-%m-%d %H:%M:%S')
    bg_date = bg_date.strftime('%Y-%m-%d')
    sql_query = f'''
            select * from stock_list where "type"='stock' and code in
                (select "Ticker" from moer_cl_d where "endDate">='{bg_date}' and class_type is not null)
                order by  "code" 
            '''
    # 获取股票列表
    stock_list = pd.read_sql(sql_query, engine)
    for ind_, row in stock_list.iterrows():
        stock_name = row["code_name"]
        ttp = row['type']
        code = row["code"]
        # code = 'sh.000001'
        # code = 'sh.600459'
        for period in period_list:
            print(f"正在获取 {stock_name}{code}-{period} 的数据...{ind_}")
            # if code == 'sz.002299':
            #     print("debug")
            # logger.debug(f"正在获取 {code}-{period} 的数据...{ind_}")
            # get_stock_minute_data_single(code, period)
            try:
                get_stock_minute_data_single_quickly(code, ttp, yestoday_shanghai, now_shanghai, period)
                dd = {'stock_code': code, 'period': period, 'stock_type': ttp, 'stock_length': 433}
                data_queue.put(dd)
                time.sleep(0.5)
            except Exception:
                pass
    data_queue.join()
    # 停止所有工作线程
    for _ in range(th_num):
        data_queue.put(None)
    print("All tasks finished! ")

def download_stock_data(period_list=['30', '5']):
    # 当前UTC时间转上海时间
    now_shanghai = datetime.now(ZoneInfo("Asia/Shanghai"))
    bg_date = now_shanghai - timedelta(weeks=1)
    now_shanghai = now_shanghai + timedelta(hours=6)
    yestoday_shanghai = now_shanghai - timedelta(days=2)
    now_shanghai = now_shanghai.strftime('%Y-%m-%d %H:%M:%S')
    yestoday_shanghai = yestoday_shanghai.strftime('%Y-%m-%d %H:%M:%S')
    bg_date = bg_date.strftime('%Y-%m-%d')
    sql_query = f'''
        select * from stock_list where "type"='stock' and code in
            (select "Ticker" from moer_cl_d where "endDate">='{bg_date}' and class_type is not null)
            order by  "code" 
        '''
    # 获取股票列表
    stock_list = pd.read_sql(sql_query, engine)
    for ind_, row in stock_list.iterrows():
        stock_name = row["code_name"]
        ttp = row['type']
        code = row["code"]
        # code = 'sh.000001'
        # code = 'sh.600459'
        for period in period_list:
            print(f"正在获取 {code}-{period} 的数据...{ind_}")
            # logger.debug(f"正在获取 {code}-{period} 的数据...{ind_}")
            # get_stock_minute_data_single(code, period)
            try:
                get_stock_minute_data_single_quickly(code, ttp, yestoday_shanghai, now_shanghai, period)
                time.sleep(0.9)
            except Exception:
                pass

def get_stock_minute_data_single_quickly(code, type, start_date, end_date, period='30'):
    code_t = code[-6:]
    if type == 'stock':
        stock_zh_a_hist_min_em_df = ak.stock_zh_a_hist_min_em(symbol=code_t,
                                                              start_date=start_date, #"2024-03-20 09:30:00",
                                                              end_date=end_date, #"2024-03-20 15:00:00",
                                                              period=period)
    elif type == 'index':
        stock_zh_a_hist_min_em_df = ak.index_zh_a_hist_min_em(code_t, period, start_date, end_date)
    stock_zh_a_hist_min_em_df.rename(columns={'时间': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high',
                                              '最低': 'low', '成交量': 'volume'}, inplace=True)
    stock_zh_a_hist_min_em_df['code'] = code
    # print(stock_zh_a_hist_min_em_df)
    stock_zh_a_hist_min_em_df['amount'] = None
    stock_zh_a_hist_min_em_df['adjustflag'] = None
    cc = [
        {"name": "date", "cType": 'datetime'},
        {"name": "code", "cType": 'string'},
        {"name": "open", "cType": 'float'},
        {"name": "high", "cType": 'float'},
        {"name": "low", "cType": 'float'},
        {"name": "close", "cType": 'float'},
        {"name": "volume", "cType": 'float'},
        {"name": "amount", "cType": 'float'},
        {"name": "adjustflag", "cType": 'float'},
    ]
    last_time = stock_zh_a_hist_min_em_df['date'].iloc[-1]
    key_string = ['date', 'code']
    # df['Datetime'] = df[flag].dt.strftime('%Y-%m-%d %H:%M:%S')
    table_name=f'stock_{period}'
    iu = InsertOrUpdate(engine, table_name, key_string, cc, stock_zh_a_hist_min_em_df)
    iu.insert_or_update()
    print(f"{code}_{period} import min_data finished. last_time: {last_time}")
    logger.debug(f"{code}_{period} import min_data finished. last_time: {last_time}")

def get_stock_minute_data_single(stock_code='sh.000001', period='5'):
    code = stock_code.replace('.', '')
    table_name = f'stock_{period}'
    data = ak.stock_zh_a_minute(code, period)
    # data2 = ak.stock_zh_a_minute(stock_code, period)
    # dd = ak.index_zh_a_hist_min_em('000001', '5', '2025-05-22', '2025-05-29')
    data.rename(columns={'day': 'date'}, inplace=True)
    data['code'] = stock_code
    data['amount'] = None
    data['adjustflag'] = None
    last_time = data['date'].iloc[-1]
    cc = [
        {"name": "date", "cType": 'datetime'},
        {"name": "code", "cType": 'string'},
        {"name": "open", "cType": 'float'},
        {"name": "high", "cType": 'float'},
        {"name": "low", "cType": 'float'},
        {"name": "close", "cType": 'float'},
        {"name": "volume", "cType": 'float'},
        {"name": "amount", "cType": 'float'},
        {"name": "adjustflag", "cType": 'float'},
    ]
    key_string = ['date', 'code']
    # df['Datetime'] = df[flag].dt.strftime('%Y-%m-%d %H:%M:%S')
    iu = InsertOrUpdate(engine, table_name, key_string, cc, data)
    iu.insert_or_update()
    print(f"{code}_{stock_code} import minute_data finished. last_time: {last_time}")
    logger.debug(f"{code}_{stock_code} import minute_data finished. last_time: {last_time}")


if __name__ == '__main__':
    # get_all_stock_list()
    # download_stock_data(['30', '5'])
    # get_stock_minute_data_single('sz.002299', '30')
    get_stock_minute_data_single_quickly('sz.002299','stock','2025-05-25','2025-05-30', '30')