from conf import engine
from datetime import datetime, timedelta
from InsertOrUpdate import InsertOrUpdate
from calcutator import calcutator_single
from queue import Queue
import baostock as bs
import pandas as pd
import time
import logging
import threading

logger = logging.getLogger('my_logger_cn')
logger.setLevel(logging.DEBUG)  # 设置最低的日志级别
# 创建一个文件处理器，用于写入日志文件
file_handler = logging.FileHandler('my_log_file.log')
file_handler.setLevel(logging.DEBUG)

# 创建一个格式化器并设置给处理器
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 添加处理器到logger
logger.addHandler(file_handler)

def download_stock_k(stock_type_list, start_date, today):
    # 登录baostock
    bs.login()
    # ---------------子线程调度器--------------
    th_num = 5
    data_queue = Queue(maxsize=th_num)  # 限制队列最大为5
    threads = []
    for _ in range(th_num):
        thread = threading.Thread(target=worker, args=(data_queue,))
        thread.start()
        threads.append(thread)
    # ---------------子线程调度器-init-finished-------------
    sql_query = "select * from stock_list order by  \"code\" asc"
    # 获取股票列表
    stock_list = pd.read_sql(sql_query, engine)
    # rs = bs.query_all_stock(day="2023-12-31")
    # stock_list = []
    # while rs.error_code == '0' and rs.next():
    #     stock_list.append(rs.get_row_data())
    # df_stock_list = pd.DataFrame(stock_list, columns=rs.fields)

    # 选择前5只股票作为示例
    # sample_stocks = df_stock_list.head(5)['code'].tolist()

    # 下载每只股票的历史数据（近30天示例）
    for ind_, row in stock_list.iterrows():
        ttp = row['type']
        for stock_type in stock_type_list:
            # stock_type = '30'
            if ttp == 'index' and (stock_type == '30' or stock_type == '5'):
                continue
            if stock_type == 'd':
                fields = "date,code,open,high,low,close,volume,amount,adjustflag,turn,tradestatus"
            elif stock_type == 'w':
                fields = 'date,code,open,high,low,close,volume,amount,adjustflag,turn'
            elif stock_type == '30':
                fields = 'date,time,code,open,high,low,close,volume,amount,adjustflag'
            else:
                fields = 'date,time,code,open,high,low,close,volume,amount,adjustflag'
            table_name = f'stock_{stock_type}'

            # download
            stock_name = row["code_name"]
            code = row["code"]
            # code = 'sh.000001'
            # code = 'sh.600459'
            print(f"正在获取 {code}-{stock_type} 的数据...{ind_}")
            logger.debug(f"正在获取 {code}-{stock_type} 的数据...{ind_}")
            rs = bs.query_history_k_data_plus(code,
                                              fields,
                                              start_date=start_date, end_date=today,
                                              frequency=stock_type)  #复权类型，默认不复权：3；1：后复权；2：前复权。已支持分钟线、日线、周线、月线前后复权。

            data_list = []
            while rs.error_code == '0' and rs.next():
                data_list.append(rs.get_row_data())

            if data_list:
                df = pd.DataFrame(data_list, columns=rs.fields)
                df.rename(columns={'time': 't_str'}, inplace=True)
                if stock_type == '30' or stock_type == '5':
                    # 取前14位做时间转换
                    df['t_str'] = pd.to_datetime(df['t_str'].str[:14], format='%Y%m%d%H%M%S')
                    # 格式化为字符串
                    df['date'] = df['t_str'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    # df['time'] = df['t_str'].str[8:10] + ':' + df['t_str'].str[10:12] + ':' + df['t_str'].str[12:14]
                # df.to_sql(table_name, engine, if_exists='append', index=False)
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
                if stock_type == 'd':
                    cc.append({"name": "tradestatus", "cType": 'float'})
                    cc.append({"name": "turn", "cType": 'float'})
                elif stock_type == 'w':
                    cc.append({"name": "turn", "cType": 'float'})
                # df['Datetime'] = df[flag].dt.strftime('%Y-%m-%d %H:%M:%S')
                iu = InsertOrUpdate(engine, table_name, key_string, cc, df)
                iu.insert_or_update()
                print(f"{code}_{stock_name} import finished.")
                logger.debug(f"{code}_{stock_name} import finished.")
                # 创建一个线程对象，并指定要运行的函数
                dd = {'stock_code': code, 'period': stock_type, 'stock_type': ttp, 'stock_length': 160}
                if stock_type == '30' or stock_type == '5':
                    dd['stock_length'] = 400
                data_queue.put(dd)
                time.sleep(1.01)
            else:
                print(f"{code} 没有返回数据。")
                logger.error(f"{code} 没有返回数据。")


    # 登出
    bs.logout()
    data_queue.join()
    # 停止所有工作线程
    for _ in range(th_num):
        data_queue.put(None)
    print("All tasks finished! ")

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


if __name__ == '__main__':
    # stock_type_list = ['w', 'd', '30', '5']
    stock_type_list = ['w', 'd', '30']
    # stock_type_list = ['5']
    # stock_type_list = ['30']
    # stock_type_list = ['w', 'd']
    today = datetime.today()
    today = timedelta(days=1) + today
    start_date = today - timedelta(days=3)
    today = today.strftime('%Y-%m-%d')
    start_date = start_date.strftime('%Y-%m-%d')
    download_stock_k(stock_type_list, start_date, today)