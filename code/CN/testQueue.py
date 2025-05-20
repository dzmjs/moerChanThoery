import threading
from queue import Queue
import time

def worker(queue):
    while True:
        data = queue.get()
        if data is None:  # 当获取到None时关闭线程
            break
        time.sleep(1)  # 模拟数据处理
        print(f"Processed {data}")
        queue.task_done()

data_list = [f"data_{i}" for i in range(100)]
data_queue = Queue(maxsize=5)  # 限制队列最大为5

# 创建5个工作线程
threads = []
for _ in range(5):
    thread = threading.Thread(target=worker, args=(data_queue,))
    thread.start()
    threads.append(thread)

# 把数据放入队列
for data in data_list:
    data_queue.put(data)

# 等待所有任务处理完毕
data_queue.join()

# 停止所有工作线程
for _ in range(5):
    data_queue.put(None)

# for thread in threads:
#     thread.join()

print("所有任务完成")