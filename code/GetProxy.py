import requests
from bs4 import BeautifulSoup
import pandas as pd

# 验证代理的函数
def check_proxy(proxies):
    # 构造代理格式
    try:
        # httpbin.org/ip 会返回访问方看到的IP，适合用来测试
        resp = requests.get("https://query2.finance.yahoo.com/", proxies=proxies, timeout=5)
        if resp.status_code == 200:
            print(f"{ip}:{port} is useful")
            # print(f"有效代理: {ip}:{port} -> 返回内容: {resp.text.strip()}")
        else:
            print(f"{ip}:{port} is not work:{resp.status_code}")
            pass
            # print(f"无效代理: {ip}:{port}，返回状态码: {resp.status_code}")
    except Exception as e:
        print(f"异常")
        # print(f"无效代理: {ip}:{port}，异常: {e}")
        pass

'''
if __name__ == '__main__':
    ip = "196.251.131.90"
    port = '8080'
    proxy = {
        "https": f"http://{ip}:{port}",
        "http": f"http://{ip}:{port}"
    }
    check_proxy(proxy)
'''
if __name__ == '__main__':
    # getProxy()
    # 代理列表，以 (ip, port) 形式
    df = pd.read_csv('proxy.csv')  # 这里只是示例入口
    # 批量验证
    # for ip, port in d:
    for index, row in df.iterrows():
        https = row['Https']
        ip = row['IP Address']
        port = row['Port']
        if https == 'yes':
            proxy = {
                "https": f"https://{ip}:{port}",
                "http": f"http://{ip}:{port}"
            }
        else:
            proxy = {
                "https": f"http://{ip}:{port}",
                "http": f"http://{ip}:{port}"
            }
        check_proxy(proxy)
