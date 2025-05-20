import baostock as bs
import pandas as pd

# 登录系统
lg = bs.login()

# 显示登录返回信息
print('登录状态:', lg.error_msg)

# 获取股票列表
rs = bs.query_all_stock()  # 可指定日期，获取当日有效的股票清单

# 存储查询结果
stock_list = []

while rs.error_code == '0' and rs.next():
    stock_list.append(rs.get_row_data())

# 转为DataFrame
df = pd.DataFrame(stock_list, columns=rs.fields)

# 退出系统
bs.logout()

# 显示前几行数据
print(df.head())

# 可选：保存为 CSV
df.to_csv("./all_stock_list.csv", index=False, encoding='utf-8-sig')
