# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine

# 读取CSV
df = pd.read_csv('nasdaq_screener_1745727021714.csv')

# 一些字段需要清理，如"Last Sale"去掉美元符和空格转为float
df['Last Sale'] = df['Last Sale'].replace({'\$':'', ',':'', ' ':''}, regex=True).replace('', '0').astype(float)
df['% Change'] = df['% Change'].replace({'%':'', ' ':'', ',':''}, regex=True).replace('', '0').astype(float)
# df['Last_Sale'] = df['Last Sale']
# df['Net_Change'] = df['Net Change']
# df['Percent_Change'] = df['% Change']
# df['IPO_Year'] = df['IPO Year']
# 请替换成你的连接串
engine = create_engine('postgresql://postgres:dzm@localhost:5432/stock')

# 建议指定表名为"us_stocks"
df.to_sql('stock_list', engine, if_exists='replace', index=False)

print("导入成功！")