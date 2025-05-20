import pandas as pd
from conf import engine
from sqlalchemy import text

# 步骤1：从数据库查询的数据（假定已经得到df_raw）
# 假设只有以下数据
df_raw = pd.DataFrame({
    'date': pd.date_range('2025-04-01', '2025-05-01'),
    'value': range(31)  # 举例的其它数据
})

# 步骤2：构造完整日期区间
full_dates = pd.date_range('2025-01-02', '2025-05-01')

# 步骤3：设置date为索引，然后reindex
df_full = df_raw.set_index('date').reindex(full_dates).reset_index()
df_full = df_full.rename(columns={'index': 'date'})  # 将索引名称改回date

print(df_full)

sql = '''
select * from stock_5m where "Ticker" = 'AAPL' 
 and "Datetime" > %s
 and "Datetime" <= %s
 order by "Datetime" asc 
'''
df = pd.read_sql(sql, engine, params=("2025-04-28 19:03:51+00", "2025-05-02 17:03:51+00"))
print(df)