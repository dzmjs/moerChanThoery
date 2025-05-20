from sqlalchemy import create_engine

# PostgreSQL 配置（请替换为你自己的）
db_user = 'postgres'
db_password = 'dzm'
db_host = 'localhost'
db_port = '5432'
db_name = 'stock_cn'

# 建立数据库连接
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
# engine = create_engine('postgresql://postgres:dzm@localhost:5432/stock')