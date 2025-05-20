from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:dzm@localhost:5432/stock')