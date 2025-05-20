from conf import engine
from InsertOrUpdate import InsertOrUpdate
import pandas as pd

if __name__ == '__main__':
    # symbol = 'NVDA'
    symbol_list_sql = "SELECT \"Symbol\" FROM stock_list where \"CodeType\" = 'stock'"
    symbol_list = pd.read_sql(symbol_list_sql, engine)
    for _, symbol in symbol_list.iterrows():
        sql = f"""
        SELECT "Datetime", "Ticker", "Volume"
        FROM stock_1d
        WHERE "Ticker" = '{symbol['Symbol']}'
          AND "Datetime" >= '2025-01-01'
        ORDER BY "Datetime" asc
        """

        df = pd.read_sql(sql, engine)

        # 3. 计算MA5与MA10
        df['MA5'] = df['Volume'].rolling(window=5, min_periods=1).mean()
        df['MA10'] = df['Volume'].rolling(window=10, min_periods=1).mean()

        # 4. 找出成交量大于MA5的数据
        df_selected = df[df['Volume'] > df['MA5']].copy()
        if df_selected.shape[0] == 0:
            continue
        # 5. 保存到数据库的目标表（例如stock_ma_selected）
        cc = [
            {'name': 'Ticker', 'cType': 'string'},
            {'name': 'Datetime', 'cType': 'date'},
            {'name': 'Volume', 'cType': 'int'},
            {'name': 'MA5', 'cType': 'int'},
            {'name': 'MA10', 'cType': 'int'},
        ]
        df_selected['Datetime'] = df_selected['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        iu = InsertOrUpdate(engine, f'stock_hot_selected', ['Ticker', 'Datetime'], cc, df_selected)
        iu.insert_or_update()
        print(f'{symbol}_ MA Calc import successful')