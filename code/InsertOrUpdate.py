import json
import pandas as pd
from sqlalchemy import text
from conf import engine

class InsertOrUpdate:
    def __init__(self, engineO, tableName, key, columns, df):
        self.data = df
        self.tableName = tableName
        self.key = key
        self.engine = engineO
        '''
        columns:[
            {'name':"id",'cType':"integer"},
            {'name':"value1",'cType':"string"},
            {'name':"value2",'cType':"string"},
        ]
        '''
        self.columns = columns
        self.insert_upsert_query = f'INSERT INTO {tableName} ('
        value_str = '('
        value_update = ''
        for column in self.columns:
            self.insert_upsert_query = self.insert_upsert_query +"\""+ column['name']+"\", "
            value_str = value_str+":"+column['name']+", "
            if column['name'] not in key:
                value_update = value_update + "\""+column['name']+"\"=EXCLUDED.\""+column['name']+'\", '
        #remove last 2 digit
        self.insert_upsert_query = self.insert_upsert_query[:-2]
        value_str =value_str[:-2]
        value_update =value_update[:-2]
        # finish remove
        self.insert_upsert_query = self.insert_upsert_query + ") "
        value_str = value_str + ") "
        self.insert_upsert_query = self.insert_upsert_query + " VALUES "+value_str
        key = ["\""+it+"\"" for it in key]
        keys = ','.join(map(str, key))
        on = f' ON CONFLICT ({keys}) DO UPDATE SET '
        self.insert_upsert_query = self.insert_upsert_query + on+ value_update
        # print(self.insert_upsert_query)

    def repleace_to_null(self, json):
        for key in json:
            if json[key] == '':
                json[key] = None
    def insert_or_update(self):
        ss = text(self.insert_upsert_query)
        list = self.data.to_json(orient='records', force_ascii=False)
        list = json.loads(list)
        # 使用SQLAlchemy执行自定义SQL
        with self.engine.connect() as connection:
            for row in list:
                self.repleace_to_null(row)
                # 传递参数时，确保是元组或字典形式
                connection.execute(ss, row)
            connection.commit()




if __name__ == '__main__':
    # 创建示例DataFrame
    data = {
        'id': [1, 2, 3],
        'value1': ['A2', 'B3', 'C'],
        'value2': ['X', 'Y4', 'Z5'],
        'time': ['2019-05-25', '2019-05-26', '2019-05-26']
    }
    df = pd.DataFrame(data)
    cc = [
        {'name': "id", 'cType': "integer"},
        {'name': "value1", 'cType': "string"},
        {'name': "value2", 'cType': "string"},
        {'name': "time", 'cType': "date"},
    ]
    iu = InsertOrUpdate(engine, tableName="my_table", key=['id'], columns=cc, df=df)
    iu.insert_or_update()
