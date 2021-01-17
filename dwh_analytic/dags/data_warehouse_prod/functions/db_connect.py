from sqlalchemy import create_engine
import pandas as pd
import psycopg2

class EngineConnect:
    def __init__(
        self, 
        uri: str
    ):
        self.engine = create_engine(uri)
        self.conn = self.engine.connect()
         
    def create(self, data, schema, table):
        df = pd.json_normalize(data)
        df.to_sql(table, schema=schema, con = self.engine, index = False, if_exists = 'append')
    
    def create_df(self, df, schema, table):
        df.to_sql(table, schema=schema, con = self.engine, index = False, if_exists = 'append')
        
    def update(self, data, schema, table):
        df = pd.json_normalize(data)
        df.to_sql(table, schema=schema, con = self.engine, index = False, if_exists = 'replace')
    
    def update_df(self, df, schema, table):
        df.to_sql(table, schema=schema, con = self.engine, index = False, if_exists = 'replace')
    
    def update_df_with_index(self, df, schema, table, index):
        df.set_index(index , inplace=True)
        df.to_sql(table, schema=schema, con = self.engine, index = True, if_exists = 'replace')
        
    def raw_sql(self, sql):
        self.conn.execute(sql)
        
    def get_max_id_table(self, schema: str, table: str, col: str):
        result = self.conn.execute('select max({col}) from {schema}."{table}";'.format(schema = schema, col=col, table=table))
        rows = result.fetchone()
        if len(rows) == 0:
            return None
        else:
            return rows[0]
    
    def close(self):
        self.engine.dispose()
        
class SqlAlchemyDB:
    def __init__(
        self,
        *kwargs,
        user: str,
        password: str,
        host: str,
        port: str,
        database: str, 
    ):
        self.conn = None
        self.cur = None
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
    
    def connect(self) -> None:
        try:
            self.conn = psycopg2.connect(user = self.user, password = self.password, host = self.host, port = self.port, database = self.database)
            self.cur = self.conn.cursor()  
        except Exception as e:
            raise Exception('Couldn\'t connect to the database. Error: {}'.format(e))

    def commit(self) -> None:
        if self.conn is not None:
            self.conn.commit()
        else:
            raise Exception('Connection not opened to commit')

    def close(self) -> None:
        if self.cur is not None or self.conn is not None:
            try:
                self.cur.close()
            except:
                pass
            try:
                self.conn.close()
            except:
                pass
        else:
            print('Connection and Cursor not opened to be closed')