from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
import pandas as pd 

class Postgres():
    def __init__ ( self, conn_id: str):
        self.conn_id = conn_id
        self.hook = PostgresHook(self.conn_id)
        
    def connect(self):
        self.connection = self.hook.get_conn()
        return self.connection


    def execute_query(self,query,schema):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.commit()
        
        cursor.execute(query)
        conn.commit()
        conn.close()

    def load_data_postgres(self, table_name, df, schema='public2', if_exists='replace'):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.commit()


        
        uri = self.hook.get_uri()
        engine = create_engine(uri)
        df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)
    
        conn.close()
    def get_data_df(self,query = None):
        conn = self.connect()
        df = pd.read_sql(query, conn)
        conn.close()
        return df

