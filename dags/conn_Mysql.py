from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd

class Mysql():
    def __init__(self, conn_id: str): 
        self.conn_id = conn_id

    def connect(self):
        self.hook = MySqlHook(self.conn_id)
        self.connection = self.hook.get_conn()
        self.cursor = self.connection.cursor()
        print("Connection successful!")

    def execute_query(self, query=None):
        try:
            self.connect()
            self.cursor.execute(query)
            # Lấy tất cả các dòng dữ liệu
            results = self.cursor.fetchall()
            # Lấy tên các cột từ cursor.description
            columns = [desc[0] for desc in self.cursor.description]
            return results, columns
        finally:
            self.cursor.close()
            self.connection.close

    def get_data_df(self, query=None):
        # Lấy kết quả và tên cột từ execute_query
        results, columns = self.execute_query(query)
        # Tạo DataFrame từ kết quả và tên cột
        return pd.DataFrame(results, columns=columns)
