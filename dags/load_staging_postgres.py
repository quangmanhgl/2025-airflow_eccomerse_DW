import subprocess
import pandas as pd
from Postgres_conn import Postgres
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

def load_staging_data():
    postgres = Postgres('postgres_2')

    import os
    #staging data postgres


    dataset_path = os.path.join(os.path.dirname(__file__), 'dataset')

    list_file = os.listdir(dataset_path) 


    schema = 'staging'
    for file in list_file:
        df = pd.read_csv(f'{dataset_path}/{file}')
        table_name = file.split('.')[0]
        postgres.load_data_postgres(table_name, df, schema)
        print(f'{table_name} loaded to Postgres into {schema} schema')


    return print('All staging data loaded to Postgres')
