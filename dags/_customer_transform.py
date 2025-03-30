import pandas as pd 
import os
import sys
import numpy as np
import datetime as dt

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from Postgres_conn import  Postgres
postgres = Postgres('postgres_2')

def customer_df():

    customer_df1 = postgres.get_data_df('SELECT * FROM staging.olist_customers_dataset')


    # Data transformations
    customer_df1['customer_zip_code_prefix'] = customer_df1['customer_zip_code_prefix'].astype(int)
    customer_df1['customer_id'] = customer_df1['customer_id'].astype(str)
    customer_df1['customer_unique_id'] = customer_df1['customer_unique_id'].astype(str)
    customer_df1['customer_city'] = customer_df1['customer_city'].astype(str)
    customer_df1['customer_state'] = customer_df1['customer_state'].astype(str)


    # Note: this line doesn't actually do anything without assignment or .inplace=True
    customer_df1 = customer_df1.drop_duplicates(subset=['customer_id'])  # Fixed to actually drop duplicates

    return customer_df1
