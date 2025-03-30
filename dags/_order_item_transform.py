
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

def order_items_df():

    order_items_df1 = postgres.get_data_df('SELECT * FROM staging.olist_order_items_dataset')

    order_items_df1["shipping_limit_date"  ] = pd.to_datetime(order_items_df1["shipping_limit_date"]).dt.strftime('%Y-%m-%d')
    return pd.DataFrame(order_items_df1)
