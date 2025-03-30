import os
import sys

import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from Postgres_conn import  Postgres
postgres = Postgres('postgres_2')

def payment_df():

    payment_df1 = postgres.get_data_df('SELECT * FROM staging.olist_order_payments_dataset')


    payment_df1['payment_value']  = payment_df1['payment_value'].astype(float)
    payment_df1.isnull().sum()

    return pd.DataFrame(payment_df1)
