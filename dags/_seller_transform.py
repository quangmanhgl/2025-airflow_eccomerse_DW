# dimension of date
from datetime import datetime
import pandas as pd

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from Postgres_conn import  Postgres
postgres = Postgres('postgres_2')

def seller_df():

    seller_df2 = postgres.get_data_df('SELECT * FROM staging.olist_sellers_dataset')

    seller_df2['seller_zip_code_prefix'] = seller_df2['seller_zip_code_prefix'].astype(int)
    seller_df2.drop_duplicates(subset='seller_id', keep='first', inplace=True)
    return pd.DataFrame(seller_df2)

