import pandas as pd 

import os
import sys


current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from Postgres_conn import  Postgres
postgres = Postgres('postgres_2')

def product_df():

    catergories_df = postgres.get_data_df('SELECT * FROM staging.product_category_name_translation')
    product_df3 = postgres.get_data_df('SELECT * FROM staging.olist_products_dataset')    

    product_df2 = pd.merge(product_df3, catergories_df, on='product_category_name', how='left')


    product_df2 = product_df2.drop("product_category_name", axis=1)
    return pd.DataFrame(product_df2)



