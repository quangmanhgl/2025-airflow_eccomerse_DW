import pandas as pd 
import numpy as np
import datetime as dt 
import os
import sys



current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from Postgres_conn import  Postgres
postgres = Postgres('postgres_2')


def order_df():


    order_df1 = postgres.get_data_df('SELECT * FROM staging.olist_orders_dataset')


    order_df1["order_id"].drop_duplicates    
    order_df1.isnull().sum()


    order_df1['order_purchase_timestamp'] = (pd.to_datetime(order_df1['order_purchase_timestamp'])).dt.strftime('%Y-%m-%d')
    order_df1['order_approved_at'] = (pd.to_datetime(order_df1['order_approved_at'])).dt.strftime('%Y-%m-%d')

    order_df1['order_delivered_carrier_date'] = pd.to_datetime(order_df1['order_delivered_carrier_date']).dt.strftime('%Y-%m-%d')
    order_df1['order_delivered_customer_date'] = pd.to_datetime(order_df1['order_delivered_customer_date']).dt.strftime('%Y-%m-%d')
    order_df1['order_estimated_delivery_date'] = pd.to_datetime(order_df1['order_estimated_delivery_date']).dt.strftime('%Y-%m-%d')

    return pd.DataFrame(order_df1)

