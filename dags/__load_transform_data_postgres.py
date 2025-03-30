import os 
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from Postgres_conn import Postgres
postgres = Postgres('postgres_2')


def load_transformed_data():
    # Import the transform functions, not the DataFrames directly
    from dags._customer_transform import customer_df as get_customer_df
    from dags._geolocation_transform import geolocation_df as get_geolocation_df
    from dags._order_transform import order_df as get_order_df
    from dags._order_item_transform import order_items_df as get_order_items_df
    from dags._payment_transform import payment_df as get_payment_df
    from dags._product_dim_transform import product_df as get_product_df
    from dags._seller_transform import seller_df as get_seller_df
    from dags._datedim_transform import dim_date as get_dim_date

    # Create Postgres connection
    

    # Call the functions to get the DataFrames
    customer_df = get_customer_df()
    geolocation_df = get_geolocation_df()
    order_df = get_order_df()
    order_items_df = get_order_items_df()
    payment_df = get_payment_df()
    product_df = get_product_df()
    seller_df = get_seller_df()
    date_df = get_dim_date()

    #in ra tất cả tên các column của các bảng 

    # print(customer_df.columns)
    # print(geolocation_df.columns)
    # print(order_df.columns)
    # print(order_items_df.columns)
    # print(payment_df.columns)
    # print(product_df.columns)
    # print(seller_df.columns)
    # print(date_df.columns)
    # print(date_df.columns)



    postgres.load_data_postgres('geolocation', geolocation_df, schema='olist_db', if_exists='replace')

    postgres.load_data_postgres('sellers', seller_df, schema='olist_db', if_exists='replace')
    postgres.load_data_postgres('products', product_df, schema='olist_db', if_exists='replace')
    postgres.load_data_postgres('orders', order_df, schema='olist_db', if_exists='replace')
    postgres.load_data_postgres('order_items', order_items_df, schema='olist_db', if_exists='replace')
    postgres.load_data_postgres('order_payments', payment_df, schema='olist_db', if_exists='replace')
    postgres.load_data_postgres('dim_date', date_df, schema='olist_db', if_exists='replace')

    postgres.load_data_postgres('customers', customer_df, schema='olist_db', if_exists='replace')
