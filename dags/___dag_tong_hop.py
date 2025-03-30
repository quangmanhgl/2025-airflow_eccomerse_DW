import sys
import os 
import subprocess

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# Import the functions with different variable names to avoid confusion
from __load_transform_data_postgres import load_transformed_data 
from load_staging_postgres import load_staging_data
from _customer_transform import customer_df
from _geolocation_transform import geolocation_df
from _order_transform import order_df
from _order_item_transform import order_items_df
from _payment_transform import payment_df
from _product_dim_transform import product_df
from _seller_transform import seller_df
from _datedim_transform import dim_date
from __create_relationship import create_relationship

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
}
# 'staging' DAG
with DAG('Staging_Transform_Load',
         default_args=default_args,
         schedule=timedelta(hours=5)) as dag:

    with TaskGroup('staging_data_load') as staging:
        load_staging_data_task = PythonOperator(
            task_id='load_staging_data',
            python_callable=load_staging_data
        )

    with TaskGroup('transform_data') as extract_and_transform:
        
        customer_df_transform = PythonOperator(
            task_id='customer_df_transform',
            python_callable=customer_df
        )

        geolocation_df_transform = PythonOperator(
            task_id='geolocation_df_transform',
            python_callable=geolocation_df
        )

        order_df_transform = PythonOperator(
            task_id='order_df_transform',
            python_callable=order_df
        )

        order_items_df_transform = PythonOperator(
            task_id='order_items_df_transform',
            python_callable=order_items_df
        )

        payment_df_transform = PythonOperator(
            task_id='payment_df_transform',
            python_callable=payment_df
        )

        product_df_transform = PythonOperator(
            task_id='product_df_transform',
            python_callable=product_df
        )

        seller_df_transform = PythonOperator(
            task_id='seller_df_transform',
            python_callable=seller_df
        )

        dim_date_transform = PythonOperator(
            task_id='dim_date_transform',
            python_callable=dim_date
        )

    with TaskGroup('load_transformed_data') as load_transformed_data_task:
        load_postgres_func = PythonOperator(
            task_id='load_postgres_func',
            python_callable=load_transformed_data
        )

        create_relationship_task = PythonOperator(
            task_id='create_relationship',
            python_callable=create_relationship
        )
        
        # Ensure load_postgres_func is executed before create_relationship
        load_postgres_func >> create_relationship_task

    # Define the task order
    load_staging_data_task >> extract_and_transform  # Ensure staging load happens before transformation
    extract_and_transform >> load_transformed_data_task  # Ensure transformation happens before loading to the database
