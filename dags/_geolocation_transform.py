import os
import sys
import pandas as pd

# Get current directory and parent directory
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# Import Postgres connection class
from Postgres_conn import Postgres
postgres = Postgres('postgres_2')  # Connect to PostgreSQL
def geolocation_df():
    # Fetch the geolocation dataset and clean it
    geolocation_df1 = postgres.get_data_df('SELECT * FROM staging.olist_geolocation_dataset')
    
    # Drop duplicates based on the zip code

    # Ensure the column 'geolocation_zip_code_prefix' is of type int

    # Query to find missing customer zip codes
    missing_customer_zips_query = """
    SELECT DISTINCT customer_zip_code_prefix 
    FROM staging.olist_customers_dataset c
    LEFT JOIN staging.olist_geolocation_dataset g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
    WHERE g.geolocation_zip_code_prefix IS NULL;
    """

    # Query to find missing seller zip codes
    missing_seller_zips_query = """
    SELECT DISTINCT seller_zip_code_prefix 
    FROM staging.olist_sellers_dataset s
    LEFT JOIN staging.olist_geolocation_dataset g ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
    WHERE g.geolocation_zip_code_prefix IS NULL;
    """
    
    # Get missing zip codes
    missing_customer_zips = postgres.get_data_df(missing_customer_zips_query)
    missing_seller_zips = postgres.get_data_df(missing_seller_zips_query)
    
    # Rename the relevant columns to match 'geolocation_zip_code_prefix'
    missing_customer_zips.rename(columns={'customer_zip_code_prefix': 'geolocation_zip_code_prefix'}, inplace=True)
    missing_seller_zips.rename(columns={'seller_zip_code_prefix': 'geolocation_zip_code_prefix'}, inplace=True)
    
    # Concatenate the customer and seller missing zip codes
    all_missing_zips = pd.concat([missing_customer_zips, missing_seller_zips], axis=0)

    # Create a list of new rows to append
    new_rows = []
    for zip_code in all_missing_zips['geolocation_zip_code_prefix']:
        new_row = {
            'geolocation_zip_code_prefix': zip_code,  # Missing zip code
            'geolocation_lat': 0,  # Default lat value
            'geolocation_lng': 0,  # Default lng value
            'geolocation_city': 'Unknown',  # Default city
            'geolocation_state': 'Unknown'  # Default state
        }
        new_rows.append(new_row)

    # Convert the list of new rows to a DataFrame
    new_rows_df = pd.DataFrame(new_rows)

    # Ensure the columns of new rows match the original DataFrame's columns
    new_rows_df = new_rows_df[geolocation_df1.columns]  # Reorder columns if necessary

    # Append new rows to the existing geolocation_df1 DataFrame
    geolocation_df1 = pd.concat([geolocation_df1, new_rows_df], ignore_index=True)
    geolocation_df1['geolocation_zip_code_prefix'] = geolocation_df1['geolocation_zip_code_prefix'].astype(int)

    geolocation_df1.drop_duplicates(subset='geolocation_zip_code_prefix', keep='first', inplace=True)

    return geolocation_df1  

