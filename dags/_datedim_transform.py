import pandas as pd 
import os
import sys
import numpy as np
import datetime as dt  

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

def dim_date():
    end_date = dt.datetime.now()  
    start_date = dt.datetime(2016, 1, 1) 
    date_range = pd.date_range(start_date, end_date) 

    dim_date_df = pd.DataFrame({
        'date': date_range.strftime('%Y-%m-%d'),
        'year': date_range.year,
        'month': date_range.month,
        'day': date_range.day,
        'quarter': date_range.quarter,
    })

    return dim_date_df

