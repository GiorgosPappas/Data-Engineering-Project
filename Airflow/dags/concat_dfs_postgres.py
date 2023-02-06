#import libraries

from tweeter import get_tweets_create_df
from stock_info import get_stock_info
import pandas as pd 
from datetime import datetime 
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import psycopg2
import sqlalchemy
import json

#merge tweeter and stock info dfs on company name
def get_final_df(ti):
  try:
    tweeter_df = ti.xcom_pull(key = "tweets",task_ids = ['tweets'])
    stock_df =ti.xcom_pull(key ="stocks" ,task_ids = ['stock_info']) 
    final_df =pd.merge(tweeter_df, stock_df, on='company_name') 
    ti.xcom_push(key="final_df",value=final_df.to_json())
  except :
    ti.xcom_push(key = "get_final_df",value = 'Cannot Concatenate DataFrames --> "IP Blocked"')


def create_connection(ti):
    # connect to db
    engine = create_engine(
    'postgresql+psycopg2:'                       
    '//username:'            # username for postgres       
    'password'               # password for postgres  
    '@<yourip>:5432/'  # vms' ip and the exposed port                
    '<DB_name>')

    con = engine.connect()

    with engine.begin() as connection:

    # create empty table


        sql = """
            create table if not exists stocks_tweeter (
                id varchar(550),
                date timestamp,
                tweeter_user varchar(20),
                tweet VARCHAR(550),
                reply_count int,
                retweet_count int,
                like_count int,
                quote_count int,
                view_count int,
                company_Name VARCHAR(50),
                language VARCHAR(50),
                sentiment VARCHAR(50),
                last_refreshed timestamp,
                close_value float,
                volume int,
                stock_name VARCHAR(20)


        )"""
    # execute the 'sql' query
    with engine.connect().execution_options(autocommit=True) as conn:
        query = conn.execute(text(sql))

    # insert the dataframe data to SQL table
    with engine.connect().execution_options(autocommit=True) as conn:
        final_df =  ti.xcom_pull(key="final_df",task_ids = ['get_finall_df'])
        final_df =pd.DataFrame(json.loads(final_df))

        return final_df.to_sql('stocks_tweeter', con=conn, if_exists='append', index= False)

