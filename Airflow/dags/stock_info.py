# coding: utf-8
#import libraries
#endoding


import requests
import json
from time import sleep
from datetime import date
import time
from datetime import datetime
import pandas as pd
#from tqdm import tqdm
import config
from airflow import DAG
pd.set_option('display.max_columns', None)


def get_stock_info(ti):
        datalist = []
        for company,stock in config.target_companies_stock_names.items():
               
                response = requests.get(config.URL.format(stock,apikey))
                if response.status_code ==  200 and  len(response.content) > 10000 :
                    data = json.loads(response.content)
                    datalist.append([data['Meta Data']['3. Last Refreshed'],list(list(data['Time Series (60min)'].values())[0].values())[3],list(list(data['Time Series (60min)'].values())[0].values())[4],company,config.target_companies_stock_names[company]])
                else:
                   return "Broken Json file"


        df_stock_info = pd.DataFrame(datalist,columns = ['last_refreshed','close_value','volume','company_name','stock_name'])
        ti.xcom_push(key="stocks",value = df_stock_info.to_json())
 



