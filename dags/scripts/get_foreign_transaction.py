import requests
from scripts.utils.helper import *
from scripts.utils.config import dbInfor
import numpy as np
import pandas as pd
import pymysql
import sqlalchemy
import sys
from datetime import datetime, timedelta

# con = get_mysql_connection()
# list_company_query = f"""
# select distinct ticker from ticker_overview
# """
# list_company = get_data_from_mysql(con, list_company_query)['ticker']
# con.close()
# engine = create_mysql_engine()
# for ticker in list_company:
#     try:
#         foreignVolURL = 'https://apipubaws.tcbs.com.vn/tcanalysis/v1/data-charts/vol-foreign'
#         foreignVolParams = {'ticker': ticker}
#         response = requests.get(foreignVolURL, params=foreignVolParams)
#         foreignVol = pd.DataFrame(response.json()['listVolumeForeignInfoDto'])
#         foreignVol['date'] = pd.to_datetime(foreignVol["dateReport"], format='%d/%m/%Y')
#         foreignVol[['date', 'ticker','foreignBuy', 'foreignSell', 'netForeignVol', 'accNetFVol']].to_sql(con=engine, name='foreign_transaction', if_exists='append', index=False)
#         print(ticker)
#     except Exception as e:
#         print(e)
#         print(ticker)

def push_foreign_transaction():
    con = get_mysql_connection()
    max_date_query = """
    select ticker, max(date) as date
    from foreign_transaction
    group by ticker
    """
    lastest_date = get_data_from_mysql(con, max_date_query)
    con.close()
    list_df = []
    for id, row in lastest_date.iterrows():
        ticker = row['ticker']
        date = row['date']
        foreignVolURL = 'https://apipubaws.tcbs.com.vn/tcanalysis/v1/data-charts/vol-foreign'
        foreignVolParams = {'ticker': ticker}
        response = requests.get(foreignVolURL, params=foreignVolParams)
        foreignVol = pd.DataFrame(response.json()['listVolumeForeignInfoDto'])
        foreignVol['date'] = pd.to_datetime(foreignVol["dateReport"], format='%d/%m/%Y')
        foreignVol = foreignVol[foreignVol['date'] > date]
        if len(foreignVol) > 0:
            list_df.append(foreignVol[['date', 'ticker','foreignBuy', 'foreignSell', 'netForeignVol', 'accNetFVol']])
        if id % 100 == 0:
            print(id)

    all_foreignVol = pd.concat(list_df)
    all_foreignVol = all_foreignVol.drop_duplicates(subset=['ticker', 'date'])
    engine = create_mysql_engine()
    all_foreignVol.to_sql(con=engine, name='foreign_transaction', if_exists='append', index=False)