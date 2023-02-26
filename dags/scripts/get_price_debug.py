import requests
from scripts.utils.helper import *
from scripts.utils.config import dbInfor
import numpy as np
import pandas as pd
import pymysql
import sqlalchemy
import sys
from datetime import datetime,timedelta

con = get_mysql_connection()
today = datetime.now()
priceURL = f"https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term"
foreignVolURL = 'https://apipubaws.tcbs.com.vn/tcanalysis/v1/data-charts/vol-foreign'
sql = f"select ticker, max(date) as date from price_board group by ticker"
date = get_data_from_mysql(con, sql)
con.close()

engine = create_mysql_engine()

for id, row in date.iterrows():
    ticker = row['ticker']
    d = row['date']
    if d.date() == today.date():
        continue

    priceParams = {'ticker': ticker, 'type': 'stock', 'from': int(d.timestamp()), 'to': int(today.timestamp()), 'resolution':'D'}
    try:
        response = requests.get(priceURL, params=priceParams)
        price = pd.DataFrame(response.json()['data'])
        price['date'] = pd.to_datetime(price["tradingDate"], format='%Y-%m-%dT%H:%M:%S.000Z')
        price['ticker'] = ticker
        list_col = ['open', 'high', 'low', 'close', 'volume', 'date', 'ticker']
    except:
        print(f"{ticker} has no price board")
        continue

    try:
        foreignVolParams = {'ticker': ticker}
        response = requests.get(foreignVolURL, params=foreignVolParams)

        foreignVol = pd.DataFrame(response.json()['listVolumeForeignInfoDto'])
        foreignVol['date'] = pd.to_datetime(foreignVol["dateReport"], format='%d/%m/%Y')
        price = price.merge(foreignVol, on=['date', 'ticker'], how='left')

        list_col.append('foreignBuy')
        list_col.append('foreignSell')
    except:
        print(f"{ticker} has no foreign transaction")
    
    try:
        bsaURL = f"https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{ticker}/bsa-month?timeWindow=1M&type=all"
        response = requests.get(bsaURL)
        bsa = pd.DataFrame(response.json()['data'])
        bsa['date'] = bsa.t.apply(lambda x: pd.to_datetime(x + "/2023", format="%d/%m/%Y"))
        
        price = price.merge(bsa, on='date', how='left')
        price = price.drop_duplicates(subset=['ticker', 'date'])
        list_col.append('bsr')
    except:
        print(f"{ticker} has no bsr")
    price[list_col].to_sql(con=engine, name='price_board', if_exists='append', index=False)
    print(ticker, 'DONE!!!')