import requests
from scripts.utils.helper import *
from scripts.utils.config import dbInfor
import numpy as np
import pandas as pd
import pymysql
import sqlalchemy
import sys
from datetime import datetime, timedelta

con = get_mysql_connection()

list_code = {
    "title": ["VN-INDEX","HNX-INDEX", "Dow Jones", "DAX", "FTSE 100", "NI225", "KOSPI", "Shanghai",
                "Crude Oil (USD/lb)", "Gold (USD/t oz)", "Natural Gas (USD/Mmbtu)", "Steel (USD/lb)", "Coal (USD/lb)", "Rubber (SGD/kg)", "Sugar (USD/lb)", "Cotton (USD/lb)",
                "Bitcoin", "Dollar Index", "USD/VND", "USD/CNY", "EUR/USD", "USD/RUB", "GBP/USD", "USD/JPY"],
    "code": ["VNINDEX", "HNX", "dji", "gdaxi", "ftse", "n225", "ks11", "ssec", 
                "wti_usd", "xau_usd", "ngu1", "srbv1", "ncfmc1", "stfc1", "sbv1", "ctz1", 
                "btc_usd", "dx", "usd_vnd", "usd_cny", "eur_usd", "usd_rub", "gbp_usd", "usd_jpy"],
    "type": ["ticker", "ticker", "ticker", "ticker", "ticker", "ticker", "ticker", "ticker", 
            "good", "good", "good", "good", "good", "good", "good", "good",
            "money", "money", "money", "money", "money", "money", "money", "money"]
}

market_overview = pd.DataFrame(list_code)

url = "https://wichart.vn/wichartapi/wichart/chartthitruong"
all_df = []

for id, row in market_overview.iterrows():
    params = {'code': row['code']}
    response = requests.get(url, params=params)
    list_value = response.json()["listClose"]
    df = pd.DataFrame(list_value, columns=["date", "value"])
    df["date"] = df.date.apply(lambda x: convert_datetime(x))
    df['code'] = row['code']
    sql = f"select max(date) as date from market_overview where code = '{row['code']}'"
    newest_date = get_data_from_mysql(con, sql).iloc[0]['date']
    df = df[df['date'] > newest_date]
    all_df.append(df)

all_df = pd.concat(all_df)
market_overview = market_overview.merge(all_df, on='code')

con.close()

engine = create_mysql_engine()
if len(market_overview) > 0:
    market_overview.to_sql(con=engine, name='market_overview', if_exists='append', index=False)
