import requests
from scripts.utils.helper import *
from scripts.utils.config import dbInfor
import numpy as np
import pandas as pd
import pymysql
import sqlalchemy
import sys
from datetime import datetime, timedelta

def push_price_data():
    con = get_mysql_connection()
    cursor = con.cursor()
    today = datetime.today()
    yesterday = today - timedelta(days=1)

    sql = f"select ticker, max(date) as date, close from price_board group by ticker"
    pre_data = get_data_from_mysql(con, sql)
    pre_data = pre_data.dropna()
    insert_sql = "insert into price_board(ticker, date, open, high, low, close, volume, ba, sa, total_transaction) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    for id, row in pre_data.iterrows():
        if row['date'].date() != yesterday.date():
            continue
        ticker = row['ticker']
        open = row['close']
        intradayURL = f"https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{ticker}/his/paging"
        params = {"page": 0, "size": 1000}
        try:
            response = requests.get(intradayURL, params=params)
        except:
            continue
        res = response.json()
        intraday = pd.DataFrame(res['data'])

        if len(intraday) == 0:
            value = (ticker, str(today.date()), open, open, open, open, 0, 0, 0, 0)
            cursor.execute(insert_sql, value)
            con.commit()
            continue

        if res['total'] > 1000:
            params = {"page": 0, "size": res['total']}
            response = requests.get(intradayURL, params=params)
            res = response.json()
            intraday = pd.DataFrame(res['data'])
        
        close = intraday.iloc[0]['p']
        low = intraday['p'].min()
        high = intraday['p'].max()
        volumne = intraday['v'].sum()
        intraday['transaction'] = intraday['v'] * intraday['p']
        total_transaction = intraday['transaction'].sum()
        ba = intraday[intraday['a'] == 'BU']['transaction'].sum()
        sa = intraday[intraday['a'] == 'SD']['transaction'].sum()
        value = (ticker, str(today.date()), open, high, low, close, volumne, ba, sa, total_transaction)
        cursor.execute(insert_sql, value)
        con.commit()
        print(ticker, "DONE!!!")
    con.close()