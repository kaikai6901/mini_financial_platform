import requests
from scripts.utils.helper import *
from scripts.utils.config import dbInfor
import numpy as np
import pandas as pd
import pymysql
import sqlalchemy
import sys
from datetime import datetime, timedelta

def push_financial_ratio():
    conn = get_mysql_connection()

    sql = "select ticker, max(year) as year, max(quarter) as quarter from financial_ratio group by ticker"
    latest_date = get_data_from_mysql(conn, sql)
    conn.close()

    today = datetime.today() - timedelta(days=90)
    year = today.year
    quarter = today.month % 4 + 1

    list_ratio = []
    for id, row in latest_date.iterrows():
        ticker = row['ticker']
        if row['year'] >= year and row['quarter'] > quarter:
            continue
        financial_ratioURL = f'https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{ticker}/financialratio?yearly=0&isAll=false'
        response = requests.get(financial_ratioURL)
        ratio = pd.DataFrame(response.json())

        if len(ratio) > 0:
            ratio = ratio[ratio['year'] >= year]
            ratio = ratio[ratio['quarter'] > quarter]
            ratio = ratio.drop_duplicates(subset=['ticker', 'year', 'quarter'])
            list_ratio.append(ratio)
        
        financial_ratioURL = f'https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{ticker}/financialratio?yearly=1&isAll=false'
        response = requests.get(financial_ratioURL)
        ratio_year = pd.DataFrame(response.json())

        if len(ratio_year) > 0:
            ratio_year = ratio_year[ratio_year['year'] >= year]
            ratio_year = ratio_year[ratio_year['quarter'] > quarter]
            ratio_year = ratio_year.drop_duplicates(subset=['ticker', 'year', 'quarter'])
            list_ratio.append(ratio_year)
        print(id, ticker)
    
    data = pd.concat(list_ratio)
    engine = create_mysql_engine()
    data[['ticker', 'quarter', 'year', 'priceToEarning', 'priceToBook',
        'valueBeforeEbitda', 'roe', 'roa', 'earningPerShare',
        'bookValuePerShare', 'equityOnTotalAsset', 'grossProfitMargin', 'operatingProfitMargin',
        'postTaxMargin', 'debtOnEquity', 'debtOnAsset', 'debtOnEbitda',
        'shortOnLongDebt', 'assetOnEquity', 'capitalBalance', 'cashOnEquity',
        'cashOnCapitalize', 'revenueOnAsset']].to_sql(con=engine, name='financial_ratio', if_exists='append', index=False)