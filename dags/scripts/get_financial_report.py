import requests
from scripts.utils.helper import *
from scripts.utils.config import dbInfor
import numpy as np
import pandas as pd
import pymysql
import sqlalchemy
import sys
from datetime import datetime, timedelta

list_report_type = ['balancesheet', 'incomestatement', 'cashflow']
list_table_name = ['balance_sheet', 'income_statement', 'cash_flow']

list_field = {
    "balancesheet": ['ticker', 'year', 'quarter', 'shortAsset', 'cash', 'shortInvest',
       'shortReceivable', 'inventory', 'longAsset', 'fixedAsset', 'asset',
       'debt', 'shortDebt', 'longDebt', 'equity', 'capital', 'otherDebt', 'payable'],
    "incomestatement": ['ticker', 'quarter', 'year', 'revenue', 'yearRevenueGrowth',
       'quarterRevenueGrowth', 'costOfGoodSold', 'grossProfit',
       'operationExpense', 'operationProfit', 'yearOperationProfitGrowth',
       'quarterOperationProfitGrowth', 'interestExpense', 'preTaxProfit',
       'postTaxProfit', 'shareHolderIncome', 'yearShareHolderIncomeGrowth',
       'quarterShareHolderIncomeGrowth', 'ebitda']
}

def push_financial_report():

    engine = create_mysql_engine()

    today = datetime.today() - timedelta(days=90)
    year = today.year
    quarter = today.month % 4 + 1

    for i in range(3):
        report_type = list_report_type[i]
        table_name = list_table_name[i]

        conn = get_mysql_connection()
        sql = f"select ticker, max(year) as year, max(quarter) as quarter from {table_name} group by ticker"
        latest_date = get_data_from_mysql(conn, sql)
        conn.close()

        list_report = []
        for id, row in latest_date.iterrows():
            ticker = row['ticker']
            if row['year'] >= year and row['quarter'] > quarter:
                continue
            url = f"https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{ticker}/{report_type}?yearly=0&isAll=false"
            response = requests.get(url)
            if len(report) > 0:
                report = pd.DataFrame(response.json())
                report = report[report['year'] >= year]
                report = report[report['quarter'] > quarter]
                report = report.drop_duplicates(subset=["ticker", "quarter", 'year'])
                list_report.append(report)


            url = f"https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{ticker}/{report_type}?yearly=1&isAll=false"
            response = requests.get(url)
            report_year = pd.DataFrame(response.json())
            if len(report_year) > 0:
                report_year = report_year[report_year['year'] >= year]
                report_year = report_year[report_year['quarter'] > quarter]
                report_year = report_year.drop_duplicates(subset=["ticker", "quarter", 'year'])
                list_report.append(report_year)
            
            print(id, ticker)
        all_report = pd.concat(list_report)
        if report_type in list_field:
            all_report = all_report[list_field[report_type]]
        all_report.to_sql(con=engine, name=table_name, if_exists='append', index=False)

        print(report_type, "DONE!!!!")