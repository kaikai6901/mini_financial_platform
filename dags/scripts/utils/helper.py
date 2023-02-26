from scripts.utils.config import *
from sqlalchemy import create_engine
from datetime import datetime
import pymysql
import pandas as pd

def create_mysql_engine():
    try:
        conn = create_engine(f"""{dbInfor["driver"]}://{dbInfor["user"]}:{dbInfor["pass"]}@{dbInfor["host"]}/{dbInfor["db"]}""")
        return conn
    except Exception as e:
        print(e)
        return None

def convert_datetime(timestamp):
    try:
        if isinstance(timestamp, int) or isinstance(timestamp, float):
            if timestamp > 1e11:
                return datetime.fromtimestamp(timestamp/1e3)
            else:
                return datetime.fromtimestamp(timestamp)
        else:
            return datetime.strptime(timestamp, "%Y-%m-%d")
    except:
        print("Invalid Timestamp")

def get_mysql_connection():
    try:
        conn = pymysql.connect(
            host= dbInfor['host'],
            user=dbInfor['user'], 
            password = dbInfor['pass'],
            db=dbInfor['db'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
            )
        return conn
    except:
        print("Failed to connect")
        return None

def get_data_from_mysql(con, query):
    try:
        dbCursor = con.cursor()
        dbCursor.execute(query)
        result = dbCursor.fetchall()
        return pd.DataFrame(result)
    except Exception as e:
        print(e)

def run_sql(con, query):
    try:
        dbCursor = con.cursor()
        dbCursor.execute(query)
        con.commit()
    except Exception as e:
        print(e)

def delete_by_ticker(con, table, ticker, date):
    try:
        query = f"delete from {table} where ticker = '{ticker}' and date = '{date}'"
        run_sql(con, query)
    except Exception as e:
        print(e)
    return
    
def delete_by_date(con, table, date):
    try:
        query = f"delete from {table} where date = '{date}''"
        run_sql(con, query)
    except Exception as e:
        print(e)
    return

def get_newest_row(con, table, ticker):
    try:
        query = f"select ticker, max(date) from {table} where ticker = '{ticker}'"
        dbCursor = con.cursor()
        dbCursor.execute(query)
        result = dbCursor.fetchall()
        return pd.DataFrame(result)
    except Exception as e:
        print(e)
    return None

def get_price_before(con, ticker, day):
    try:
        query = f"select ticker, max(date) as date, close from vnstock_resources.price_board where date < '{day}' and ticker = '{ticker}'"
        dbCursor = con.cursor()
        dbCursor.execute(query)
        result = dbCursor.fetchall()
        return pd.DataFrame(result)
    except Exception as e:
        print(e)
    return None
