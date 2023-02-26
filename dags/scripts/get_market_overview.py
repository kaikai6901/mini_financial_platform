import requests
from scripts.utils.helper import *
from scripts.utils.config import dbInfor
import numpy as np
import pandas as pd
import pymysql
import sqlalchemy
import sys
from datetime import datetime, timedelta

def push_data():
    con = get_mysql_connection()

    priceURL = "https://wichart.vn/wichartapi/wichart/price"
    list_type = [1, 2, 3]
    type_code = {1: "ticker", 2: "good", 3: "money"}
    for value in list_type:
        params = {"type": value}
        response = requests.get(priceURL, params=params)
        data = pd.DataFrame(response.json())
        data["type"] = type_code[value]
        data["date"] = str(datetime.today().date())
        for id, row in data.iterrows():
            if row['value'] != None:
                deletedQuery = f"delete from market_overview where date = '{row['date']}' and title = '{row['title']}'"
                run_sql(con, deletedQuery)
                insertQuery = f"insert into market_overview (title, code, value, type, date) values ('{row['title']}', '{row['code']}', {row['value']}, '{row['type']}', '{row['date']}')"
                run_sql(con, insertQuery)
    print("DONE!!!")
    con.close()