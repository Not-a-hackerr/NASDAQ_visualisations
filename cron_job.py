import os
from dotenv import load_dotenv
import psycopg2
import requests
from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd
from datetime import timedelta
import psycopg2.extras as extras

# Web Scrap to get all 100 NASDAQ companies and their corresponding tickers
url = 'https://stockanalysis.com/list/nasdaq-100-stocks/'
headers ={
    'User_Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}
nas_res = requests.get(url, headers=headers)
NDQ = BeautifulSoup(nas_res.text, 'html.parser')

tickers = [i.text for i in NDQ.findAll('td', class_="sym svelte-eurwtr")]
comp_name = [i.text for i in NDQ.findAll('td', class_="slw svelte-eurwtr")]

NASDAQ = dict(zip(tickers,comp_name))


load_dotenv()
SQL_User = os.getenv('SQLUser')
SQL_Pass = os.getenv('SQLPass')
Host = os.getenv('Host')


def fetch_new_data(ticker, last_retrieval_datetime):
    start_datetime = last_retrieval_datetime + timedelta(hours=1)
    
    
    ticker_obj = yf.Ticker(ticker)
    new_data = ticker_obj.history(start=start_datetime, interval='1h')
    return new_data

def last_updated():

    conn = psycopg2.connect(database='pagila',
                        user= SQL_User,
                        host= Host,
                        password= SQL_Pass,
                        port=5432)
    
    cur = conn.cursor()
    
    sql = """
        SELECT 
            datetime
        FROM
            student.nm_MSFT
        ORDER BY
            datetime DESC
        LIMIT 
            1;
        """
    
    cur.execute(sql)
    rows = cur.fetchall()
    return rows[0][0]

def cleaning_data(data):
    data.reset_index(inplace=True)
    # data.drop('Stock Splits', axis=1, inplace=True)
    return data

def updated_db(conn, df, ticker): 
  
    tuples = [tuple(x) for x in df.to_numpy()] 
  
    cols = ','.join(list(df.columns)) 
    query = "INSERT INTO %s(%s) VALUES %%s" % (f'student.nm_{ticker}', cols) 
    cursor = conn.cursor() 
    try: 
        extras.execute_values(cursor, query, tuples) 
        conn.commit() 
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error: %s" % error) 
        conn.rollback() 
        cursor.close() 
        return 1
    print(f"the table stundent.nm_{ticker} was updated") 
    cursor.close() 

def update_db_new_value(ticker, last_retrieval_time):

    conn = psycopg2.connect(database='pagila',
                        user= SQL_User,
                        host= Host,
                        password= SQL_Pass,
                        port=5432)
    
    new_data = fetch_new_data(ticker, last_retrieval_time)
    new_data = cleaning_data(new_data)
    
    return updated_db(conn, new_data, ticker)
    
if __name__ =="__main__":
    last_time_updated = last_updated()
    for i in NASDAQ:
        update_db_new_value(i, last_time_updated)



