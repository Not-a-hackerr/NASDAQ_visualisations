import psycopg2
import requests
from bs4 import BeautifulSoup
import yfinance as yf
from datetime import timedelta
import psycopg2.extras as extras
import streamlit as st
import pandas as pd

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

SQL_User = st.secrets['SQLUser']
SQL_Pass = st.secrets['SQLPass']
Host = st.secrets['Host']


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


def replace_table(conn, table_ticker, df):
    cursor = conn.cursor()

    drop_sql = f"DROP TABLE IF EXISTS student.nm_{table_ticker};"
    cursor.execute(drop_sql)
    conn.commit()

    create_sql = f"""
    CREATE TABLE student.nm_{table_ticker} ( 
        ticker VARCHAR(10), 
        name VARCHAR(50),
        market_cap BIGINT,
        sector VARCHAR(50), 
        industry VARCHAR(50), 
        market_cap_billions REAL
    );
    """
    cursor.execute(create_sql)
    conn.commit()

    return updated_db(conn, df, f'student.nm_{table_ticker}')

def get_company_info(ticker_symbol):
    stock = yf.Ticker(ticker_symbol)
    info = stock.info
    return {
        'ticker': ticker_symbol,
        'name': info.get('longName'),
        'market_cap': info.get('marketCap'),
        'sector': info.get('sector'),
        'industry': info.get('industry')
    }


conn = psycopg2.connect(database='pagila',
                        user= SQL_User,
                        host= Host,
                        password= SQL_Pass,
                        port=5432
)
    
if __name__ =="__main__":
    company_data = [get_company_info(ticker) for ticker in NASDAQ]
    df = pd.DataFrame(company_data)
    df['market_cap_billions'] = df['market_cap'] / 1e9
    replace_table(conn, 'nasdaq_info', df)

