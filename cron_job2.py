import psycopg2
from psycopg2 import extras
import pandas as pd
import yfinance as yf
import os
from dotenv import load_dotenv
from cron_job import NASDAQ


load_dotenv()
SQL_User = os.getenv('SQLUser')
SQL_Pass = os.getenv('SQLPass')
Host = os.getenv('Host')

def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    
    query = f"INSERT INTO {table}({cols}) VALUES %s"
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error:", error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()

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

    return execute_values(conn, df, f'student.nm_{table_ticker}')

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

company_data = [get_company_info(ticker) for ticker in NASDAQ]
df = pd.DataFrame(company_data)
df['market_cap_billions'] = df['market_cap'] / 1e9

conn = psycopg2.connect(database='pagila',
                        user= SQL_User,
                        host= Host,
                        password= SQL_Pass,
                        port=5432
)


if __name__ =="__main__":
    replace_table(conn, 'nasdaq_info', df)

