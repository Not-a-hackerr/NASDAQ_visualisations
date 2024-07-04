import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import streamlit as st
import plotly.express as px
from streamlit_extras.stateful_button import button
from cron_job import NASDAQ
import plotly.graph_objects as go


load_dotenv()
SQL_User = os.getenv('SQLUser')
SQL_Pass = os.getenv('SQLPass')
Host = os.getenv('Host')

conn = psycopg2.connect(database='pagila',
                        user= SQL_User,
                        host= Host,
                        password= SQL_Pass,
                        port=5432)


def get_columns(ticker):
    ticker = ticker.lower()
    cur = conn.cursor()
    sql = f"""
        SELECT 
            column_name
        FROM
            information_schema.COLUMNS
        WHERE 
            table_schema = 'student'
            AND table_name = 'nm_{ticker}'
        """
    cur.execute(sql)
    rows = cur.fetchall()
    columns = [i[0] for i in rows]
    return columns


def get_data_from_db(ticker):
    ticker = ticker.lower()
    cur = conn.cursor()
    sql = f"""
        SELECT
            *
        FROM
            student.nm_{ticker}
        """
    
    cur.execute(sql)
    rows = cur.fetchall()
    df = pd.DataFrame(columns=get_columns(ticker))
    num = 0
    for i in rows:
        df.loc[num] = i
        num += 1 
    
    return df


def format_int_with_commas(x):
    return f'{x:,}'

# Values / dfs commonly used
nasdaq_info = get_data_from_db('nasdaq_info')
NASDAQ_tickers = list(NASDAQ.keys())


def display_market_cap():
    st.header('Market Capitalization Comparison')

    with st.container():
        with st.expander('What is market capitalisation?'):
            st.write("""The value of a company that is traded on the stock market,
                      calculated by multiplying the total number of shares by the 
                     present share price. Many people misinterpret this figure and 
                     take it as the price of the company which is far from the case.""")

        col1, col2= st.columns(2, vertical_alignment="center")
        with col1:
            sect_but = button("Sector", key='button1',use_container_width=True)
        
        with col2:
            ind_but = button("Industry", key='button2',use_container_width=True)
        
        if 'cate_compare' not in st.session_state:
            st.session_state['cate_compare'] = [px.Constant('NASDAQ'), 'name']


        if sect_but:
            if 'sector' not in st.session_state['cate_compare']:
                st.session_state['cate_compare'].insert(1, 'sector')
        else:
            if 'sector' in st.session_state.cate_compare:
                st.session_state['cate_compare'].remove('sector')
            

        if ind_but:
            if 'industry' not in st.session_state['cate_compare']:
                st.session_state['cate_compare'].insert(-1, 'industry')
        else:
            if 'industry' in st.session_state.cate_compare:
                st.session_state['cate_compare'].remove('industry')

        fig = px.treemap(data_frame=nasdaq_info, path=st.session_state['cate_compare'],
                    values='market_cap',
                    labels={'labels': 'Label', 'market_cap':'Market Cap', 'parent':'Parent'})
        fig

    st.markdown('#### Find the Market Capitalization of a Company')
    lef, rig= st.columns(2)
    with lef:
        stock_mark = st.selectbox('Stock', options=nasdaq_info['name'],key='stock')

    with rig:
        stock_mark_cap = nasdaq_info.loc[nasdaq_info['name']==stock_mark][['market_cap']].map(format_int_with_commas)
        st.metric(label=f'{stock_mark} Market Capitalization',value=f"$ {stock_mark_cap.values[0][0]}")

    with st.expander('"What Number is that" is what you\'re probably thinking, because I was'):
        
        commas_num = str(stock_mark_cap.values[0][0]).count(',')
        num_size = {3:'billions', 4:'trillions'}
        st.write(f"little hint: That number is in the {num_size[commas_num]}")


def display_stock_sharing(start_date, end_date):
    tab1,tab2 = st.tabs(['Metrics', 'Graphs'])
    with tab1:
        def get_last_date_last_month():
            dates = get_data_from_db(NASDAQ_tickers[1])['datetime']
            this_month = dates.iloc[dates.size-1].month
            last_month = []
            for i in range(dates.size):
                if (dates.iloc[i].month == this_month - 1) and (dates.iloc[i].day >= 27) :
                    last_month.append(dates.iloc[i])

            return last_month[-1]
        

        c1, c2 = st.columns(2)
        with c1:
            st.metric(label='Latest Price Today', value=f"$ {stock_1_data.iloc[len(stock_1_data) - 1]['close']}")
            st.metric(label='Last Month Last Day', value=f"$ {stock_1_data.loc[stock_1_data['datetime']==get_last_date_last_month()]['close'].values[0]}")
            st.metric(label='Beginning of year', value=f"$ {stock_1_data.iloc[0]['close']}")
        
        with c2:
            st.metric(label='Latest Price Today', value=f"$ {stock_2_data.iloc[len(stock_2_data) - 1]['close']}")
            st.metric(label='Last Month Last Day', value=f"$ {stock_2_data.loc[stock_2_data['datetime']==get_last_date_last_month()]['close'].values[0]}")
            st.metric(label='Beginning of year', value=f"$ {stock_2_data.iloc[0]['close']}")

    with tab2:
        
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)


        stock_between_stock_1 = stock_1_data[stock_1_data['datetime'].between(start_date,end_date)]
        stock_between_stock_2 = stock_2_data[stock_2_data['datetime'].between(start_date,end_date)]

        merged_df = pd.merge(stock_between_stock_1[['datetime', 'close']], stock_between_stock_2[['datetime', 'close']], on='datetime', suffixes=('_company1', '_company2'))


        st.title('Stock Prices Comparison')

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=merged_df['datetime'], 
            y=merged_df['close_company1'], 
            mode='lines', 
            name=f'{stock1}'
        ))

        fig.add_trace(go.Scatter(
            x=merged_df['datetime'], 
            y=merged_df['close_company2'], 
            mode='lines', 
            name=f'{stock2}'
        ))

        fig.update_layout(
            title='Stock Prices Comparison',
            xaxis_title='Date',
            yaxis_title='Stock Price ($)',
            legend_title='Companies',
            template='plotly_white'
        )

        st.plotly_chart(fig)


st.title('Explore the NASDAQ Index 100 Companies',)

st.subheader('What is the NASDAQ?')
st.write("""Nasdaq is a global electronic marketplace for buying and selling securities. Its name was originally an acronym for the National Association of Securities Dealers Automated Quotations.""")

st.subheader("NASDAQ Index 100?")
st.write('The Nasdaq-100 is a stock market index made up of equity securities issued by 100 of the largest non-financial companies listed on the Nasdaq stock exchange.')

st.subheader('Comparing Stock Prices')

with st.form('Stock Selection'):
    left, right = st.columns(2)  
    with left:
        st.markdown('##### Choose First Stock')
        stock1 = st.selectbox('Select a company', options=nasdaq_info['name'],key='stock1',index=0)
        st.markdown('##### Choose Second Stock')
        stock2 = st.selectbox('Select a company', options=nasdaq_info['name'],key='stock2',index=1)

        stock_1_data = get_data_from_db(nasdaq_info.loc[nasdaq_info['name']==stock1]['ticker'].values[0])
        stock_2_data = get_data_from_db(nasdaq_info.loc[nasdaq_info['name']==stock2]['ticker'].values[0])

    with right:
        st.markdown('##### Choose Start Date')
        start_date = st.date_input(label='select a date', key='date1', min_value=stock_1_data['datetime'][0], max_value=stock_1_data.iloc[len(stock_1_data)-20]['datetime'], value=stock_1_data['datetime'][0])
        
        st.markdown('##### Choose End Date')
        end_date =  st.date_input(label='select a date', key='date2', min_value=stock_1_data['datetime'][0], max_value=stock_1_data.iloc[len(stock_1_data)-20]['datetime'], value=stock_1_data.iloc[len(stock_1_data)-20]['datetime'])

    compare_button = st.form_submit_button('Compare', type='primary')

if start_date > end_date:
    st.error("These dates don't work. The start date must come before the end date.")
elif stock1 == stock2:
    st.error("Please reselect two different Companies")
else:
    display_stock_sharing(start_date, end_date)

st.divider()

display_market_cap()

