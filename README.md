# NASDAQ_visualisations

## Table of Contents

- [Project Brief](#project-brief)
- [Data](#data)
- [Project Pipeline](#project-pipeline)


## Project Brief

This projects aims to teach all individuals about the stock market through the use of the worlds largest non financial companies (NASDAQ-100). This is achieved by creating a website within streamlit that displays a variety of insightful informations about these companies. The financial world is a large industry which everyone interacts with in their life although, not everyone understands,  therefore a platform which teachs about it of great benefit.


## Data

The source of all the data in this project is from the open source tool [yfinance](https://pypi.org/project/yfinance/). This open source tool uses Yahoo's pubically available API's to provide data on a wide variety of stocks. All the companies used in this project can be found on [this](https://stockanalysis.com/list/nasdaq-100-stocks/) website where the company tickers were webscrapped.

The data on yfinance is exported to a postgreSQL database with the use of python scripts where 102 tables sit. Out of those 102 tables, 101 contain hourly historical data about each respective company, the data is from 2024-01-02 and is updated every 12 hours.

### Company stock data columns:

| datetime | open | high | low | close | volume | dividends |
| -------- | ---- | ---- | --- | ----- | ------ |---------- |

The other table contains information about the market captialization of each company as well other relating information.

### Company info columns:

| ticker | name | market_cap | sector | industry | market_cap_billions |
| ------ | ---- | ---------- | ------ | -------- | --------------------|


## Project Pipeline

![Alt Text](./project_pipeline.png)




<!-- ``` Python
url = 'https://stockanalysis.com/list/nasdaq-100-stocks/'
headers ={
    'User_Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}
nas_res = requests.get(url, headers=headers)
NDQ = BeautifulSoup(nas_res.text, 'html.parser')

tickers = [i.text for i in NDQ.findAll('td', class_="sym svelte-eurwtr")]
comp_name = [i.text for i in NDQ.findAll('td', class_="slw svelte-eurwtr")]

NASDAQ = dict(zip(tickers,comp_name))
``` -->