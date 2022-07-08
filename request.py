import pandas as pd
import requests
import json
import sys
import os

sys.path.append('../')


def get_companies(url, headers):
    response = requests.request("GET", url, headers=headers)
    response_dict = dict(json.loads(response.text))
    return response_dict['stocks']


def get_response(url, headers, company, years):
    querystring = {"ticker_symbol": company, "years": years, "format": "json"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    response_dict = dict(json.loads(response.text))
    return response_dict


def get_cvs_data(response_dict, company):
    df = pd.DataFrame(response_dict['historical prices'])
    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'].dt.date
    df['stock_name'] = company
    file_name = 'csv_data/' + company + '.csv'
    df.to_csv(file_name)


def main():
    url = ["https://stock-market-data.p.rapidapi.com/market/index/nasdaq-one-hundred",
           "https://stock-market-data.p.rapidapi.com/stock/historical-prices"]

    headers = {
        "X-RapidAPI-Key": "5d149f0f70mshcf5bb7fa5f98823p183bfejsn5ac83a37517c",
        "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
    }

    if not os.path.exists('companies.txt'):
        companies = get_companies(url[0], headers)
        companies = [i + '\n' for i in companies]
        with open('companies.txt', 'w') as f:
            f.writelines(companies)

    with open('companies.txt', 'r') as f:
        companies = f.readlines()
    companies = [i.rstrip('\n') for i in companies]
    print(companies)
    for company in companies[:25]:
        response_data = get_response(url[1], headers, company, 1)
        get_cvs_data(response_data, company)


if __name__ == "__main__":
    main()
