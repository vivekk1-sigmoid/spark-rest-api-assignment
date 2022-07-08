import requests
import json
import pandas as pd

url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"

headers = {
    "X-RapidAPI-Key": "2fdca71074msh2ce0dc9321591d1p1f71ebjsne303cd02e85",
    "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
}


def get_stock_data(company_name):
    querystring = {"ticker_symbol": company_name, "years": "5", "format": "json"}
    response = requests.request("GET", url, headers=headers, params=querystring)

    json_data = json.loads(response.text)
    stock_details = json_data['historical prices']
    for detail in stock_details:
        date_data = detail['Date'].split('T')[0]
        detail['Date'] = date_data
    stock_details_df = pd.DataFrame(stock_details)
    stock_details_df['company'] = company_name
    stock_file_path_name = "../data/" + company_name + ".csv"
    stock_details_df.to_csv(stock_file_path_name)


def get_company_name():
    list_of_companies = []
    url = "https://stock-market-data.p.rapidapi.com/market/index/s-and-p-one-hundred"
    response = requests.request("GET", url, headers=headers)
    json_data = json.loads(response.text)
    for company in json_data['stocks']:
        list_of_companies.append(company)
    return list_of_companies


if __name__ == '__main__':
    company_list = ["AAPL", "GOOG", "IBM", "META", "NFLX"]
    company_list = get_company_name()
    for company in company_list[:10]:
        get_stock_data(company)
