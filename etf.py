from typing import Dict

import requests
import pandas as pd
import json

pd.set_option('display.max_rows', None)
# https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities/GLDRUB_TOM.json?iss.meta=off&iss.only=marketdata
# https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities/GLDRUB_TOM/candles.json?from=2024-01-08&till=2024-03=20&interval=60

# /iss/engines/[engine]/markets/[market]/boards/[board]/securities/[security]/candles

#"https://iss.moex.com/iss/engines/stock/markets/shares/securities/SBMX.json?iss.meta=off"


total_sum = 490_000
data_dir = "N:\\KurzanovAV\\Только для чтения\\"

def read_portfolio()->pd.DataFrame:
    df = pd.read_csv(data_dir+"etf.csv", sep=";", encoding="Windows-1251")
    return df

def read_params_json()->Dict:
    with open(data_dir+"etf.json", encoding='utf-8') as jsn:
        data = json.load(jsn)[0]
    return data

def read_index(index_name: str)->pd.DataFrame:
    return pd.read_csv(data_dir+index_name+".csv", sep=";", encoding="Windows-1251")


def get_price(ticker):
    pass



def sec_price(etf_portfolio: pd.DataFrame):
    '''
    :param etf_portfolio:
    :param etf_params:
    :return:
    '''
    print(etf_portfolio)
    etf_portfolio["url"] = "https://iss.moex.com/iss/engines/" + etf_portfolio['engines'] +\
                           "/markets/" + etf_portfolio['markets'] +\
                           "/boards/" + etf_portfolio['boards'] +\
                           "/securities/" + etf_portfolio['ticker'] + ".json"
    for item in etf_portfolio["url"]:
        print(item)
    etf_portfolio["price"] = etf_portfolio["url"].apply(
        lambda x: requests.get(x, params={
                "iss.meta": "off",
                "marketdata.columns": "SECID,BID"
                }
            ).json()["marketdata"]["data"][0][2]
    )
    print(etf_portfolio["price"])




    print(etf_portfolio)


print(total_sum)
portfolio = read_portfolio()
etf_params = read_params_json()
df2 = read_index("imoex")
sec_price(portfolio)

