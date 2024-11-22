from typing import Dict

import requests
import pandas as pd
import json

pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', 15)
# https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities/GLDRUB_TOM.json?iss.meta=off&iss.only=marketdata
# https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities/GLDRUB_TOM/candles.json?from=2024-01-08&till=2024-03=20&interval=60

# /iss/engines/[engine]/markets/[market]/boards/[board]/securities/[security]/candles

# "https://iss.moex.com/iss/engines/stock/markets/shares/securities/SBMX.json?iss.meta=off"


total_sum = 486_000
data_dir = "N:\\KurzanovAV\\Только для чтения\\"


def read_etf() -> pd.DataFrame:
    df = pd.read_csv(data_dir + "etf.csv", sep=";", encoding="Windows-1251")
    return df


def read_index(index_name: str) -> pd.DataFrame:
    return pd.read_csv(data_dir + index_name + ".csv", sep=";", encoding="Windows-1251")


def get_etf_data(df: pd.DataFrame) -> Dict:
    '''
    :param df:
    :return:
    '''
    url = "https://iss.moex.com/iss/engines/" + df['engines'] + \
          "/markets/" + df['markets'] + \
          "/boards/" + df['boards'] + \
          "/securities/" + df['ticker'] + ".json"
    resp = requests.get(url, params={"iss.meta": "off", "marketdata.columns": "SECID,BID"})
    return resp.json()["marketdata"]["data"][0][1]


def get_index_data(df: pd.DataFrame) -> pd.DataFrame:
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{df['ticker']}.json"
    r = requests.get(url, params={
        "iss.meta": "off",
        "marketdata.columns": "SECID,LAST"
    }
                     )
    if r.json()['marketdata']['data'][0][1]:
        return r.json()['marketdata']['data'][0][1]
    else:
        return r.json()["securities"]["data"][0][3]

def get_index_sec_shortnames(df: pd.DataFrame) -> pd.DataFrame:
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{df['ticker']}.json"
    r = requests.get(url, params={
            "iss.meta": "off",
            "marketdata.columns": "SECID,LAST"
        }
    )
    return r.json()["securities"]["data"][0][2]


def index_data(index: str, etf: pd.DataFrame) -> pd.DataFrame:
    '''
    :param index: Название индекса
    :param etf: DataFrame с ETG
    :return: DataFrame со структурой ценных бумаг в индексе
    '''
    df = read_index(index)
    df['shortname'] = df.apply(lambda x: get_index_sec_shortnames(x), axis=1)
    df['price'] = df.apply(lambda x: get_index_data(x), axis=1)
    total = etf.groupby('base')['actual_amount'].sum()[index]
    df["sum_by_seq"] = total * 0.95 * df["weight"]
    df["volume"] = df["sum_by_seq"] / df["price"]
    df["weight"] = df["weight"] * 100
    return df[["shortname", "weight", "price", "sum_by_seq", "volume"]].\
        sort_values(by='sum_by_seq', ascending=False)


def etf_data(df: pd.DataFrame):
    '''
    :param df: Датафрэйм портфеля etf
    :return:
    '''
    df["price"] = df.apply(lambda x: get_etf_data(x), axis=1)
    df["actual_amount"] = df["volume"] * df["price"]
    df["plan_amount"] = total_sum * df["percent"] / 100
    df["plan_volume"] = (df["plan_amount"] / df["price"]).astype(int)
    df[["ticker", "category", "percent", "plan_amount", "plan_volume", "volume", "actual_amount", "price"]]

    return df


def etf_data_grouped(df: pd.DataFrame) -> pd.DataFrame:
    return df[["category", "percent", "plan_amount", "actual_amount"]].groupby(["category"]).agg(
        {
            "percent": "sum",
            "plan_amount": "sum",
            "actual_amount": "sum"
        }
    ).reset_index()

def sum_actual_by_index(index: str, etf: pd.DataFrame):
    return etf.groupby('base')['actual_amount'].sum()[index]

def print_portfolio_by_index(index: str) -> None:
    print("=" * 50)
    print("Сумма по индексу "+index+": ", sum_actual_by_index(index, df_etf))
    print("=" * 50)
    print(index_data(index, df_etf))


print("Общая сумма портфеля:", total_sum)
etf = read_etf()
df_etf = etf_data(etf)
print(df_etf[["ticker", "percent", "plan_amount", "plan_volume", "volume", "price", "actual_amount"]])
print(etf_data_grouped(df_etf))
print("+"*50)
print_portfolio_by_index("IMOEX")
print_portfolio_by_index("MCXSM")

