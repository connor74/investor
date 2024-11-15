import requests
import pandas as pd
import matplotlib.pyplot as plt

#url = "http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/SU52003RMFS9.json?from=2024-01-01&till=2024-02-20"




def read_report(file_name: str, sheet: str):
    path = "N:\\KurzanovAV\\Личное\\"
    df = pd.read_excel(path+file_name, engine="openpyxl", sheet_name=sheet)
    return df



df_deals = read_report("Операции за 01.01.2023 — 31.12.2023.xlsx", "Сделки")
df_money_transfer = read_report("Операции за 01.01.2023 — 31.12.2023.xlsx", "Движение ДС")
print(df_money_transfer)