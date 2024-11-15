from typing import List, Dict

import requests
import pandas as pd
from io import StringIO
import boto3

import configparser


config = configparser.ConfigParser()
config.read('config.ini')
key_id = config['s3']['aws_access_key_id']
secret_key = config['s3']['aws_secret_access_key']


URL = "https://iss.moex.com//iss/history/engines/stock/zcyc.json"

def get_data_api_json(url, **kwargs) -> Dict:
    headers = {}
    for key, value in kwargs.items():
        headers[key] = value
    if headers:
        resp = requests.get(url, headers=headers)
    else:
        resp = requests.get(URL)
    if resp.status_code == 200:
        return resp.json()
    return None


def transform_data_api(json, columns_path: List[str], data_path: List[str]) -> pd.DataFrame:
    if json:
        df = pd.DataFrame(
            columns=json[columns_path[0]][columns_path[1]],
            data=json[data_path[0]][data_path[1]]
        )
        return df
    return None


def to_object_storage(df: pd.DataFrame, bucket: str, key: str, **kwargs):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, mode="w", encoding="UTF-8")
    csv_buffer.seek(0)

    s3 = boto3.session.Session(
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_key
    )
    cl = s3.client('s3', endpoint_url='https://storage.yandexcloud.net')

    cl.put_object(
        Bucket=bucket,
        Body=csv_buffer.getvalue(),
        Metadata={
            "type": "test",
            "another_data": "t233"
        },
        Key=key
    )







    print(cl.head_object(Bucket=bucket, Key='trt')['Metadata'])


#data = get_data_api_json(URL)

data = get_data_api_json(URL)

df = transform_data_api(data, ['params', 'columns'], ['params', 'data'])
to_object_storage(df, bucket="moex-files", key="trt")