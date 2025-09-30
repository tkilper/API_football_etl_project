from airflow.hooks.base import BaseHook
from minio import Minio
import json
from io import BytesIO

def _get_players(url, teamid):
    import requests
    
    querystring = {"teamid":f"{teamid}"}
    api = BaseHook.get_connection('football_api')
    response = requests.get(url, headers=api.extra_dejson['headers'], params=querystring)
    return json.dumps(response.json()['response']['list']['squad'])

def _store_players(players, teamid):
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint = minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key = minio.login,
        secret_key = minio.password,
        secure = False
    )
    bucket_name = 'football'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    players = json.loads(players)
    data = json.dumps(players, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name = bucket_name,
        object_name = f'{teamid}/players.json',
        data = BytesIO(data),
        length = len(data)
    )
    return f'{objw.bucket_name}/{teamid}'
