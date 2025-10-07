from airflow.hooks.base import BaseHook
from minio import Minio
import json
from io import BytesIO
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.discord.notifications.discord import DiscordNotifier
from airflow.utils.context import Context

BUCKET_NAME = 'football'

def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint = minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key = minio.login,
        secret_key = minio.password,
        secure = False
    )
    return client

def _get_players(url, teamid):
    import requests
    
    querystring = {"teamid":f"{teamid}"}
    api = BaseHook.get_connection('football_api')
    response = requests.get(url, headers=api.extra_dejson['headers'], params=querystring)
    return json.dumps(response.json()['response']['list']['squad'])

def _store_players(players, teamid):
    client = _get_minio_client()
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    players = json.loads(players)
    data = json.dumps(players, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name = BUCKET_NAME,
        object_name = f'{teamid}/players.json',
        data = BytesIO(data),
        length = len(data)
    )
    return f'{objw.bucket_name}/{teamid}'

def _get_formatted_csv(path):
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_players/"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
    return AirflowNotFoundException('The csv file is not found')