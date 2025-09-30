from airflow.decorators import dag, task
from datetime import datetime
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from include.football.tasks import _get_players
from include.football.tasks import _store_players

TEAMID = "8634"

@dag(
    start_date=datetime(2025, 1, 1),
    schedule='@weekly',
    catchup=False,
    tags=['football']
)
def football():

    # sensor task decorator
    # params:
    # poke_interval: how often the sensor will check the api in secs
    # timeout: how much time until sensor timesout in secs
    # mode: ?
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        import requests

        api = BaseHook.get_connection('football_api')
        url = f"{api.host}"
        print(url)
        response  = requests.get(url)
        condition = response.json()['message'] == 'Invalid API key. Go to https://docs.rapidapi.com/docs/keys for more info.'
        return PokeReturnValue(is_done=condition, xcom_value=response.url)
    
    get_players = PythonOperator(
        task_id = 'get_players',
        python_callable = _get_players,
        op_kwargs = {'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'teamid': TEAMID}
    )

    store_players = PythonOperator(
        task_id = 'players',
        python_callable = _store_players,
        op_kwargs = {'players': '{{ ti.xcom_pull(task_ids="get_players") }}', 'teamid': TEAMID}
    )

    format_players = DockerOperator(
        task_id = 'format_prices',
        image = 'airflow/football-app',
        container_name = 'format_players',
        api_version = 'auto',
        auto_remove = True,
        docker_url = 'tcp//docker-proxy:2375',
        network_node = 'container:spark-master',
        tty = True,
        xcom_all = False,
        mount_tmp_dir = False,
        environment
    )
    
    is_api_available() >> get_players >> store_players

football()