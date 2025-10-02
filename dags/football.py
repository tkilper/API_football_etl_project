from airflow.decorators import dag, task
from datetime import datetime
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

from include.football.tasks import _get_players, _store_players, _get_formatted_csv, BUCKET_NAME

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
        task_id = 'store_players',
        python_callable = _store_players,
        op_kwargs = {'players': '{{ ti.xcom_pull(task_ids="get_players") }}', 'teamid': TEAMID}
    )

    format_players = DockerOperator(
        task_id = 'format_players',
        image = 'airflow/football-app',
        container_name = 'format_players',
        api_version = 'auto',
        auto_remove = 'success',
        docker_url = 'tcp://docker-proxy:2375',
        network_mode = 'container:spark-master',
        tty = True,
        xcom_all = False,
        mount_tmp_dir = False,
        environment = {
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids = "store_players") }}'
        }
    )

    get_formatted_csv = PythonOperator(
        task_id = 'get_formatted_csv',
        python_callable = _get_formatted_csv,
        op_kwargs = {
            'path': '{{ ti.xcom_pull(task_ids = "store_players") }}'
        }
    )

    load_to_dw = aql.load_file(
        task_id = 'load_to_dw',
        input_file = File(
            path = f"s3://{BUCKET_NAME}/{{{{ ti.xcom_pull(task_ids = 'get_formatted_csv') }}}}",
            conn_id = 'minio'
        ),
        output_table = Table(
            name = 'dim_players',
            conn_id = 'postgres',
            metadata = Metadata(
                schema = 'public'
            )
        ),
        load_options = {
            "aws_access_key_id": BaseHook.get_connection('minio').login,
            "aws_secret_access_key": BaseHook.get_connection('minio').password,
            "endpoint_url": BaseHook.get_connection('minio').host
        }
    )
    
    is_api_available() >> get_players >> store_players >> format_players >> get_formatted_csv >> load_to_dw

football()