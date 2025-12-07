from datetime import datetime, timedelta
import os
import json
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

from api_auth import *

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/tmp")
PROJECT_ROOT = Path(__file__).resolve().parents[2]

def load_config():
    cfg_path = PROJECT_ROOT / "config.json"
    with cfg_path.open() as f:
        return json.load(f)


def fetch_new_fuel_prices(api_key: str, base_url: str, new_prices_path: str, **context) -> dict:
    """
    Fetch new fuel prices from the API and return the JSON data.
    """
    ti = context["ti"]
    access_token = ti.xcom_pull(task_ids="get_access_token")

    data = call_new_fuel_prices(
        access_token=access_token,
        api_key=api_key,
        base_url=base_url,
        new_prices_path=new_prices_path,
    )

    return data


with DAG(
    dag_id="new_price_per_hour",
    description="fetch new fuel prices every hour, save to s3",
    schedule="@hourly",
    start_date=datetime(2025, 12, 7),
    catchup=False,
) as dag:

    config = load_config()

    ts_operator = PythonOperator(
        task_id="build_timestamp",
        python_callable=build_ts_string,
    )

    access_token_operator = PythonOperator(
        task_id="get_access_token",
        python_callable=get_access_token,
        op_kwargs={
            "api_key": config["fuel_api"]["api_key"],
            "api_secret": config["fuel_api"]["api_secret"],
            "base_url": config["fuel_api"]["base_url"],
            "auth_path": config["fuel_api"]["auth_path"],
        },
    )

    fetch_prices_operator = PythonOperator(
        task_id="fetch_new_fuel_prices",
        python_callable=fetch_new_fuel_prices,
        op_kwargs={
            "api_key": config["fuel_api"]["api_key"],
            "base_url": config["fuel_api"]["base_url"],
            "new_prices_path": config["fuel_api"]["new_prices_path"],
        },
    )

    write_to_s3_operator = S3CreateObjectOperator(
        task_id="write_new_prices_to_s3",
        s3_bucket=config["airflow"]["s3_bucket_name"],
        s3_key=(
            f"{config['airflow']['s3_prefix']}"
            "fuel_new_prices_raw_{{ ti.xcom_pull(task_ids='build_timestamp') }}.json"
        ),
        data="{{ ti.xcom_pull(task_ids='fetch_new_fuel_prices') }}",
        aws_conn_id=config["airflow"]["aws_conn_id"],
    )

    ts_operator >> access_token_operator >> fetch_prices_operator >> write_to_s3_operator
