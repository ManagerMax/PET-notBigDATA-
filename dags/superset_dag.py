import requests
import json
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from io import BytesIO

bot_token = Variable.get("TELEGRAM_BOT_TOKEN")
chat_id = Variable.get("TELEGRAM_CHAT_ID")

# Параметры
BASE_URL = "http://localhost:8088"
DATASET_ENDPOINT = f"{BASE_URL}/api/v1/dataset/"
CHART_ENDPOINT = f"{BASE_URL}/api/v1/chart/"
TELEGRAM_BOT_TOKEN = bot_token
CHAT_ID = chat_id
USERNAME = "admin"
PASSWORD = "admin"

# Сессия для взаимодействия с Superset
session = requests.Session()


def get_tokens():
    try:
        auth_url = f"{BASE_URL}/api/v1/security/login"
        auth_data = {
            "username": USERNAME,
            "password": PASSWORD,
            "provider": "db",
            "refresh": True,
        }
        auth_response = session.post(auth_url, json=auth_data)
        auth_response.raise_for_status()
        access_token = auth_response.json().get("access_token")
        session.headers.update({"Authorization": f"Bearer {access_token}"})
        print("Access token retrieved:", access_token)

        csrf_url = f"{BASE_URL}/api/v1/security/csrf_token/"
        csrf_response = session.get(csrf_url)
        csrf_response.raise_for_status()
        csrf_token = csrf_response.json().get("result")
        session.headers.update({"X-CSRFToken": csrf_token})
        print("CSRF token retrieved:", csrf_token)

    except requests.exceptions.RequestException as e:
        print("Error in get_tokens:", e)
        raise


def create_dataset():
    get_tokens()
    dataset_data = {
        "table_name": "stock_data_aapl",
        "database": 1,
        "schema": "public",
    }
    response = session.post(DATASET_ENDPOINT, json=dataset_data)
    response.raise_for_status()
    print("Dataset created:", response.json())
    return response.json()["id"]


def create_chart(dataset_id):
    chart_data = {
        "slice_name": "close_price_line",
        "viz_type": "line",
        "params": json.dumps(
            {
                "metrics": [
                    {
                        "label": "Average Close Price",
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "close price"},
                        "aggregate": "AVG",
                    }
                ],
                "x_axis": "timestamp",
                "adhoc_filters": [],
                "y_axis_format": "SMART_NUMBER",
                "time_column": "timestamp",
                "granularity_sqla": "timestamp",
            }
        ),
        "datasource_id": dataset_id,
        "datasource_type": "table",
    }
    response = session.post(CHART_ENDPOINT, json=chart_data)
    response.raise_for_status()
    print("Chart created:", response.json())
    return response.json()["id"]


def send_chart_to_telegram(chart_id):
    chart_export_url = f"{BASE_URL}/api/v1/chart/{chart_id}/export/"
    response = session.get(chart_export_url)
    response.raise_for_status()

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    files = {"photo": ("chart.png", BytesIO(response.content))}
    data = {"chat_id": CHAT_ID}
    telegram_response = requests.post(url, data=data, files=files)
    telegram_response.raise_for_status()
    print("Chart sent to Telegram")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "create_and_send_chart", default_args=default_args, schedule_interval="@daily"
) as dag:
    create_dataset_task = PythonOperator(
        task_id="create_dataset",
        python_callable=create_dataset,
    )

    create_chart_task = PythonOperator(
        task_id="create_chart",
        python_callable=lambda **kwargs: create_chart(
            kwargs["ti"].xcom_pull(task_ids="create_dataset")
        ),
        provide_context=True,
    )

    send_chart_task = PythonOperator(
        task_id="send_chart_to_telegram",
        python_callable=lambda **kwargs: send_chart_to_telegram(
            kwargs["ti"].xcom_pull(task_ids="create_chart")
        ),
        provide_context=True,
    )

    create_dataset_task >> create_chart_task >> send_chart_task
