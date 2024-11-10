from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from telegram_notifications import on_failure_callback, send_telegram_alert
import os

# загрузим переменные из .env
load_dotenv()
tokenApi = os.getenv("POLYGON_API_KEY")

# Конфигурация
today_date = datetime.today().strftime("%Y-%m-%d")  # текущая дата
tickers = ["AAPL", "MSFT", "GOOGL"]


def on_dag_success(context):
    dag_id = context.get("dag").dag_id
    message = f"DAG {dag_id} успешно завершён!"
    send_telegram_alert(message)


# Параметры для всех задач DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_failure_callback,
}

# Определяем DAG
# Название, параметры, интервал запуска
with DAG(
    "etl_tickers_data",
    default_args=default_args,
    schedule_interval="@daily",
    on_success_callback=on_dag_success,
) as dag:

    def extract_data():
        result = {}
        for ticker in tickers:
            params = {
                "timespan": "day",
                "from": "2022-01-01",
                "to": today_date,
                "apiKey": tokenApi,
            }
            url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/{params['timespan']}/{params['from']}/{params['to']}"
            response = requests.get(url, params={"apiKey": tokenApi})

            if response.status_code == 200:
                result[ticker] = response.json()
            else:
                print(
                    f"Ошибка при получении данных для {ticker}: {response.status_code}"
                )
        return result

    def extract_news():
        news_data = {}
        for ticker in tickers:
            url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&apiKey={tokenApi}"
            response = requests.get(url)
            if response.status_code == 200:
                news_data[ticker] = response.json().get("results", [])
            else:
                print(
                    f"Ошибка при получении новостей для {ticker}: {response.status_code}"
                )
        return news_data

    def extract_details():
        details_data = {}
        for ticker in tickers:
            url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={tokenApi}"
            response = requests.get(url)
            if response.status_code == 200:
                details_data[ticker] = response.json()
            else:
                print(
                    f"Ошибка при получении новостей для {ticker}: {response.status_code}"
                )
        return details_data

    def transform_data(ti):
        raw_data = ti.xcom_pull(task_ids="extract_data")
        processed_data = {}

        for ticker, data in raw_data.items():
            if "results" in data:
                df = pd.DataFrame(data["results"])

                # Переименуем столбцы
                df.rename(
                    columns={
                        "c": "close price",
                        "h": "highest price",
                        "l": "lowest price",
                        "n": "transactions",
                        "o": "open price",
                        "t": "timestamp",
                        "v": "volume",
                    },
                    inplace=True,
                )

                # Преобразуем timestamp из миллисекунд в datetime
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

                # Преобразуем timestamp в строку в формате ISO
                df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

                # Устанавливаем 'timestamp' как индекс
                df.set_index("timestamp", inplace=True)

                # Проверка и очистка данных
                df.drop_duplicates(inplace=True)
                df.dropna(subset=["close price", "volume"], inplace=True)
                df = df[df["close price"] > 0]

                # Преобразуем индексы в строки для совместимости с XCom
                processed_data[ticker] = df.apply(lambda x: x.to_dict()).to_dict()
            else:
                print(f"Нет данных для тикера {ticker}")

        return processed_data

    def transform_news(ti):
        raw_news_data = ti.xcom_pull(task_ids="extract_news")
        transformed_news_data = {}

        for ticker, news_list in raw_news_data.items():
            if news_list:
                # Создаем DataFrame из списка новостей
                df = pd.DataFrame(news_list)

                # Добавляем недостающие столбцы с значениями по умолчанию
                if "source" not in df.columns:
                    df["source"] = "Unknown"  # Значение по умолчанию для source
                if "summary" not in df.columns:
                    df["summary"] = ""  # Пустая строка для отсутствующих summary

                # Выбираем только нужные столбцы
                df = df[["title", "published_utc", "source", "summary", "article_url"]]
                df.rename(columns={"published_utc": "timestamp"}, inplace=True)
                # чтобы сериализовать данные и передать через xcom - укажем что df должен быть преобразован в список
                # каждая строка - отдельный словарь
                transformed_news_data[ticker] = df.to_dict(orient="records")
            else:
                print(f"Нет новостей для тикера {ticker}")

        return transformed_news_data

    def transform_details(ti):
        raw_details_data = ti.xcom_pull(task_ids="extract_details")
        transformed_details_data = {}

        for ticker, details in raw_details_data.items():
            if details and "results" in details:
                result = details["results"]
                # Включаем описание, а также плоскую структуру остальных данных
                flat_data = {
                    "ticker": ticker,
                    "name": result.get("name"),
                    "market": result.get("market"),
                    "locale": result.get("locale"),
                    "primary_exchange": result.get("primary_exchange"),
                    "currency": result.get("currency_name"),
                    "active": result.get("active"),
                    "city": result.get("city"),
                    "state": result.get("state"),
                    "homepage": result.get("homepage_url"),
                    "phone": result.get("phone_number"),
                }
                transformed_details_data[ticker] = [flat_data]
            else:
                print(f"Нет данных для тикера {ticker}")

        return transformed_details_data

    def load_data(ti):
        processed_data = ti.xcom_pull(task_ids="transform_data")
        transformed_news_data = ti.xcom_pull(task_ids="transform_news")
        transformed_details_data = ti.xcom_pull(task_ids="transform_details")
        # Создаём строку подключения
        engine = create_engine(
            f"postgresql+psycopg2://airflow:airflow@postgres/airflow"
        )

        # Загружаем данные для каждого тикера
        for ticker, data in processed_data.items():
            df = pd.DataFrame.from_dict(data)
            table_name = f"stock_data_{ticker.lower()}"
            df.to_sql(
                table_name,
                engine,
                if_exists="replace",
                index=True,
                index_label="timestamp",
            )
            print(f"Данные успешно загружены в таблицу {table_name}")

        # Загружаем данные новостей для каждого тикера
        for ticker, news_list in transformed_news_data.items():
            news_df = pd.DataFrame(news_list)
            news_table_name = f"news_data_{ticker.lower()}"
            news_df.to_sql(
                news_table_name,
                engine,
                if_exists="replace",
                index=False,
            )
            print(f"Новости успешно загружены в таблицу {news_table_name}")

        all_details_data = []
        for ticker, details in transformed_details_data.items():
            for detail in details:
                detail["ticker"] = ticker  # Добавляем колонку с тикером
                all_details_data.append(detail)

        details_df = pd.DataFrame(all_details_data)
        details_df.to_sql(
            "details_data",
            engine,
            if_exists="replace",
            index=False,
        )
        print("Детали успешно загружены в единую таблицу details_data")

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    extract_news_task = PythonOperator(
        task_id="extract_news",
        python_callable=extract_news,
    )

    extract_details_task = PythonOperator(
        task_id="extract_details",
        python_callable=extract_details,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    transform_news_task = PythonOperator(
        task_id="transform_news",
        python_callable=transform_news,
    )

    transform_details_task = PythonOperator(
        task_id="transform_details",
        python_callable=transform_details,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    # Зависимости задач
    extract_task >> transform_task
    extract_news_task >> transform_news_task
    extract_details_task >> transform_details_task
    [transform_task, transform_news_task, transform_details_task] >> load_task
