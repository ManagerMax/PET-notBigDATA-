import requests
import os
from dotenv import load_dotenv

load_dotenv()
# Загрузите переменные среды для токена и чата
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def on_failure_callback(context):
    task_instance = context.get("task_instance")
    message = f"Задача {task_instance.task_id} в DAG {task_instance.dag_id} завершилась с ошибкой!"
    send_telegram_alert(message)


def send_telegram_alert(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    response = requests.post(url, data=payload)
    if response.status_code != 200:
        raise Exception("Ошибка при отправке сообщения в Telegram")
