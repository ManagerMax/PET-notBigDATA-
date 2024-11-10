from airflow.models import Variable
import requests

# Загрузите переменные среды для токена и чата
TELEGRAM_TOKEN = Variable.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = Variable.get("TELEGRAM_CHAT_ID")


def on_success_callback(context):
    task_instance = context.get("task_instance")
    message = f"Спешу вас предупредить! Задача {task_instance.task_id} в DAGе {task_instance.dag_id} выполнена успешно!"
    send_telegram_alert(message)


def on_failure_callback(context):
    task_instance = context.get("task_instance")
    message = f"Спешу вас предупредить! Задача {task_instance.task_id} в DAGе {task_instance.dag_id} завершилась с ошибкой!"
    send_telegram_alert(message)


def send_telegram_alert(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    response = requests.post(url, data=payload)
    if response.status_code != 200:
        raise Exception("Ошибка при отправке сообщения в Telegram")
