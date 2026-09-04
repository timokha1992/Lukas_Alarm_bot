import os
import time
import requests
from bs4 import BeautifulSoup

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "ТВОЙ_ТОКЕН")
CHAT_ID = os.environ.get("CHAT_ID", "ТВОЙ_CHAT_ID")

URL = "ССЫЛКА_НА_ОТСЛЕЖИВАЕМУЮ_СТРАНИЦУ"

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"Ошибка отправки: {e}")

def check_updates():
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(URL, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Логика проверки страницы
        # ...
        
    except Exception as e:
        print(f"Ошибка запроса: {e}")

if __name__ == "__main__":
    print("Бот запущен и мониторит изменения...")
    while True:
        check_updates()
        time.sleep(15)  # Проверка каждые 15 секунд