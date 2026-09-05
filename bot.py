import os
import threading
import time
import socket
from datetime import datetime, timezone, timedelta

from bs4 import BeautifulSoup
from flask import Flask
import requests


app = Flask(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

URL = "https://t.me/s/kpszsu"

MAX_MESSAGE_AGE_MINUTES = 5

sent_messages = set()


def send_telegram_message(message):
    print("Пробую отправить уведомление в Telegram...")

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": message
    }

    try:
        response = requests.post(
            url,
            json=payload,
            timeout=10
        )

        print(
            f"Telegram sendMessage: "
            f"{response.status_code} "
            f"{response.text[:500]}"
        )

        if response.status_code == 200:
            print("Уведомление успешно отправлено")
            return True

        print("Telegram НЕ принял сообщение")
        return False

    except Exception as e:
        print(f"ОШИБКА отправки в Telegram: {e}")
        return False


def test_telegram_api():
    print("Проверяю доступ к Telegram Bot API...")

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getMe"

    try:
        response = requests.get(
            url,
            timeout=5
        )

        print(
            f"Telegram Bot API: "
            f"{response.status_code} "
            f"{response.text[:300]}"
        )

        if response.status_code == 200:
            print("Telegram Bot API доступен")
            return True

        print("Telegram Bot API вернул ошибку")
        return False

    except Exception as e:
        print(f"ОШИБКА доступа к Telegram Bot API: {e}")
        return False


def get_post_datetime(post):
    time_elem = post.find("time")

    if not time_elem:
        return None

    datetime_value = time_elem.get("datetime")

    if not datetime_value:
        return None

    try:
        post_time = datetime.fromisoformat(
            datetime_value.replace("Z", "+00:00")
        )

        if post_time.tzinfo is None:
            post_time = post_time.replace(
                tzinfo=timezone.utc
            )

        return post_time

    except Exception as e:
        print(
            f"Ошибка определения времени поста: {e}"
        )
        return None


def check_updates():

    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/122.0.0.0 "
            "Safari/537.36"
        )
    }

    try:
        print("----- НАЧАЛО ПРОВЕРКИ -----")

        print("Отправляю запрос на t.me...")

        response = requests.get(
            URL,
            headers=headers,
            timeout=10
        )

        print(
            f"Ответ t.me: "
            f"{response.status_code}, "
            f"{len(response.text)} bytes"
        )

        if response.status_code != 200:
            print(
                f"Telegram channel вернул "
                f"HTTP {response.status_code}"
            )
            return

        print("Разбираю HTML страницы...")

        soup = BeautifulSoup(
            response.text,
            "html.parser"
        )

        posts = soup.find_all(
            "div",
            class_="tgme_widget_message"
        )

        print(
            f"Найдено постов на странице: "
            f"{len(posts)}"
        )

        now = datetime.now(timezone.utc)

        cutoff_time = (
            now -
            timedelta(
                minutes=MAX_MESSAGE_AGE_MINUTES
            )
        )

        for post in posts:

            post_id = post.get(
                "data-post",
                ""
            )

            if not post_id:
                continue

            if post_id in sent_messages:
                continue

            post_time = get_post_datetime(post)

            if post_time is None:
                continue

            if post_time < cutoff_time:
                continue

            text_elem = post.find(
                "div",
                class_="tgme_widget_message_text"
            )

            if not text_elem:
                continue

            post_text = text_elem.get_text(
                "\n",
                strip=True
            )

            if "бпл" not in post_text.lower():
                continue

            print(
                f"🚨 НАЙДЕНО УПОМИНАНИЕ БпЛА: "
                f"{post_id}"
            )

            alert_text = (
                "🚨 ВНИМАНИЕ!\n\n"
                "Обнаружено упоминание БпЛА "
                "в официальном Telegram-канале "
                "Повітряних Сил України:\n\n"
                f"{post_text}"
            )

            if send_telegram_message(
                alert_text
            ):
                sent_messages.add(post_id)

        print("----- ПРОВЕРКА ЗАВЕРШЕНА -----")

    except requests.exceptions.Timeout:
        print(
            "ОШИБКА: запрос к t.me "
            "превысил 10 секунд"
        )

    except requests.exceptions.RequestException as e:
        print(
            f"ОШИБКА сетевого запроса: {e}"
        )

    except Exception as e:
        print(
            f"ОШИБКА ПАРСЕРА: {e}"
        )


def run_bot():

    print("==============================")
    print("ФОНОВЫЙ ПАРСЕР ЗАПУЩЕН")
    print("Ключевое слово: БпЛА")
    print("Проверка каждые 15 секунд")
    print("==============================")

    test_telegram_api()

    while True:
        check_updates()
        time.sleep(15)


@app.route("/")
def home():
    return "Lukas_Alarm_bot is active!"


@app.route("/health")
def health():
    return "OK"


@app.route("/test")
def test():

    results = []

    # Проверяем DNS Telegram
    try:
        start = time.time()

        telegram_ip = socket.gethostbyname(
            "api.telegram.org"
        )

        elapsed = round(
            time.time() - start,
            2
        )

        results.append(
            f"DNS Telegram OK: "
            f"api.telegram.org -> "
            f"{telegram_ip}, "
            f"time={elapsed}s"
        )

    except Exception as e:

        results.append(
            f"DNS Telegram ERROR: "
            f"{type(e).__name__}: {e}"
        )

        return "<br>".join(results)


    # Проверяем TCP-соединение с Telegram
    try:
        start = time.time()

        connection = socket.create_connection(
            ("api.telegram.org", 443),
            timeout=5
        )

        connection.close()

        elapsed = round(
            time.time() - start,
            2
        )

        results.append(
            f"TCP Telegram OK: "
            f"port 443, "
            f"time={elapsed}s"
        )

    except Exception as e:

        results.append(
            f"TCP Telegram ERROR: "
            f"{type(e).__name__}: {e}"
        )


    # Проверяем TCP-соединение с обычным сайтом
    try:
        start = time.time()

        connection = socket.create_connection(
            ("example.com", 443),
            timeout=5
        )

        connection.close()

        elapsed = round(
            time.time() - start,
            2
        )

        results.append(
            f"TCP example.com OK: "
            f"port 443, "
            f"time={elapsed}s"
        )

    except Exception as e:

        results.append(
            f"TCP example.com ERROR: "
            f"{type(e).__name__}: {e}"
        )


    return "<br>".join(results)


print("Запускаю фоновый поток...")

threading.Thread(
    target=run_bot,
    daemon=True
).start()


if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
