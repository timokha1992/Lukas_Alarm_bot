import os
import threading
import time
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
            f"{response.status_code} {response.text[:300]}"
        )

        if response.status_code == 200:
            print("Уведомление успешно отправлено")
            return True

        return False

    except Exception as e:
        print(f"Ошибка отправки в Telegram: {e}")
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
            post_time = post_time.replace(tzinfo=timezone.utc)

        return post_time

    except Exception as e:
        print(f"Ошибка определения времени поста: {e}")
        return None


def check_updates():
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    try:
        print("Проверяю канал...")

        response = requests.get(
            URL,
            headers=headers,
            timeout=10
        )

        print(
            f"Telegram channel: "
            f"{response.status_code}, "
            f"{len(response.text)} bytes"
        )

        if response.status_code != 200:
            return

        soup = BeautifulSoup(
            response.text,
            "html.parser"
        )

        posts = soup.find_all(
            "div",
            class_="tgme_widget_message"
        )

        print(f"Найдено постов: {len(posts)}")

        now = datetime.now(timezone.utc)

        cutoff_time = (
            now -
            timedelta(minutes=MAX_MESSAGE_AGE_MINUTES)
        )

        for post in posts:

            post_id = post.get("data-post", "")

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
                f"🚨 НАЙДЕНО БпЛА: {post_id}"
            )

            alert_text = (
                "🚨 ВНИМАНИЕ!\n\n"
                "Обнаружено упоминание БпЛА "
                "в официальном Telegram-канале "
                "Повітряних Сил України:\n\n"
                f"{post_text}"
            )

            if send_telegram_message(alert_text):
                sent_messages.add(post_id)

    except Exception as e:
        print(f"ОШИБКА ПАРСЕРА: {e}")


def run_bot():
    print("================================")
    print("ФОНОВЫЙ ПАРСЕР ЗАПУЩЕН")
    print("Проверка каждые 15 секунд")
    print("================================")

    while True:
        check_updates()
        time.sleep(15)


@app.route("/")
def home():
    return "Lukas_Alarm_bot is active!"


@app.route("/health")
def health():
    return "OK"


# ВАЖНО:
# Запускаем парсер СРАЗУ при старте приложения,
# а не после открытия страницы "/"
threading.Thread(
    target=run_bot,
    daemon=True
).start()


if __name__ == "__main__":
    port = int(
        os.environ.get("PORT", 10000)
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
