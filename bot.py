import os
import threading
import time
from bs4 import BeautifulSoup
from flask import Flask
import requests

app = Flask(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "ТВОЙ_ТОКЕН")
CHAT_ID = os.environ.get("CHAT_ID", "ТВОЙ_ЧАТ_ID")

URL = "https://t.me/s/kpszsu"
sent_messages = set()


@app.route("/")
def home():
  return "Lukas_Alarm_bot is active!"


def send_telegram_message(message):
  url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
  payload = {"chat_id": CHAT_ID, "text": message}
  try:
    requests.post(url, json=payload)
  except Exception as e:
    print(f"Ошибка отправки: {e}")


def check_updates():
  headers = {
      "User-Agent": (
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML,"
          " like Gecko) Chrome/120.0.0.0 Safari/537.36"
      )
  }
  try:
    response = requests.get(URL, headers=headers)
    if response.status_code != 200:
      print(f"Ошибка доступа к каналу: статус {response.status_code}")
      return

    soup = BeautifulSoup(response.text, "html.parser")
    posts = soup.find_all("div", class_="tgme_widget_message")

    for post in posts:
      post_id = post.get("data-post", "")
      text_elem = post.find("div", class_="tgme_widget_message_text")
      if not text_elem:
        continue

      post_text = text_elem.get_text()
      post_text_lower = post_text.lower()

      if "бпла" in post_text_lower:
        if post_id not in sent_messages:
          alert_text = f"🚨 Тест: обнаружено слово БПЛА!\n\n{post_text}"
          send_telegram_message(alert_text)
          sent_messages.add(post_id)

          if len(sent_messages) > 100:
            sent_messages.clear()

  except Exception as e:
    print(f"Ошибка запроса: {e}")


def run_bot():
  print("Бот запущен на тест слова БПЛА...")
  while True:
    check_updates()
    time.sleep(15)


# Запускаем парсер в фоновом потоке сразу при импорте модуля (нужно для Gunicorn)
bot_thread = threading.Thread(target=run_bot, daemon=True)
bot_thread.start()

if __name__ == "__main__":
  port = int(os.environ.get("PORT", 10000))
  app.run(host="0.0.0.0", port=port)
