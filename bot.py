import os
import threading
import time
from bs4 import BeautifulSoup
from flask import Flask
import requests

app = Flask(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "ТВОЙ_ТОКЕН")
CHAT_ID = os.environ.get("CHAT_ID", "ТВОЙ_ЧАТ_ID")
RENDER_EXTERNAL_URL = os.environ.get(
    "RENDER_EXTERNAL_URL", "https://lukas-alarm-bot-1.onrender.com"
)

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
      return

    soup = BeautifulSoup(response.text, "html.parser")
    posts = soup.find_all("div", class_="tgme_widget_message")

    for post in posts:
      post_id = post.get("data-post", "")
      text_elem = post.find("div", class_="tgme_widget_message_text")
      if not text_elem:
        continue

      post_text = text_elem.get_text()
      if "бпла" in post_text.lower():
        if post_id not in sent_messages:
          alert_text = f"🚨 Внимание! Обнаружено слово БПЛА:\n\n{post_text}"
          send_telegram_message(alert_text)
          sent_messages.add(post_id)

          if len(sent_messages) > 100:
            sent_messages.clear()
  except Exception as e:
    print(f"Ошибка парсинга: {e}")


def run_bot():
  print("Фоновый поток парсера Telegram запущен...")
  while True:
    check_updates()
    time.sleep(15)


def self_ping():
  """Каждые 5 минут шлет запрос сама себе, чтобы контейнер не засыпал"""
  print("Фоновый поток самопинга запущен...")
  while True:
    time.sleep(300)  # 5 минут
    try:
      requests.get(RENDER_EXTERNAL_URL)
      print("Самопинг выполнен успешно")
    except Exception as e:
      print(f"Ошибка самопинга: {e}")


# Запускаем парсер и самопинг в отдельных фоновых потоках
threading.Thread(target=run_bot, daemon=True).start()
threading.Thread(target=self_ping, daemon=True).start()

if __name__ == "__main__":
  port = int(os.environ.get("PORT", 10000))
  app.run(host="0.0.0.0", port=port)
