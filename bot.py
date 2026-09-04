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

# Используем публичный RSS-мост для чтения канала без блокировок
RSS_URL = "https://rsshub.app/telegram/channel/kpszsu"
sent_messages = set()
initialized = False
init_lock = threading.Lock()


def send_telegram_message(message):
  url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
  payload = {"chat_id": CHAT_ID, "text": message}
  try:
    requests.post(url, json=payload, timeout=10)
  except Exception as e:
    print(f"Ошибка отправки: {e}")


def check_updates():
  headers = {
      "User-Agent": (
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML,"
          " like Gecko) Chrome/122.0.0.0 Safari/537.36"
      )
  }
  try:
    response = requests.get(RSS_URL, headers=headers, timeout=15)
    print(
        f"Статус RSS-моста: {response.status_code}, длина: {len(response.text)}"
    )

    if response.status_code != 200:
      return

    # Парсим XML-ленту вместо HTML
    soup = BeautifulSoup(response.content, "xml")
    items = soup.find_all("item")
    print(f"Найдено записей в RSS: {len(items)}")

    for item in items:
      guid = item.find("guid")
      post_id = guid.text if guid else None

      title = item.find("title")
      description = item.find("description")

      post_text = ""
      if title:
        post_text += title.text + "\n"
      if description:
        post_text += description.text

      if not post_text:
        continue

      if "бпла" in post_text.lower():
        if post_id not in sent_messages:
          alert_text = f"🚨 Внимание! Обнаружено слово БПЛА:\n\n{post_text}"
          send_telegram_message(alert_text)
          if post_id:
            sent_messages.add(post_id)

          if len(sent_messages) > 100:
            sent_messages.clear()
  except Exception as e:
    print(f"Ошибка парсинга RSS: {e}")


def run_bot():
  print("Фоновый поток RSS-парсера запущен...")
  while True:
    check_updates()
    time.sleep(30)


def self_ping():
  print("Фоновый поток самопинга запущен...")
  while True:
    time.sleep(300)
    try:
      requests.get(RENDER_EXTERNAL_URL, timeout=10)
      print("Самопинг выполнен успешно")
    except Exception as e:
      print(f"Ошибка самопинга: {e}")


@app.route("/")
def home():
  global initialized
  if not initialized:
    with init_lock:
      if not initialized:
        threading.Thread(target=run_bot, daemon=True).start()
        threading.Thread(target=self_ping, daemon=True).start()
        initialized = True
  return "Lukas_Alarm_bot is active via RSS!"


if __name__ == "__main__":
  port = int(os.environ.get("PORT", 10000))
  app.run(host="0.0.0.0", port=port)
