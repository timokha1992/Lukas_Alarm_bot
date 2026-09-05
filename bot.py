import os
import threading
import time
import socket
from datetime import datetime, timezone, timedelta
from collections import deque

import requests
from bs4 import BeautifulSoup
from flask import Flask


# ============================================================
# НАСТРОЙКИ
# ============================================================

app = Flask(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
CHAT_ID = os.environ.get("CHAT_ID", "").strip()

SOURCE_URL = "https://t.me/s/kpszsu"

KEYWORD = "бпл"

CHECK_INTERVAL_SECONDS = 15
STATUS_UPDATE_INTERVAL_SECONDS = 60

MAX_MESSAGE_AGE_MINUTES = 5
MAX_SENT_MESSAGES = 1000

REQUEST_TIMEOUT = (5, 15)


# ============================================================
# СОСТОЯНИЕ БОТА
# ============================================================

state = {
    "parser_running": False,

    "started_at": None,

    "last_check_at": None,
    "last_successful_check_at": None,
    "last_source_status": None,

    "total_checks": 0,
    "successful_checks": 0,
    "failed_checks": 0,
    "consecutive_errors": 0,

    "last_alert_at": None,
    "last_alert_id": None,

    "telegram_api_ok": False,

    "status_message_id": None,
}

sent_messages = set()
sent_messages_order = deque(maxlen=MAX_SENT_MESSAGES)

state_lock = threading.Lock()


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
})


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def format_time(dt):
    if not dt:
        return "нет данных"

    try:
        return dt.astimezone().strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        return str(dt)


def remember_sent_message(post_id):
    if post_id in sent_messages:
        return

    if len(sent_messages_order) >= MAX_SENT_MESSAGES:
        old_id = sent_messages_order.popleft()
        sent_messages.discard(old_id)

    sent_messages_order.append(post_id)
    sent_messages.add(post_id)


# ============================================================
# TELEGRAM API
# ============================================================

def telegram_api_url(method):
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"


def telegram_request(method, payload=None, attempts=3):
    if not TELEGRAM_TOKEN:
        print("ОШИБКА: TELEGRAM_TOKEN не задан")
        return None

    for attempt in range(1, attempts + 1):
        try:
            response = session.post(
                telegram_api_url(method),
                json=payload or {},
                timeout=REQUEST_TIMEOUT
            )

            print(
                f"Telegram {method}: "
                f"попытка {attempt}/{attempts}, "
                f"HTTP {response.status_code}"
            )

            if response.status_code == 200:
                try:
                    data = response.json()
                except Exception:
                    data = None

                if data and data.get("ok"):
                    return data

                print(f"Telegram вернул ошибку: {response.text[:500]}")

            elif response.status_code == 429:
                try:
                    data = response.json()
                    retry_after = int(
                        data.get("parameters", {}).get("retry_after", 2)
                    )
                except Exception:
                    retry_after = 2

                print(
                    f"Telegram rate limit. "
                    f"Жду {retry_after} сек."
                )

                time.sleep(min(retry_after, 30))

            elif response.status_code >= 500:
                print(
                    f"Telegram server error: "
                    f"{response.text[:500]}"
                )

            else:
                print(
                    f"Telegram API error: "
                    f"{response.text[:500]}"
                )

        except requests.exceptions.Timeout as e:
            print(
                f"Telegram timeout "
                f"(попытка {attempt}/{attempts}): {e}"
            )

        except requests.exceptions.RequestException as e:
            print(
                f"Telegram network error "
                f"(попытка {attempt}/{attempts}): {e}"
            )

        except Exception as e:
            print(
                f"Telegram unexpected error "
                f"(попытка {attempt}/{attempts}): {type(e).__name__}: {e}"
            )

        if attempt < attempts:
            time.sleep(2)

    return None


def send_telegram_message(message):
    print("Пробую отправить уведомление в Telegram...")

    data = telegram_request(
        "sendMessage",
        {
            "chat_id": CHAT_ID,
            "text": message,
        },
        attempts=3
    )

    if data:
        print("Уведомление успешно отправлено")
        return True

    print("Уведомление НЕ отправлено")
    return False


def edit_telegram_message(message_id, text):
    data = telegram_request(
        "editMessageText",
        {
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "text": text,
        },
        attempts=3
    )

    return bool(data)


# ============================================================
# TELEGRAM API ПРОВЕРКА
# ============================================================

def test_telegram_api():
    print("Проверяю доступ к Telegram Bot API...")

    data = telegram_request(
        "getMe",
        {},
        attempts=2
    )

    with state_lock:
        state["telegram_api_ok"] = bool(data)

    if data:
        print("Telegram Bot API доступен")
        return True

    print("Telegram Bot API недоступен")
    return False


# ============================================================
# ПОЛУЧЕНИЕ PINNED MESSAGE
# ============================================================

def get_pinned_message_id():
    data = telegram_request(
        "getChat",
        {
            "chat_id": CHAT_ID
        },
        attempts=2
    )

    if not data:
        return None

    chat = data.get("result", {})
    pinned_message = chat.get("pinned_message")

    if not pinned_message:
        return None

    message_id = pinned_message.get("message_id")

    if message_id:
        print(f"Найдено закреплённое сообщение: {message_id}")

    return message_id


# ============================================================
# СОЗДАНИЕ / ПОИСК СТАТУСА
# ============================================================

def ensure_status_message():
    print("Проверяю сообщение состояния системы...")

    pinned_id = get_pinned_message_id()

    if pinned_id:
        with state_lock:
            state["status_message_id"] = pinned_id

        print(
            f"Буду обновлять закреплённое сообщение "
            f"{pinned_id}"
        )

        return pinned_id

    print("Закреплённого сообщения нет. Создаю новое.")

    status_text = build_status_text()

    data = telegram_request(
        "sendMessage",
        {
            "chat_id": CHAT_ID,
            "text": status_text,
        },
        attempts=3
    )

    if not data:
        print("Не удалось создать сообщение состояния")
        return None

    message = data.get("result", {})
    message_id = message.get("message_id")

    if not message_id:
        print("Telegram не вернул message_id")
        return None

    pin_data = telegram_request(
        "pinChatMessage",
        {
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "disable_notification": True,
        },
        attempts=3
    )

    if pin_data:
        print(f"Сообщение состояния закреплено: {message_id}")
    else:
        print(
            "Сообщение создано, но закрепить его не удалось. "
            "Проверь права бота."
        )

    with state_lock:
        state["status_message_id"] = message_id

    return message_id


# ============================================================
# СТАТУС
# ============================================================

def build_status_text():
    with state_lock:
        parser_running = state["parser_running"]
        started_at = state["started_at"]

        last_check_at = state["last_check_at"]
        last_successful_check_at = state["last_successful_check_at"]
        last_source_status = state["last_source_status"]

        total_checks = state["total_checks"]
        successful_checks = state["successful_checks"]
        failed_checks = state["failed_checks"]
        consecutive_errors = state["consecutive_errors"]

        last_alert_at = state["last_alert_at"]
        last_alert_id = state["last_alert_id"]

        telegram_ok = state["telegram_api_ok"]

    parser_icon = "🟢" if parser_running else "🔴"
    telegram_icon = "🟢" if telegram_ok else "🔴"

    if last_source_status == 200:
        source_icon = "🟢"
    elif last_source_status is None:
        source_icon = "⚪"
    else:
        source_icon = "🔴"

    return (
        "🛠️ СОСТОЯНИЕ СИСТЕМЫ\n\n"

        f"{parser_icon} Парсер: "
        f"{'РАБОТАЕТ' if parser_running else 'ОСТАНОВЛЕН'}\n"

        f"{telegram_icon} Telegram API: "
        f"{'OK' if telegram_ok else 'ОШИБКА'}\n"

        f"{source_icon} Источник: "
        f"{'HTTP ' + str(last_source_status) if last_source_status else 'нет данных'}\n\n"

        f"🔎 Ключевое слово: {KEYWORD.upper()}\n"
        f"⏱ Проверка: каждые {CHECK_INTERVAL_SECONDS} сек.\n"
        f"⌛ Возраст сообщения: до {MAX_MESSAGE_AGE_MINUTES} мин.\n\n"

        f"🔄 Проверок: {total_checks}\n"
        f"✅ Успешных: {successful_checks}\n"
        f"❌ Ошибок: {failed_checks}\n"
        f"⚠️ Ошибок подряд: {consecutive_errors}\n\n"

        f"🕐 Последняя проверка:\n"
        f"{format_time(last_check_at)}\n\n"

        f"✅ Последняя успешная проверка:\n"
        f"{format_time(last_successful_check_at)}\n\n"

        f"🚨 Последняя тревога:\n"
        f"{format_time(last_alert_at)}\n"

        f"ID: {last_alert_id or 'нет'}\n\n"

        f"🚀 Запущен:\n"
        f"{format_time(started_at)}"
    )


def update_status_message():
    with state_lock:
        message_id = state["status_message_id"]

    if not message_id:
        message_id = ensure_status_message()

    if not message_id:
        return

    text = build_status_text()

    if edit_telegram_message(message_id, text):
        print("Статус системы обновлён")
    else:
        print("Не удалось обновить статус")


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

def process_telegram_command(message):
    text = message.get("text", "").strip()

    if not text:
        return

    if not text.startswith("/"):
        return

    command = text.split()[0].lower()

    # /test или /test@имя_бота
    command_name = command.split("@")[0]

    if command_name == "/test":
        print("=== ПОЛУЧЕНА КОМАНДА /test ===")

        test_message = (
            "🔔 ТЕСТОВОЕ УВЕДОМЛЕНИЕ\n\n"
            "Бот получил команду /test.\n"
            "Связь с Telegram и рабочей группой проверена."
        )

        success = send_telegram_message(test_message)

        if success:
            print("/test успешно отправлен")
        else:
            print("/test НЕ удалось отправить")


def telegram_command_listener():
    print("Запускаю обработчик Telegram-команд...")

    offset = 0

    while True:
        try:
            data = telegram_request(
                "getUpdates",
                {
                    "offset": offset,
                    "timeout": 20,
                    "allowed_updates": ["message"],
                },
                attempts=1
            )

            if not data:
                time.sleep(2)
                continue

            updates = data.get("result", [])

            for update in updates:
                update_id = update.get("update_id")

                if update_id is not None:
                    offset = update_id + 1

                message = update.get("message")

                if not message:
                    continue

                # Обрабатываем команды только из нашей группы
                message_chat_id = str(
                    message.get("chat", {}).get("id", "")
                )

                if message_chat_id != str(CHAT_ID):
                    continue

                process_telegram_command(message)

        except Exception as e:
            print(
                f"ОШИБКА обработчика Telegram-команд: "
                f"{type(e).__name__}: {e}"
            )
            time.sleep(5)


# ============================================================
# ОБНОВЛЕНИЕ СТАТУСА
# ============================================================

def status_loop():
    print(
        f"Запущено обновление статуса "
        f"каждые {STATUS_UPDATE_INTERVAL_SECONDS} секунд"
    )

    while True:
        try:
            update_status_message()
        except Exception as e:
            print(
                f"ОШИБКА обновления статуса: "
                f"{type(e).__name__}: {e}"
            )

        time.sleep(STATUS_UPDATE_INTERVAL_SECONDS)


# ============================================================
# ВРЕМЯ ПОСТА
# ============================================================

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


# ============================================================
# ПРОВЕРКА КАНАЛА
# ============================================================

def check_updates():
    now = utc_now()

    with state_lock:
        state["last_check_at"] = now
        state["total_checks"] += 1

    try:
        print("----- НАЧАЛО ПРОВЕРКИ -----")
        print("Отправляю запрос на t.me...")

        response = session.get(
            SOURCE_URL,
            timeout=REQUEST_TIMEOUT
        )

        print(
            f"Ответ t.me: "
            f"{response.status_code}, "
            f"{len(response.text)} bytes"
        )

        with state_lock:
            state["last_source_status"] = response.status_code

        if response.status_code != 200:
            print(
                f"Telegram channel вернул HTTP "
                f"{response.status_code}"
            )

            with state_lock:
                state["failed_checks"] += 1
                state["consecutive_errors"] += 1

            return

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

        with state_lock:
            state["successful_checks"] += 1
            state["consecutive_errors"] = 0
            state["last_successful_check_at"] = utc_now()

        now = utc_now()

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

            if KEYWORD not in post_text.lower():
                continue

            print(
                f"🚨 НАЙДЕНО УПОМИНАНИЕ "
                f"{KEYWORD.upper()}: {post_id}"
            )

            alert_text = (
                "🚨 ВНИМАНИЕ!\n\n"
                "Обнаружено упоминание БпЛА "
                "в официальном Telegram-канале "
                "Повітряних Сил України:\n\n"
                f"{post_text}"
            )

            if send_telegram_message(alert_text):

                remember_sent_message(post_id)

                with state_lock:
                    state["last_alert_at"] = utc_now()
                    state["last_alert_id"] = post_id

        print("----- ПРОВЕРКА ЗАВЕРШЕНА -----")

    except requests.exceptions.Timeout:
        print("ОШИБКА: запрос к t.me превысил лимит времени")

        with state_lock:
            state["failed_checks"] += 1
            state["consecutive_errors"] += 1

    except requests.exceptions.RequestException as e:
        print(
            f"ОШИБКА сетевого запроса: {e}"
        )

        with state_lock:
            state["failed_checks"] += 1
            state["consecutive_errors"] += 1

    except Exception as e:
        print(
            f"ОШИБКА ПАРСЕРА: "
            f"{type(e).__name__}: {e}"
        )

        with state_lock:
            state["failed_checks"] += 1
            state["consecutive_errors"] += 1


# ============================================================
# ОСНОВНОЙ ПАРСЕР
# ============================================================

def run_bot():
    print("==============================")
    print("ФОНОВЫЙ ПАРСЕР ЗАПУЩЕН")
    print(f"Ключевое слово: {KEYWORD.upper()}")
    print(
        f"Проверка каждые "
        f"{CHECK_INTERVAL_SECONDS} секунд"
    )
    print(
        f"Максимальный возраст сообщения: "
        f"{MAX_MESSAGE_AGE_MINUTES} минут"
    )
    print("==============================")

    with state_lock:
        state["parser_running"] = True
        state["started_at"] = utc_now()

    test_telegram_api()

    # Проверяем / создаём статус
    try:
        ensure_status_message()
    except Exception as e:
        print(
            f"Ошибка создания статуса: "
            f"{type(e).__name__}: {e}"
        )

    while True:
        try:
            check_updates()

        except Exception as e:
            # Последняя страховка.
            # Парсер НЕ должен умереть из-за одной ошибки.
            print(
                f"КРИТИЧЕСКАЯ ОШИБКА ЦИКЛА: "
                f"{type(e).__name__}: {e}"
            )

            with state_lock:
                state["failed_checks"] += 1
                state["consecutive_errors"] += 1

        time.sleep(CHECK_INTERVAL_SECONDS)


# ============================================================
# WEB ENDPOINTS
# ============================================================

@app.route("/")
def home():
    return "Lukas_Alarm_bot is active!"


@app.route("/health")
def health():
    return "OK"


@app.route("/status")
def status():
    return (
        "<pre>"
        + build_status_text()
        + "</pre>"
    )


@app.route("/test")
def web_test():
    print("=== ВЕБ-ТЕСТОВАЯ ОТПРАВКА ===")

    message = (
        "🔔 ВЕБ-ТЕСТ\n\n"
        "Lukas_Alarm работает.\n"
        "Проверка отправки через веб-интерфейс."
    )

    success = send_telegram_message(message)

    if success:
        return (
            "SUCCESS: тестовое уведомление "
            "отправлено в Telegram."
        )

    return (
        "ERROR: Telegram не принял "
        "тестовое уведомление."
    )


# ============================================================
# ЗАПУСК ФОНОВЫХ ПОТОКОВ
# ============================================================

print("Запускаю фоновые потоки...")

threading.Thread(
    target=run_bot,
    daemon=True,
    name="telegram-monitor"
).start()

threading.Thread(
    target=status_loop,
    daemon=True,
    name="status-updater"
).start()

threading.Thread(
    target=telegram_command_listener,
    daemon=True,
    name="telegram-commands"
).start()


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    port = int(
        os.environ.get("PORT", 10000)
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
