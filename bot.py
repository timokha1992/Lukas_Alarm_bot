import os
import threading
import time
import socket
from collections import deque
from datetime import datetime, timezone, timedelta

from bs4 import BeautifulSoup
from flask import Flask
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


app = Flask(__name__)


# ============================================================
# НАСТРОЙКИ
# ============================================================

TELEGRAM_TOKEN = (os.environ.get("TELEGRAM_TOKEN") or "").strip()
CHAT_ID = (os.environ.get("CHAT_ID") or "").strip()

URL = "https://t.me/s/kpszsu"

# Пока тестируем БпЛА
KEYWORD = "бпл"

# Максимальный возраст сообщения, которое можно отправить
MAX_MESSAGE_AGE_MINUTES = 5

# Интервал между проверками канала
CHECK_INTERVAL_SECONDS = 15

# Сколько последних обработанных сообщений держим в памяти
MAX_SENT_MESSAGES = 1000


# ============================================================
# СОСТОЯНИЕ БОТА
# ============================================================

sent_messages = set()
sent_messages_order = deque(maxlen=MAX_SENT_MESSAGES)

state_lock = threading.Lock()

bot_started_at = datetime.now(timezone.utc)

last_check_at = None
last_successful_check_at = None
last_alert_at = None
last_alert_id = None

total_checks = 0
successful_checks = 0
failed_checks = 0
consecutive_errors = 0

parser_running = False


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

retry_strategy = Retry(
    total=2,
    connect=2,
    read=2,
    status=2,
    backoff_factor=1,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET"]),
    respect_retry_after_header=True,
)

adapter = HTTPAdapter(
    max_retries=retry_strategy
)

session.mount("https://", adapter)
session.mount("http://", adapter)


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram_message(message):
    print("Пробую отправить уведомление в Telegram...")

    if not TELEGRAM_TOKEN:
        print("ОШИБКА: TELEGRAM_TOKEN не задан")
        return False

    if not CHAT_ID:
        print("ОШИБКА: CHAT_ID не задан")
        return False

    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": CHAT_ID,
        "text": message
    }

    # Делаем несколько попыток.
    # Это особенно важно при кратковременном сбое сети.
    for attempt in range(1, 4):

        try:
            print(
                f"Попытка отправки в Telegram "
                f"{attempt}/3..."
            )

            response = requests.post(
                url,
                json=payload,
                timeout=(3, 10)
            )

            print(
                f"Telegram sendMessage: "
                f"{response.status_code} "
                f"{response.text[:500]}"
            )

            if response.status_code == 200:
                print(
                    "Уведомление успешно отправлено"
                )
                return True

            # Telegram может попросить подождать
            if response.status_code == 429:

                try:
                    data = response.json()

                    retry_after = (
                        data
                        .get("parameters", {})
                        .get("retry_after", 2)
                    )

                except Exception:
                    retry_after = 2

                retry_after = min(
                    max(int(retry_after), 1),
                    30
                )

                print(
                    f"Telegram попросил повторить "
                    f"через {retry_after} сек."
                )

                if attempt < 3:
                    time.sleep(retry_after)
                    continue

                return False

            # Временная ошибка Telegram
            if response.status_code in (
                500,
                502,
                503,
                504
            ):

                if attempt < 3:
                    delay = attempt * 2

                    print(
                        f"Временная ошибка Telegram. "
                        f"Повтор через {delay} сек."
                    )

                    time.sleep(delay)
                    continue

            print(
                "Telegram НЕ принял сообщение"
            )

            return False

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError
        ) as e:

            print(
                f"Временная ошибка сети при "
                f"отправке в Telegram: {e}"
            )

            if attempt < 3:
                delay = attempt * 2

                print(
                    f"Повтор отправки через "
                    f"{delay} сек."
                )

                time.sleep(delay)
                continue

            print(
                "Все попытки отправки исчерпаны"
            )
            return False

        except Exception as e:

            print(
                f"ОШИБКА отправки в Telegram: "
                f"{type(e).__name__}: {e}"
            )

            return False

    return False


# ============================================================
# ПРОВЕРКА TELEGRAM API
# ============================================================

def test_telegram_api():

    print(
        "Проверяю доступ к Telegram Bot API..."
    )

    if not TELEGRAM_TOKEN:
        print(
            "ОШИБКА: TELEGRAM_TOKEN не задан"
        )
        return False

    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/getMe"
    )

    for attempt in range(1, 3):

        try:

            response = requests.get(
                url,
                timeout=(3, 5)
            )

            print(
                f"Telegram Bot API: "
                f"{response.status_code} "
                f"{response.text[:500]}"
            )

            if response.status_code == 200:
                print(
                    "Telegram Bot API доступен"
                )
                return True

            print(
                "Telegram Bot API вернул ошибку"
            )

            if attempt < 2:
                time.sleep(2)

        except Exception as e:

            print(
                f"ОШИБКА доступа к Telegram Bot API: "
                f"{type(e).__name__}: {e}"
            )

            if attempt < 2:
                time.sleep(2)

    return False


# ============================================================
# ВРЕМЯ ПОСТА
# ============================================================

def get_post_datetime(post):

    time_elem = post.find("time")

    if not time_elem:
        return None

    datetime_value = time_elem.get(
        "datetime"
    )

    if not datetime_value:
        return None

    try:

        post_time = datetime.fromisoformat(
            datetime_value.replace(
                "Z",
                "+00:00"
            )
        )

        if post_time.tzinfo is None:
            post_time = post_time.replace(
                tzinfo=timezone.utc
            )

        return post_time

    except Exception as e:

        print(
            f"Ошибка определения времени поста: "
            f"{e}"
        )

        return None


# ============================================================
# РАБОТА С ДУБЛЯМИ
# ============================================================

def is_message_sent(post_id):

    with state_lock:
        return post_id in sent_messages


def remember_sent_message(post_id):

    with state_lock:

        if post_id in sent_messages:
            return

        # deque с maxlen автоматически
        # удаляет старые элементы.
        if len(sent_messages_order) >= MAX_SENT_MESSAGES:

            oldest = sent_messages_order[0]

            sent_messages.discard(
                oldest
            )

        sent_messages.add(post_id)
        sent_messages_order.append(
            post_id
        )


# ============================================================
# ОСНОВНАЯ ПРОВЕРКА КАНАЛА
# ============================================================

def check_updates():

    global last_check_at
    global last_successful_check_at
    global last_alert_at
    global last_alert_id
    global total_checks
    global successful_checks
    global failed_checks
    global consecutive_errors

    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/151.0.0.0 "
            "Safari/537.36"
        )
    }

    with state_lock:
        last_check_at = datetime.now(
            timezone.utc
        )
        total_checks += 1

    try:

        print(
            "----- НАЧАЛО ПРОВЕРКИ -----"
        )

        print(
            "Отправляю запрос на t.me..."
        )

        response = session.get(
            URL,
            headers=headers,
            timeout=(3, 10)
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

            with state_lock:
                failed_checks += 1
                consecutive_errors += 1

            return

        print(
            "Разбираю HTML страницы..."
        )

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

        now = datetime.now(
            timezone.utc
        )

        cutoff_time = (
            now -
            timedelta(
                minutes=MAX_MESSAGE_AGE_MINUTES
            )
        )

        # Сначала считаем проверку успешной.
        with state_lock:
            successful_checks += 1
            consecutive_errors = 0
            last_successful_check_at = now

        for post in posts:

            post_id = post.get(
                "data-post",
                ""
            )

            if not post_id:
                continue

            if is_message_sent(post_id):
                continue

            post_time = get_post_datetime(
                post
            )

            if post_time is None:
                continue

            # Старые сообщения НЕ отправляем.
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
                f"🚨 НАЙДЕНО УПОМИНАНИЕ БпЛА: "
                f"{post_id}"
            )

            # Не допускаем слишком длинное
            # сообщение Telegram.
            safe_text = post_text[:3500]

            alert_text = (
                "🚨 ВНИМАНИЕ!\n\n"
                "Обнаружено упоминание БпЛА "
                "в официальном Telegram-канале "
                "Повітряних Сил України:\n\n"
                f"{safe_text}"
            )

            if send_telegram_message(
                alert_text
            ):

                remember_sent_message(
                    post_id
                )

                with state_lock:
                    last_alert_at = now
                    last_alert_id = post_id

                print(
                    f"Сообщение {post_id} "
                    f"помечено как отправленное."
                )

        print(
            "----- ПРОВЕРКА ЗАВЕРШЕНА -----"
        )

    except requests.exceptions.Timeout:

        print(
            "ОШИБКА: запрос к t.me "
            "превысил лимит времени"
        )

        with state_lock:
            failed_checks += 1
            consecutive_errors += 1

    except requests.exceptions.RequestException as e:

        print(
            f"ОШИБКА сетевого запроса: {e}"
        )

        with state_lock:
            failed_checks += 1
            consecutive_errors += 1

    except Exception as e:

        print(
            f"ОШИБКА ПАРСЕРА: "
            f"{type(e).__name__}: {e}"
        )

        with state_lock:
            failed_checks += 1
            consecutive_errors += 1


# ============================================================
# ОСНОВНОЙ ЦИКЛ
# ============================================================

def run_bot():

    global parser_running

    print(
        "=============================="
    )

    print(
        "ФОНОВЫЙ ПАРСЕР ЗАПУЩЕН"
    )

    print(
        "Ключевое слово: БпЛА"
    )

    print(
        f"Проверка каждые "
        f"{CHECK_INTERVAL_SECONDS} секунд"
    )

    print(
        f"Максимальный возраст сообщения: "
        f"{MAX_MESSAGE_AGE_MINUTES} минут"
    )

    print(
        "=============================="
    )

    # Проверяем API, но НЕ останавливаем
    # запуск парсера, если Telegram временно
    # недоступен.
    test_telegram_api()

    while True:

        try:

            with state_lock:
                parser_running = True

            check_updates()

            time.sleep(
                CHECK_INTERVAL_SECONDS
            )

        except Exception as e:

            # Это дополнительная защита.
            # Даже если что-то неожиданно
            # выскочит за пределами check_updates(),
            # основной поток не погибнет.
            print(
                "КРИТИЧЕСКАЯ ОШИБКА "
                "ОСНОВНОГО ЦИКЛА:"
            )

            print(
                f"{type(e).__name__}: {e}"
            )

            with state_lock:
                parser_running = False
                failed_checks += 1
                consecutive_errors += 1

            print(
                "Парсер будет автоматически "
                "перезапущен через 5 секунд."
            )

            time.sleep(5)


# ============================================================
# FLASK
# ============================================================

@app.route("/")
def home():

    return (
        "Lukas_Alarm_bot is active!"
    )


@app.route("/health")
def health():

    # ВАЖНО:
    # этот endpoint должен оставаться быстрым
    # и возвращать 200 для UptimeRobot.
    return "OK"


@app.route("/status")
def status():

    now = datetime.now(
        timezone.utc
    )

    with state_lock:

        uptime_seconds = int(
            (
                now -
                bot_started_at
            ).total_seconds()
        )

        return (
            "<h2>Lukas_Alarm status</h2>"
            f"<p>Parser running: "
            f"<b>{parser_running}</b></p>"
            f"<p>Total checks: "
            f"<b>{total_checks}</b></p>"
            f"<p>Successful checks: "
            f"<b>{successful_checks}</b></p>"
            f"<p>Failed checks: "
            f"<b>{failed_checks}</b></p>"
            f"<p>Consecutive errors: "
            f"<b>{consecutive_errors}</b></p>"
            f"<p>Last check: "
            f"<b>{last_check_at}</b></p>"
            f"<p>Last successful check: "
            f"<b>{last_successful_check_at}</b></p>"
            f"<p>Last alert: "
            f"<b>{last_alert_at}</b></p>"
            f"<p>Last alert ID: "
            f"<b>{last_alert_id}</b></p>"
            f"<p>Uptime: "
            f"<b>{uptime_seconds} sec</b></p>"
            f"<p>Remembered messages: "
            f"<b>{len(sent_messages)}</b></p>"
        )


@app.route("/test")
def test():

    results = []

    # --------------------------------------------------------
    # DNS
    # --------------------------------------------------------

    try:

        start = time.time()

        ip = socket.gethostbyname(
            "api.telegram.org"
        )

        elapsed = round(
            time.time() - start,
            2
        )

        results.append(
            f"DNS OK: api.telegram.org -> "
            f"{ip}, time={elapsed}s"
        )

    except Exception as e:

        results.append(
            f"DNS ERROR: "
            f"{type(e).__name__}: {e}"
        )

        return "<br>".join(results)


    # --------------------------------------------------------
    # TCP
    # --------------------------------------------------------

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
            f"TCP OK: api.telegram.org:443, "
            f"time={elapsed}s"
        )

    except Exception as e:

        results.append(
            f"TCP ERROR: "
            f"{type(e).__name__}: {e}"
        )

        return "<br>".join(results)


    # --------------------------------------------------------
    # BOT API
    # --------------------------------------------------------

    try:

        start = time.time()

        response = requests.get(
            f"https://api.telegram.org/"
            f"bot{TELEGRAM_TOKEN}/getMe",
            timeout=(3, 5)
        )

        elapsed = round(
            time.time() - start,
            2
        )

        results.append(
            f"BOT API: HTTP "
            f"{response.status_code}, "
            f"time={elapsed}s"
        )

        results.append(
            f"BOT RESPONSE: "
            f"{response.text[:500]}"
        )

    except Exception as e:

        elapsed = round(
            time.time() - start,
            2
        )

        results.append(
            f"BOT API ERROR: "
            f"{type(e).__name__}: {e}, "
            f"time={elapsed}s"
        )


    return "<br>".join(results)


# ============================================================
# ЗАПУСК ФОНОВОГО ПАРСЕРА
# ============================================================

print(
    "Запускаю фоновый поток..."
)

threading.Thread(
    target=run_bot,
    daemon=True,
    name="telegram-monitor"
).start()


# ============================================================
# ЗАПУСК FLASK
# ============================================================

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
