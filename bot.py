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


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# НАСТРОЙКИ
# ============================================================

TELEGRAM_TOKEN = (
    os.environ.get("TELEGRAM_TOKEN") or ""
).strip()

CHAT_ID = (
    os.environ.get("CHAT_ID") or ""
).strip()

URL = "https://t.me/s/kpszsu"

# Пока тестируем БпЛА.
# Позже поменяем на нужное ключевое слово.
KEYWORD = "бпл"

# Максимальный возраст сообщения,
# которое разрешено отправлять.
MAX_MESSAGE_AGE_MINUTES = 5

# Интервал проверки канала.
CHECK_INTERVAL_SECONDS = 15

# Как часто обновлять закреплённый статус.
STATUS_UPDATE_INTERVAL_SECONDS = 60

# Сколько обработанных сообщений держим в памяти.
MAX_SENT_MESSAGES = 1000

# Специальный маркер нашего статусного сообщения.
STATUS_MARKER = "LUKAS_ALARM_STATUS_V1"

# Callback для кнопки теста.
TEST_CALLBACK_DATA = "lukas_test_alert"


# ============================================================
# СОСТОЯНИЕ БОТА
# ============================================================

sent_messages = set()
sent_messages_order = deque(
    maxlen=MAX_SENT_MESSAGES
)

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

# ID статусного сообщения в Telegram.
status_message_id = None

# Последнее время обновления статусного сообщения.
last_status_update_at = None

# Offset для getUpdates.
telegram_update_offset = None


# ============================================================
# ВРЕМЕННАЯ ЗОНА УКРАИНЫ
# ============================================================

# Для отображения времени пользователю.
# В сентябре Украина находится в UTC+3.
UKRAINE_TZ = timezone(
    timedelta(hours=3)
)


def format_time(value):
    """
    Переводит UTC datetime в удобное украинское время.
    """
    if value is None:
        return "нет данных"

    try:
        local_value = value.astimezone(
            UKRAINE_TZ
        )
        return local_value.strftime(
            "%d.%m.%Y %H:%M:%S"
        )
    except Exception:
        return "ошибка времени"


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
    status_forcelist=(
        429,
        500,
        502,
        503,
        504,
    ),
    allowed_methods=frozenset(
        ["GET"]
    ),
    respect_retry_after_header=True,
)

adapter = HTTPAdapter(
    max_retries=retry_strategy
)

session.mount(
    "https://",
    adapter
)

session.mount(
    "http://",
    adapter
)


# ============================================================
# TELEGRAM API
# ============================================================

def telegram_api(
    method,
    payload=None,
    timeout=(3, 10)
):
    """
    Универсальный запрос к Telegram Bot API.
    """
    if not TELEGRAM_TOKEN:
        print(
            "ОШИБКА: TELEGRAM_TOKEN не задан"
        )
        return None

    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/{method}"
    )

    try:
        response = requests.post(
            url,
            json=payload or {},
            timeout=timeout
        )

        print(
            f"Telegram {method}: "
            f"{response.status_code} "
            f"{response.text[:500]}"
        )

        if response.status_code != 200:
            return None

        data = response.json()

        if not data.get("ok"):
            print(
                f"Telegram API error: "
                f"{data}"
            )
            return None

        return data

    except (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
    ) as e:
        print(
            f"Временная ошибка Telegram API "
            f"({method}): {e}"
        )
        return None

    except Exception as e:
        print(
            f"ОШИБКА Telegram API "
            f"({method}): "
            f"{type(e).__name__}: {e}"
        )
        return None


# ============================================================
# ОТПРАВКА СООБЩЕНИЯ В ОСНОВНОЙ ЧАТ
# ============================================================

def send_telegram_message(message):
    """
    Отправляет обычное сообщение
    непосредственно в основной чат.
    """
    print(
        "Пробую отправить уведомление "
        "в Telegram..."
    )

    if not TELEGRAM_TOKEN:
        print(
            "ОШИБКА: TELEGRAM_TOKEN не задан"
        )
        return False

    if not CHAT_ID:
        print(
            "ОШИБКА: CHAT_ID не задан"
        )
        return False

    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": CHAT_ID,
        "text": message,
    }

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
                "Telegram sendMessage: "
                f"{response.status_code} "
                f"{response.text[:500]}"
            )

            if response.status_code == 200:
                print(
                    "Уведомление успешно отправлено"
                )
                return True

            # Telegram попросил подождать.
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
                    "Telegram попросил "
                    f"повторить через "
                    f"{retry_after} сек."
                )

                if attempt < 3:
                    time.sleep(
                        retry_after
                    )
                    continue

                return False

            # Временная ошибка Telegram.
            if response.status_code in (
                500,
                502,
                503,
                504,
            ):

                if attempt < 3:

                    delay = attempt * 2

                    print(
                        "Временная ошибка Telegram. "
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
            requests.exceptions.ConnectionError,
        ) as e:

            print(
                "Временная ошибка сети "
                f"при отправке в Telegram: {e}"
            )

            if attempt < 3:

                delay = attempt * 2

                print(
                    "Повтор отправки через "
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
                "ОШИБКА отправки в Telegram: "
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

    for attempt in range(1, 3):

        try:

            url = (
                "https://api.telegram.org/"
                f"bot{TELEGRAM_TOKEN}/getMe"
            )

            response = requests.get(
                url,
                timeout=(3, 5)
            )

            print(
                "Telegram Bot API: "
                f"{response.status_code} "
                f"{response.text[:500]}"
            )

            if response.status_code == 200:

                print(
                    "Telegram Bot API доступен"
                )

                return True

            print(
                "Telegram Bot API "
                "вернул ошибку"
            )

            if attempt < 2:
                time.sleep(2)

        except Exception as e:

            print(
                "ОШИБКА доступа к "
                "Telegram Bot API: "
                f"{type(e).__name__}: {e}"
            )

            if attempt < 2:
                time.sleep(2)

    return False


# ============================================================
# ПРОВЕРКА АДМИНИСТРАТОРА
# ============================================================

def is_chat_admin(user_id):
    """
    Проверяем, является ли пользователь
    администратором основной группы.

    Кнопка теста доступна только администраторам.
    """

    if not CHAT_ID:
        return False

    data = telegram_api(
        "getChatMember",
        {
            "chat_id": CHAT_ID,
            "user_id": user_id,
        },
        timeout=(3, 5)
    )

    if not data:
        return False

    member = data.get(
        "result",
        {}
    )

    status = member.get(
        "status"
    )

    return status in (
        "administrator",
        "creator",
    )


# ============================================================
# ОТВЕТ НА НАЖАТИЕ КНОПКИ
# ============================================================

def answer_callback_query(
    callback_query_id,
    text
):

    telegram_api(
        "answerCallbackQuery",
        {
            "callback_query_id":
                callback_query_id,
            "text": text,
            "show_alert": False,
        },
        timeout=(3, 5)
    )


# ============================================================
# СОЗДАНИЕ / РЕДАКТИРОВАНИЕ СТАТУСА
# ============================================================

def build_status_text():

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

        current_parser_running = (
            parser_running
        )

        current_total_checks = (
            total_checks
        )

        current_successful_checks = (
            successful_checks
        )

        current_failed_checks = (
            failed_checks
        )

        current_consecutive_errors = (
            consecutive_errors
        )

        current_last_check = (
            last_check_at
        )

        current_last_successful = (
            last_successful_check_at
        )

        current_last_alert = (
            last_alert_at
        )

    if (
        current_parser_running
        and current_consecutive_errors == 0
        and current_successful_checks > 0
    ):
        status_icon = "🟢"
        status_title = (
            "СИСТЕМА РАБОТАЕТ"
        )

    elif current_consecutive_errors > 0:
        status_icon = "🟡"
        status_title = (
            "ЕСТЬ ОШИБКИ"
        )

    else:
        status_icon = "🟡"
        status_title = (
            "СИСТЕМА ЗАПУСКАЕТСЯ"
        )

    text = (
        f"{status_icon} "
        f"<b>Lukas Alarm — "
        f"{status_title}</b>\n\n"

        f"Проверка канала: "
        f"{CHECK_INTERVAL_SECONDS} сек.\n"

        f"Последняя проверка: "
        f"{format_time(current_last_check)}\n"

        f"Последняя успешная проверка: "
        f"{format_time(current_last_successful)}\n"

        f"Последнее оповещение: "
        f"{format_time(current_last_alert)}\n\n"

        f"Проверок всего: "
        f"{current_total_checks}\n"

        f"Успешных: "
        f"{current_successful_checks}\n"

        f"Ошибок: "
        f"{current_failed_checks}\n"

        f"Ошибок подряд: "
        f"{current_consecutive_errors}\n"

        f"Время работы: "
        f"{uptime_seconds // 3600} ч "
        f"{(uptime_seconds % 3600) // 60} мин\n\n"

        f"{STATUS_MARKER}"
    )

    return text


def build_status_keyboard():

    return {
        "inline_keyboard": [
            [
                {
                    "text":
                        "🧪 ПРОВЕРИТЬ ОПОВЕЩЕНИЕ",
                    "callback_data":
                        TEST_CALLBACK_DATA,
                }
            ]
        ]
    }


def create_status_message():

    global status_message_id

    if not CHAT_ID:
        print(
            "Невозможно создать статус: "
            "CHAT_ID не задан."
        )
        return False

    print(
        "Создаю статусное сообщение..."
    )

    data = telegram_api(
        "sendMessage",
        {
            "chat_id": CHAT_ID,
            "text": build_status_text(),
            "parse_mode": "HTML",
            "reply_markup":
                build_status_keyboard(),
        },
        timeout=(3, 10)
    )

    if not data:
        print(
            "Не удалось создать "
            "статусное сообщение."
        )
        return False

    message = data.get(
        "result",
        {}
    )

    message_id = message.get(
        "message_id"
    )

    if not message_id:
        print(
            "Telegram не вернул "
            "message_id статуса."
        )
        return False

    with state_lock:
        status_message_id = message_id

    print(
        "Статусное сообщение создано. "
        f"message_id={message_id}"
    )

    # Пытаемся закрепить.
    pin_status_message(
        message_id
    )

    return True


def pin_status_message(message_id):

    print(
        "Пытаюсь закрепить "
        "статусное сообщение..."
    )

    data = telegram_api(
        "pinChatMessage",
        {
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "disable_notification": True,
        },
        timeout=(3, 10)
    )

    if data:
        print(
            "Статусное сообщение "
            "успешно закреплено."
        )
        return True

    print(
        "Не удалось закрепить "
        "статусное сообщение."
    )

    print(
        "Проверь, что бот является "
        "администратором группы и имеет "
        "право закреплять сообщения."
    )

    return False


def find_existing_status_message():

    global status_message_id

    print(
        "Проверяю, есть ли уже "
        "закреплённый статус..."
    )

    data = telegram_api(
        "getChat",
        {
            "chat_id": CHAT_ID
        },
        timeout=(3, 5)
    )

    if not data:
        print(
            "Не удалось получить "
            "информацию о группе."
        )
        return False

    chat = data.get(
        "result",
        {}
    )

    pinned_message = chat.get(
        "pinned_message"
    )

    if not pinned_message:
        print(
            "Закреплённого сообщения "
            "не найдено."
        )
        return False

    pinned_text = (
        pinned_message.get(
            "text"
        )
        or ""
    )

    if STATUS_MARKER not in pinned_text:
        print(
            "Закреплённое сообщение "
            "не является статусом Lukas Alarm."
        )
        return False

    message_id = pinned_message.get(
        "message_id"
    )

    if not message_id:
        return False

    with state_lock:
        status_message_id = message_id

    print(
        "Нашёл существующий статус. "
        f"message_id={message_id}"
    )

    return True


def ensure_status_message():

    if not TELEGRAM_TOKEN:
        return False

    if not CHAT_ID:
        return False

    # Сначала пытаемся найти уже существующий
    # закреплённый статус.
    if find_existing_status_message():
        return True

    # Если его нет — создаём новый.
    return create_status_message()


def update_status_message(
    force=False
):

    global last_status_update_at

    now = datetime.now(
        timezone.utc
    )

    with state_lock:

        message_id = (
            status_message_id
        )

        previous_update = (
            last_status_update_at
        )

    if not message_id:

        ensure_status_message()

        with state_lock:
            message_id = (
                status_message_id
            )

        if not message_id:
            return False

    if not force and previous_update:

        elapsed = (
            now -
            previous_update
        ).total_seconds()

        if (
            elapsed <
            STATUS_UPDATE_INTERVAL_SECONDS
        ):
            return True

    data = telegram_api(
        "editMessageText",
        {
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "text": build_status_text(),
            "parse_mode": "HTML",
            "reply_markup":
                build_status_keyboard(),
        },
        timeout=(3, 10)
    )

    if data:

        with state_lock:
            last_status_update_at = now

        return True

    print(
        "Не удалось обновить "
        "статусное сообщение."
    )

    # Возможно, сообщение было удалено.
    # Сбрасываем ID и попробуем создать новое.
    with state_lock:
        status_message_id = None

    return False


# ============================================================
# ТЕСТОВОЕ ОПОВЕЩЕНИЕ
# ============================================================

def send_test_alert(user_id):

    print(
        "Получена команда тестирования "
        f"от user_id={user_id}"
    )

    if not is_chat_admin(user_id):

        print(
            "Пользователь не является "
            "администратором. Тест запрещён."
        )

        return False

    now = datetime.now(
        timezone.utc
    )

    test_text = (
        "🧪 <b>ТЕСТОВОЕ ОПОВЕЩЕНИЕ</b>\n\n"
        "Это проверка полного пути "
        "оповещения Lukas Alarm.\n\n"
        f"Время: {format_time(now)}\n\n"
        "Это НЕ реальная тревога."
    )

    data = telegram_api(
        "sendMessage",
        {
            "chat_id": CHAT_ID,
            "text": test_text,
            "parse_mode": "HTML",
        },
        timeout=(3, 10)
    )

    if data:

        print(
            "Тестовое оповещение "
            "успешно отправлено."
        )

        return True

    print(
        "Не удалось отправить "
        "тестовое оповещение."
    )

    return False


# ============================================================
# TELEGRAM CALLBACK POLLING
# ============================================================

def process_callback_query(
    callback_query
):

    callback_id = (
        callback_query.get(
            "id"
        )
    )

    data = (
        callback_query.get(
            "data"
        )
    )

    from_user = (
        callback_query.get(
            "from",
            {}
        )
    )

    user_id = from_user.get(
        "id"
    )

    if data != TEST_CALLBACK_DATA:

        answer_callback_query(
            callback_id,
            "Неизвестная команда."
        )

        return

    if not user_id:

        answer_callback_query(
            callback_id,
            "Не удалось определить пользователя."
        )

        return

    # Сразу подтверждаем нажатие,
    # чтобы Telegram не показывал
    # бесконечную загрузку кнопки.
    if not is_chat_admin(user_id):

        answer_callback_query(
            callback_id,
            "Тест доступен только администраторам группы."
        )

        return

    answer_callback_query(
        callback_id,
        "Запускаю тест оповещения..."
    )

    success = send_test_alert(
        user_id
    )

    if success:

        # После теста обновляем статус.
        update_status_message(
            force=True
        )

    else:

        print(
            "Тестовое оповещение "
            "завершилось ошибкой."
        )


def telegram_updates_loop():

    global telegram_update_offset

    print(
        "=============================="
    )
    print(
        "TELEGRAM CALLBACK POLLING ЗАПУЩЕН"
    )
    print(
        "=============================="
    )

    # Получаем последние обновления,
    # чтобы не проигрывать старые нажатия
    # после перезапуска.
    try:

        initial_data = telegram_api(
            "getUpdates",
            {
                "timeout": 0,
                "limit": 100,
            },
            timeout=(3, 10)
        )

        if initial_data:

            initial_updates = (
                initial_data
                .get("result", [])
            )

            if initial_updates:

                last_update_id = (
                    initial_updates[-1]
                    .get("update_id")
                )

                if (
                    last_update_id
                    is not None
                ):
                    telegram_update_offset = (
                        last_update_id + 1
                    )

                    print(
                        "Пропущены старые "
                        "обновления до offset="
                        f"{telegram_update_offset}"
                    )

    except Exception as e:

        print(
            "Ошибка начальной инициализации "
            "Telegram updates: "
            f"{type(e).__name__}: {e}"
        )

    while True:

        try:

            payload = {
                "timeout": 25,
                "limit": 100,
                "allowed_updates": [
                    "callback_query"
                ],
            }

            if (
                telegram_update_offset
                is not None
            ):
                payload["offset"] = (
                    telegram_update_offset
                )

            data = telegram_api(
                "getUpdates",
                payload,
                timeout=(5, 35)
            )

            if not data:

                time.sleep(2)
                continue

            updates = (
                data
                .get("result", [])
            )

            for update in updates:

                update_id = (
                    update.get(
                        "update_id"
                    )
                )

                if (
                    update_id
                    is not None
                ):
                    telegram_update_offset = (
                        update_id + 1
                    )

                callback_query = (
                    update.get(
                        "callback_query"
                    )
                )

                if callback_query:

                    try:
                        process_callback_query(
                            callback_query
                        )

                    except Exception as e:

                        print(
                            "ОШИБКА обработки "
                            "callback_query: "
                            f"{type(e).__name__}: {e}"
                        )

        except Exception as e:

            print(
                "КРИТИЧЕСКАЯ ОШИБКА "
                "Telegram polling: "
                f"{type(e).__name__}: {e}"
            )

            time.sleep(5)


# ============================================================
# ВРЕМЯ ПОСТА
# ============================================================

def get_post_datetime(post):

    time_elem = post.find(
        "time"
    )

    if not time_elem:
        return None

    datetime_value = (
        time_elem.get(
            "datetime"
        )
    )

    if not datetime_value:
        return None

    try:

        post_time = (
            datetime.fromisoformat(
                datetime_value.replace(
                    "Z",
                    "+00:00"
                )
            )
        )

        if post_time.tzinfo is None:

            post_time = (
                post_time.replace(
                    tzinfo=timezone.utc
                )
            )

        return post_time

    except Exception as e:

        print(
            "Ошибка определения времени "
            f"поста: {e}"
        )

        return None


# ============================================================
# РАБОТА С ДУБЛЯМИ
# ============================================================

def is_message_sent(post_id):

    with state_lock:

        return (
            post_id in sent_messages
        )


def remember_sent_message(post_id):

    with state_lock:

        if post_id in sent_messages:
            return

        # deque с maxlen автоматически
        # удаляет старые элементы.
        if (
            len(sent_messages_order)
            >= MAX_SENT_MESSAGES
        ):

            oldest = (
                sent_messages_order[0]
            )

            sent_messages.discard(
                oldest
            )

        sent_messages.add(
            post_id
        )

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

        last_check_at = (
            datetime.now(
                timezone.utc
            )
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
                "Telegram channel вернул "
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
            "Найдено постов на странице: "
            f"{len(posts)}"
        )

        now = datetime.now(
            timezone.utc
        )

        cutoff_time = (
            now -
            timedelta(
                minutes=
                MAX_MESSAGE_AGE_MINUTES
            )
        )

        # Проверка успешна.
        with state_lock:

            successful_checks += 1

            consecutive_errors = 0

            last_successful_check_at = (
                now
            )

        for post in posts:

            post_id = (
                post.get(
                    "data-post",
                    ""
                )
            )

            if not post_id:
                continue

            if is_message_sent(
                post_id
            ):
                continue

            post_time = (
                get_post_datetime(
                    post
                )
            )

            if post_time is None:
                continue

            # Старые сообщения НЕ отправляем.
            if post_time < cutoff_time:
                continue

            text_elem = post.find(
                "div",
                class_=
                "tgme_widget_message_text"
            )

            if not text_elem:
                continue

            post_text = (
                text_elem.get_text(
                    "\n",
                    strip=True
                )
            )

            if (
                KEYWORD
                not in post_text.lower()
            ):
                continue

            print(
                "🚨 НАЙДЕНО УПОМИНАНИЕ БпЛА: "
                f"{post_id}"
            )

            # Не допускаем слишком длинное
            # сообщение Telegram.
            safe_text = (
                post_text[:3500]
            )

            alert_text = (
                "🚨 <b>ВНИМАНИЕ!</b>\n\n"
                "Обнаружено упоминание БпЛА "
                "в официальном Telegram-канале "
                "Повітряних Сил України:\n\n"
                f"{safe_text}"
            )

            # Отправляем непосредственно
            # в основной чат.
            data = telegram_api(
                "sendMessage",
                {
                    "chat_id": CHAT_ID,
                    "text": alert_text,
                    "parse_mode": "HTML",
                },
                timeout=(3, 10)
            )

            if data:

                remember_sent_message(
                    post_id
                )

                with state_lock:

                    last_alert_at = now

                    last_alert_id = (
                        post_id
                    )

                print(
                    "Сообщение "
                    f"{post_id} "
                    "помечено как отправленное."
                )

                # Обновляем статус сразу
                # после реального оповещения.
                update_status_message(
                    force=True
                )

            else:

                print(
                    "Не удалось отправить "
                    f"оповещение {post_id}. "
                    "Сообщение НЕ помечаем "
                    "как отправленное — "
                    "следующая проверка попробует "
                    "снова."
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
            "ОШИБКА сетевого запроса: "
            f"{e}"
        )

        with state_lock:

            failed_checks += 1

            consecutive_errors += 1

    except Exception as e:

        print(
            "ОШИБКА ПАРСЕРА: "
            f"{type(e).__name__}: {e}"
        )

        with state_lock:

            failed_checks += 1

            consecutive_errors += 1


# ============================================================
# ОСНОВНОЙ ЦИКЛ ПАРСЕРА
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
        "Проверка каждые "
        f"{CHECK_INTERVAL_SECONDS} секунд"
    )

    print(
        "Максимальный возраст сообщения: "
        f"{MAX_MESSAGE_AGE_MINUTES} минут"
    )

    print(
        "=============================="
    )

    test_telegram_api()

    # Создаём/находим статус.
    ensure_status_message()

    while True:

        try:

            with state_lock:
                parser_running = True

            check_updates()

            # Обновляем статус не чаще,
            # чем раз в минуту.
            update_status_message()

            time.sleep(
                CHECK_INTERVAL_SECONDS
            )

        except Exception as e:

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

            # Пытаемся показать проблему
            # в статусе.
            try:

                update_status_message(
                    force=True
                )

            except Exception:
                pass

            print(
                "Парсер будет автоматически "
                "перезапущен через 5 секунд."
            )

            time.sleep(5)


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def home():

    return (
        "Lukas_Alarm_bot is active!"
    )


@app.route("/health")
def health():

    # Этот endpoint должен оставаться
    # быстрым и возвращать 200
    # для UptimeRobot.
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

        current_status_message_id = (
            status_message_id
        )

        current_parser_running = (
            parser_running
        )

        current_total_checks = (
            total_checks
        )

        current_successful_checks = (
            successful_checks
        )

        current_failed_checks = (
            failed_checks
        )

        current_consecutive_errors = (
            consecutive_errors
        )

        current_last_check = (
            last_check_at
        )

        current_last_successful = (
            last_successful_check_at
        )

        current_last_alert = (
            last_alert_at
        )

        current_last_alert_id = (
            last_alert_id
        )

        remembered_count = (
            len(sent_messages)
        )

    return (
        "<h2>Lukas_Alarm status</h2>"

        f"<p>Parser running: "
        f"<b>{current_parser_running}</b></p>"

        f"<p>Total checks: "
        f"<b>{current_total_checks}</b></p>"

        f"<p>Successful checks: "
        f"<b>{current_successful_checks}</b></p>"

        f"<p>Failed checks: "
        f"<b>{current_failed_checks}</b></p>"

        f"<p>Consecutive errors: "
        f"<b>{current_consecutive_errors}</b></p>"

        f"<p>Last check: "
        f"<b>{current_last_check}</b></p>"

        f"<p>Last successful check: "
        f"<b>{current_last_successful}</b></p>"

        f"<p>Last alert: "
        f"<b>{current_last_alert}</b></p>"

        f"<p>Last alert ID: "
        f"<b>{current_last_alert_id}</b></p>"

        f"<p>Status message ID: "
        f"<b>{current_status_message_id}</b></p>"

        f"<p>Uptime: "
        f"<b>{uptime_seconds} sec</b></p>"

        f"<p>Remembered messages: "
        f"<b>{remembered_count}</b></p>"
    )


@app.route("/test")
def technical_test():

    """
    Технический HTTP-тест.
    Оставляем его для диагностики Render,
    DNS и Telegram API.

    Это НЕ тест тревожного сообщения.
    """

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
            "DNS OK: "
            "api.telegram.org -> "
            f"{ip}, time={elapsed}s"
        )

    except Exception as e:

        results.append(
            "DNS ERROR: "
            f"{type(e).__name__}: {e}"
        )

        return "<br>".join(
            results
        )

    # --------------------------------------------------------
    # TCP
    # --------------------------------------------------------

    try:

        start = time.time()

        connection = (
            socket.create_connection(
                (
                    "api.telegram.org",
                    443
                ),
                timeout=5
            )
        )

        connection.close()

        elapsed = round(
            time.time() - start,
            2
        )

        results.append(
            "TCP OK: "
            "api.telegram.org:443, "
            f"time={elapsed}s"
        )

    except Exception as e:

        results.append(
            "TCP ERROR: "
            f"{type(e).__name__}: {e}"
        )

        return "<br>".join(
            results
        )

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
            "BOT API: HTTP "
            f"{response.status_code}, "
            f"time={elapsed}s"
        )

        results.append(
            "BOT RESPONSE: "
            f"{response.text[:500]}"
        )

    except Exception as e:

        elapsed = round(
            time.time() - start,
            2
        )

        results.append(
            "BOT API ERROR: "
            f"{type(e).__name__}: {e}, "
            f"time={elapsed}s"
        )

    return "<br>".join(
        results
    )


# ============================================================
# ЗАПУСК ФОНОВЫХ ПОТОКОВ
# ============================================================

print(
    "Запускаю фоновый поток парсера..."
)

threading.Thread(
    target=run_bot,
    daemon=True,
    name="telegram-monitor"
).start()


print(
    "Запускаю Telegram callback polling..."
)

threading.Thread(
    target=telegram_updates_loop,
    daemon=True,
    name="telegram-callbacks"
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
