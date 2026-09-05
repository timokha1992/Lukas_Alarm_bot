import os
import time
import threading
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from flask import Flask


# ============================================================
# НАСТРОЙКИ
# ============================================================

SOURCE_URL = "https://t.me/s/kpszsu"

KEYWORD = "бпл"

CHECK_INTERVAL_SECONDS = 15
STATUS_UPDATE_INTERVAL_SECONDS = 60
MAX_MESSAGE_AGE_MINUTES = 5

MAX_SENT_MESSAGES = 1000

REQUEST_TIMEOUT = (5, 15)

KYIV_TZ = ZoneInfo("Europe/Kyiv")


# ============================================================
# ENV
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID_RAW = os.getenv("CHAT_ID")

if not TELEGRAM_TOKEN:
    raise RuntimeError("Не задан TELEGRAM_TOKEN")

if not CHAT_ID_RAW:
    raise RuntimeError("Не задан CHAT_ID")

try:
    CHAT_ID = int(CHAT_ID_RAW)
except ValueError:
    raise RuntimeError("CHAT_ID должен быть числом")


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def index():
    return "Lukas Alarm Bot is active", 200


@app.route("/health")
def health():
    return "OK", 200


# ============================================================
# СОСТОЯНИЕ
# ============================================================

state = {
    "parser_running": False,
    "telegram_api_ok": False,
    "source_ok": False,

    "last_check": None,
    "last_alert": None,

    "status_message_id": None,

    "started_at": None,
}

state_lock = threading.Lock()


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36"
    )
})


# ============================================================
# DEDUP
# ============================================================

sent_messages = set()
sent_messages_lock = threading.Lock()


# ============================================================
# ВРЕМЯ
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def format_time(dt):
    if not dt:
        return "—"

    try:
        return dt.astimezone(KYIV_TZ).strftime("%H:%M:%S")
    except Exception:
        return "—"


def format_datetime(dt):
    if not dt:
        return "—"

    try:
        return dt.astimezone(KYIV_TZ).strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        return "—"


# ============================================================
# TELEGRAM API
# ============================================================

def telegram_request(method, data=None, retries=3):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"

    if data is None:
        data = {}

    for attempt in range(retries):
        try:
            response = session.post(
                url,
                data=data,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                try:
                    retry_after = response.json().get(
                        "parameters", {}
                    ).get("retry_after", 5)
                except Exception:
                    retry_after = 5

                time.sleep(min(int(retry_after) + 1, 30))
                continue

            if response.status_code >= 500:
                time.sleep(2 + attempt)
                continue

            response.raise_for_status()

            result = response.json()

            if not result.get("ok"):
                raise RuntimeError(
                    f"Telegram API error: {result}"
                )

            with state_lock:
                state["telegram_api_ok"] = True

            return result

        except Exception as e:
            if attempt == retries - 1:
                with state_lock:
                    state["telegram_api_ok"] = False

                print(
                    f"Telegram API ошибка после {retries} попыток: {e}",
                    flush=True,
                )
                return None

            time.sleep(2 + attempt)

    return None


def send_telegram_message(text):
    result = telegram_request(
        "sendMessage",
        {
            "chat_id": CHAT_ID,
            "text": text,
        },
    )

    if not result:
        return None

    try:
        return result["result"]["message_id"]
    except Exception:
        return None


def edit_telegram_message(message_id, text):
    if not message_id:
        return False

    result = telegram_request(
        "editMessageText",
        {
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "text": text,
        },
    )

    return result is not None


def test_telegram_api():
    result = telegram_request(
        "getMe",
        {},
    )

    return result is not None


# ============================================================
# PINNED STATUS
# ============================================================

STATUS_TITLE = "🛠️ СОСТОЯНИЕ СИСТЕМЫ"


def get_pinned_message_id():
    result = telegram_request(
        "getChat",
        {
            "chat_id": CHAT_ID,
        },
    )

    if not result:
        return None

    try:
        pinned = result["result"].get("pinned_message")

        if not pinned:
            return None

        message_id = pinned.get("message_id")
        text = pinned.get("text", "")

        if text.startswith(STATUS_TITLE):
            return message_id

    except Exception as e:
        print(
            f"Ошибка определения закреплённого сообщения: {e}",
            flush=True,
        )

    return None


def build_status_text():
    with state_lock:
        parser_running = state["parser_running"]
        telegram_api_ok = state["telegram_api_ok"]
        source_ok = state["source_ok"]

        last_check = state["last_check"]
        last_alert = state["last_alert"]

    parser_text = "🟢 РАБОТАЕТ" if parser_running else "🔴 ОСТАНОВЛЕН"
    telegram_text = "🟢 OK" if telegram_api_ok else "🔴 ОШИБКА"
    source_text = "🟢 OK" if source_ok else "🔴 ОШИБКА"

    return (
        f"{STATUS_TITLE}\n"
        f"\n"
        f"🟢 Парсер: {parser_text}\n"
        f"🟢 Telegram API: {telegram_text}\n"
        f"🟢 Источник: {source_text}\n"
        f"\n"
        f"🔎 Ключевое слово: БПЛА\n"
        f"⏱ Проверка: каждые 15 сек.\n"
        f"\n"
        f"🕐 Последняя проверка: {format_time(last_check)}\n"
        f"🚨 Последняя тревога: {format_time(last_alert)}"
    )


def ensure_status_message():
    existing_id = get_pinned_message_id()

    if existing_id:
        with state_lock:
            state["status_message_id"] = existing_id

        update_status_message()
        return True

    text = build_status_text()

    message_id = send_telegram_message(text)

    if not message_id:
        print(
            "Не удалось создать сообщение состояния.",
            flush=True,
        )
        return False

    with state_lock:
        state["status_message_id"] = message_id

    pin_result = telegram_request(
        "pinChatMessage",
        {
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "disable_notification": True,
        },
    )

    if not pin_result:
        print(
            "Сообщение состояния создано, но закрепить его не удалось.",
            flush=True,
        )

    return True


def update_status_message():
    with state_lock:
        message_id = state["status_message_id"]

    if not message_id:
        return

    text = build_status_text()

    success = edit_telegram_message(
        message_id,
        text,
    )

    if not success:
        print(
            "Не удалось обновить сообщение состояния.",
            flush=True,
        )


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

def is_group_admin(user_id):
    result = telegram_request(
        "getChatMember",
        {
            "chat_id": CHAT_ID,
            "user_id": user_id,
        },
    )

    if not result:
        return False

    try:
        status = result["result"]["status"]

        return status in (
            "creator",
            "administrator",
        )

    except Exception:
        return False


def handle_test_command(message):
    chat = message.get("chat", {})
    sender = message.get("from", {})

    chat_id = chat.get("id")
    user_id = sender.get("id")

    if chat_id != CHAT_ID:
        return

    if not user_id:
        return

    if not is_group_admin(user_id):
        print(
            f"Команда /test отклонена: пользователь {user_id} не администратор.",
            flush=True,
        )
        return

    print(
        f"Получена команда /test от администратора {user_id}.",
        flush=True,
    )

    test_text = (
        "🔔 ТЕСТОВОЕ УВЕДОМЛЕНИЕ\n"
        "\n"
        "Бот получил команду /test.\n"
        "Связь с Telegram и рабочей группой проверена."
    )

    send_telegram_message(test_text)


def telegram_command_listener():
    print(
        "Запускаю обработчик Telegram-команд...",
        flush=True,
    )

    # Если раньше был установлен webhook,
    # переключаемся на long polling.
    telegram_request(
        "deleteWebhook",
        {
            "drop_pending_updates": False,
        },
    )

    offset = None

    # Убираем старые накопившиеся команды при запуске,
    # чтобы после перезапуска старый /test не сработал неожиданно.
    try:
        result = telegram_request(
            "getUpdates",
            {
                "offset": -1,
                "timeout": 0,
            },
        )

        if result and result.get("result"):
            last_update = result["result"][-1]
            offset = last_update["update_id"] + 1

    except Exception as e:
        print(
            f"Ошибка очистки старых Telegram-команд: {e}",
            flush=True,
        )

    while True:
        try:
            data = {
                "timeout": 20,
                "allowed_updates": '["message"]',
            }

            if offset is not None:
                data["offset"] = offset

            result = telegram_request(
                "getUpdates",
                data,
                retries=2,
            )

            if not result:
                time.sleep(2)
                continue

            updates = result.get("result", [])

            for update in updates:
                offset = update["update_id"] + 1

                message = update.get("message")

                if not message:
                    continue

                text = message.get("text", "").strip()

                if not text:
                    continue

                # Обрабатываем /test и варианты вида /test@BotName
                command = text.split()[0].lower()

                if command.startswith("/test"):
                    if command == "/test" or command.startswith("/test@"):
                        handle_test_command(message)

        except Exception as e:
            print(
                f"Ошибка обработчика Telegram-команд: {e}",
                flush=True,
            )

            time.sleep(5)


# ============================================================
# STATUS LOOP
# ============================================================

def status_loop():
    print(
        "Запущено обновление статуса каждые 60 секунд",
        flush=True,
    )

    while True:
        try:
            update_status_message()
        except Exception as e:
            print(
                f"Ошибка обновления статуса: {e}",
                flush=True,
            )

        time.sleep(STATUS_UPDATE_INTERVAL_SECONDS)


# ============================================================
# ПАРСИНГ ДАТЫ
# ============================================================

def get_post_datetime(element):
    try:
        time_element = element.select_one("time")

        if not time_element:
            return None

        value = time_element.get("datetime")

        if not value:
            return None

        dt = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    except Exception:
        return None


# ============================================================
# ПРОВЕРКА ИСТОЧНИКА
# ============================================================

def check_updates():
    check_time = now_utc()

    with state_lock:
        state["last_check"] = check_time

    try:
        response = session.get(
            SOURCE_URL,
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code != 200:
            with state_lock:
                state["source_ok"] = False

            print(
                f"Источник вернул HTTP {response.status_code}",
                flush=True,
            )
            return

        with state_lock:
            state["source_ok"] = True

        soup = BeautifulSoup(
            response.text,
            "html.parser",
        )

        posts = soup.select(
            ".tgme_widget_message"
        )

        if not posts:
            return

        current_time = now_utc()

        # Сначала смотрим самые свежие сообщения.
        posts = list(reversed(posts))

        for post in posts:
            post_datetime = get_post_datetime(post)

            if not post_datetime:
                continue

            age_seconds = (
                current_time - post_datetime
            ).total_seconds()

            # Если сообщение ещё новее текущего времени
            # из-за особенностей часов/парсинга — не отбрасываем.
            if age_seconds < 0:
                age_seconds = 0

            # Старше установленного окна — не рассматриваем.
            if age_seconds > MAX_MESSAGE_AGE_MINUTES * 60:
                # Так как сообщения идут от новых к старым,
                # дальше можно прекращать поиск.
                break

            text_element = post.select_one(
                ".tgme_widget_message_text"
            )

            if not text_element:
                continue

            text = text_element.get_text(
                "\n",
                strip=True,
            )

            if not text:
                continue

            if KEYWORD not in text.lower():
                continue

            # Уникальный ID сообщения канала.
            post_id = None

            try:
                post_id = post.get("data-post")
            except Exception:
                pass

            if not post_id:
                continue

            with sent_messages_lock:
                if post_id in sent_messages:
                    continue

                sent_messages.add(post_id)

                if len(sent_messages) > MAX_SENT_MESSAGES:
                    # Удаляем произвольный старый элемент,
                    # чтобы память не росла бесконечно.
                    sent_messages.pop()

            alert_text = (
                "🚨 ТРЕВОГА\n"
                "\n"
                f"{text}"
            )

            message_id = send_telegram_message(
                alert_text
            )

            if message_id:
                with state_lock:
                    state["last_alert"] = now_utc()

                print(
                    f"ОТПРАВЛЕНА ТРЕВОГА: {post_id}",
                    flush=True,
                )

            # После найденного свежего сообщения с ключевым
            # словом можно продолжать поиск остальных свежих
            # сообщений, если они появились почти одновременно.

    except Exception as e:
        with state_lock:
            state["source_ok"] = False

        print(
            f"Ошибка проверки источника: {e}",
            flush=True,
        )


# ============================================================
# ОСНОВНОЙ ПАРСЕР
# ============================================================

def run_bot():
    print(
        "ФОНОВЫЙ ПАРСЕР ЗАПУЩЕН",
        flush=True,
    )

    print(
        "Ключевое слово: БПЛА",
        flush=True,
    )

    print(
        "Проверка каждые 15 секунд",
        flush=True,
    )

    print(
        "Максимальный возраст сообщения: 5 минут",
        flush=True,
    )

    with state_lock:
        state["parser_running"] = True
        state["started_at"] = now_utc()

    print(
        "Проверяю доступ к Telegram Bot API...",
        flush=True,
    )

    api_ok = test_telegram_api()

    if api_ok:
        print(
            "Telegram Bot API: OK",
            flush=True,
        )
    else:
        print(
            "Telegram Bot API: ОШИБКА",
            flush=True,
        )

    print(
        "Проверяю сообщение состояния системы...",
        flush=True,
    )

    ensure_status_message()

    while True:
        try:
            check_updates()

        except Exception as e:
            # Очень важно:
            # ошибка одной проверки НЕ должна убить поток парсера.
            with state_lock:
                state["source_ok"] = False

            print(
                f"Ошибка цикла парсера: {e}",
                flush=True,
            )

        time.sleep(CHECK_INTERVAL_SECONDS)


# ============================================================
# ЗАПУСК ФОНОВЫХ ПОТОКОВ
# ============================================================

parser_thread = threading.Thread(
    target=run_bot,
    name="telegram-monitor",
    daemon=True,
)

parser_thread.start()


status_thread = threading.Thread(
    target=status_loop,
    name="status-updater",
    daemon=True,
)

status_thread.start()


commands_thread = threading.Thread(
    target=telegram_command_listener,
    name="telegram-commands",
    daemon=True,
)

commands_thread.start()


# ============================================================
# LOCAL START
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
    )
