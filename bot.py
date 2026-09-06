import os
import time
import html
import json
import re
import threading
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from flask import Flask


# ============================================================
# НАСТРОЙКИ
# ============================================================

PSZSU_URL = "https://t.me/s/kpszsu"
MONITOR_URL = "https://t.me/s/war_monitor"

PSZSU_NAME = "Повітряні Сили ЗС України"
PSZSU_LINK = "https://t.me/kpszsu"

MONITOR_NAME = "monitor"
MONITOR_LINK = "https://t.me/war_monitor"

# Корень для всех форм Кременчуга:
# Кременчук, Кременчука, Кременчуці, Кременчуцький район и т.д.
KEYWORD = "кременч"

CHECK_INTERVAL_SECONDS = 15
STATUS_UPDATE_INTERVAL_SECONDS = 60
WATCHDOG_INTERVAL_SECONDS = 10

PARSER_STALE_AFTER_SECONDS = 60
FAILURE_NOTIFICATION_AFTER_SECONDS = 60
STARTUP_GRACE_SECONDS = 90

MAX_MESSAGE_AGE_MINUTES = 5
MAX_SENT_MESSAGES = 1000

# Telegram /test использует long polling до 20 секунд.
REQUEST_TIMEOUT = (5, 35)
SOURCE_REQUEST_TIMEOUT = (5, 15)

PARSER_HEARTBEAT_FILE = "/tmp/lukas_alarm_parser_heartbeat"

KYIV_TZ = ZoneInfo("Europe/Kyiv")


# ============================================================
# ФИЛЬТР MONITOR
# ============================================================

# Высокоскоростная угроза.
# Все эти варианты относятся к одному классу HIGH_SPEED_THREAT.
HIGH_SPEED_PATTERNS = (
    "балістик",
    "балістична ракета",
    "балістичні ракети",
    "балістичне озброєння",
    "аеробалістик",
    "аеробалістична",
    "аеробалістичні",
    "кинжал",
    "кинджал",
    "циркон",
    "3м22",
    "х-47м2",
    "швидкісна ціль",
    "швидкісні цілі",
)

# Для аббревиатуры БР используем отдельную проверку границ слова.
# Это не обычный substring-поиск.
BR_PATTERN = "бр"

# Сообщения о продолжении угрозы сами по себе не отправляем.
# Они рассматриваются как информационное сопровождение.
CONTINUING_PATTERNS = (
    "загроза балістики триває",
    "триває загроза балістики",
    "загроза балістична триває",
    "триває загроза",
)

# Явные признаки итоговых сводок / постфактумной статистики.
# ВАЖНО: эти признаки применяем к HIGH_SPEED_THREAT.
# IMPACT_CONFIRMED проверяется раньше, поэтому оперативное
# сообщение о взрыве не теряется только из-за фразы
# "було застосовано ...".
POST_EVENT_PATTERNS = (
    "#зведення",
    "зведення",
    "у ніч на",
    "в ніч на",
    "за ніч",
    "за останню ніч",
    "згідно зі звітом",
    "згідно зі звітом повітряних сил",
    "підсумки",
    "після атаки",
    "загалом було запущено",
    "загалом було застосовано",
    "всього було застосовано",
    "всього знищено",
    "знешкоджено",
)

# Признаки события / взрыва.
# Если есть такая формулировка + Кременчугская география,
# отправляем отдельное подтверждение независимо от причины.
IMPACT_PATTERNS = (
    "вибух",
    "вибухи",
    "вибух пролунав",
    "вибухи пролунали",
    "пролунав вибух",
    "пролунали вибухи",
    "лунали вибухи",
)


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


def write_parser_heartbeat():
    now = now_utc()

    try:
        tmp_file = f"{PARSER_HEARTBEAT_FILE}.tmp"

        with open(tmp_file, "w", encoding="utf-8") as f:
            f.write(now.isoformat())
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_file, PARSER_HEARTBEAT_FILE)

    except Exception as e:
        print(
            "Ошибка записи heartbeat-файла: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )

    with state_lock:
        state["parser_heartbeat"] = now


def get_parser_heartbeat_age():
    try:
        mtime = os.path.getmtime(PARSER_HEARTBEAT_FILE)

        return max(
            0,
            time.time() - mtime,
        )

    except (FileNotFoundError, OSError):
        return None


@app.route("/health")
def health():
    heartbeat_age = get_parser_heartbeat_age()

    if heartbeat_age is not None:
        if heartbeat_age <= PARSER_STALE_AFTER_SECONDS:
            return "OK", 200

        print(
            "HEALTH 503: parser heartbeat устарел "
            f"({int(heartbeat_age)} сек.)",
            flush=True,
        )

        return (
            "NOT OK: parser heartbeat устарел "
            f"({int(heartbeat_age)} сек.)",
            503,
        )

    with state_lock:
        started_at = state["started_at"]

    if started_at is None:
        return "OK", 200

    startup_age = (
        now_utc() - started_at
    ).total_seconds()

    if startup_age < STARTUP_GRACE_SECONDS:
        return "OK", 200

    print(
        "HEALTH 503: parser heartbeat отсутствует",
        flush=True,
    )

    return (
        "NOT OK: parser heartbeat отсутствует",
        503,
    )


# ============================================================
# СОСТОЯНИЕ
# ============================================================

state = {
    "parser_running": False,
    "telegram_api_ok": False,

    "pszsu_ok": False,
    "monitor_ok": False,

    "last_check": None,
    "last_alert": None,

    "parser_heartbeat": None,

    # Инциденты watchdog.
    "parser_failure_since": None,
    "parser_failure_notified": False,

    "pszsu_failure_since": None,
    "pszsu_failure_notified": False,

    "monitor_failure_since": None,
    "monitor_failure_notified": False,

    "telegram_failure_since": None,
    "telegram_failure_notified": False,

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


def format_duration(seconds):
    if seconds is None:
        return "—"

    seconds = max(0, int(seconds))

    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours} ч {minutes} мин {sec} сек"

    if minutes:
        return f"{minutes} мин {sec} сек"

    return f"{sec} сек"


# ============================================================
# TELEGRAM API
# ============================================================

def telegram_request(method, data=None, retries=3):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"

    if data is None:
        data = {}

    last_error = "неизвестная ошибка"

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
                last_error = (
                    f"Telegram Bot API вернул HTTP "
                    f"{response.status_code}"
                )

                if attempt < retries - 1:
                    time.sleep(2 + attempt)
                    continue

                break

            response.raise_for_status()

            result = response.json()

            if not result.get("ok"):
                last_error = "Telegram Bot API вернул ошибку"
                raise RuntimeError(last_error)

            with state_lock:
                state["telegram_api_ok"] = True

            return result

        except Exception as e:
            last_error = str(e)

            if attempt == retries - 1:
                with state_lock:
                    state["telegram_api_ok"] = False

                print(
                    f"Telegram API ошибка после {retries} попыток: "
                    f"{type(e).__name__}: {e}",
                    flush=True,
                )
                return None

            time.sleep(2 + attempt)

    with state_lock:
        state["telegram_api_ok"] = False

    print(
        f"Telegram API ошибка после {retries} попыток: {last_error}",
        flush=True,
    )

    return None


def send_telegram_message(
    text,
    parse_mode=None,
    disable_link_preview=False,
):
    data = {
        "chat_id": CHAT_ID,
        "text": text,
    }

    if parse_mode:
        data["parse_mode"] = parse_mode

    if disable_link_preview:
        data["link_preview_options"] = json.dumps({
            "is_disabled": True,
        })

    result = telegram_request(
        "sendMessage",
        data,
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
# WATCHDOG
# ============================================================

def set_failure_state(failure_type, reason):
    key_since = f"{failure_type}_failure_since"
    key_notified = f"{failure_type}_failure_notified"

    now = now_utc()

    with state_lock:
        if state[key_since] is None:
            state[key_since] = now
            state[key_notified] = False

            print(
                f"НАЧАЛО ИНЦИДЕНТА: {failure_type}; "
                f"причина: {reason}",
                flush=True,
            )

            return True

    return False


def clear_failure_state(failure_type):
    key_since = f"{failure_type}_failure_since"
    key_notified = f"{failure_type}_failure_notified"

    now = now_utc()

    with state_lock:
        since = state[key_since]
        notified = state[key_notified]

        state[key_since] = None
        state[key_notified] = False

    if since is None:
        return None

    return {
        "since": since,
        "notified": notified,
        "duration": (now - since).total_seconds(),
    }


def watchdog_send_failure(failure_type, reason):
    key_since = f"{failure_type}_failure_since"
    key_notified = f"{failure_type}_failure_notified"

    with state_lock:
        since = state[key_since]
        already_notified = state[key_notified]
        last_check = state["last_check"]

    if since is None or already_notified:
        return

    duration = (now_utc() - since).total_seconds()

    if duration < FAILURE_NOTIFICATION_AFTER_SECONDS:
        return

    titles = {
        "parser": "🔴 ПРОБЛЕМА СИСТЕМЫ",
        "pszsu": "🔴 ПРОБЛЕМА ИСТОЧНИКА PSZSU",
        "monitor": "🔴 ПРОБЛЕМА ИСТОЧНИКА MONITOR",
        "telegram": "🔴 ПРОБЛЕМА TELEGRAM API",
    }

    if failure_type == "parser":
        details = (
            "Парсер не выполняет проверки.\n"
            f"Последняя проверка: {format_time(last_check)}\n"
            "Причина: причина не определена."
        )

    elif failure_type == "pszsu":
        details = (
            "Источник PSZSU временно недоступен "
            "или произошла ошибка при его обработке.\n"
            f"Причина: {reason}"
        )

    elif failure_type == "monitor":
        details = (
            "Источник monitor временно недоступен "
            "или произошла ошибка при его обработке.\n"
            f"Причина: {reason}"
        )

    elif failure_type == "telegram":
        details = (
            "Бот не может нормально связаться "
            "с Telegram Bot API.\n"
            f"Причина: {reason}"
        )

    else:
        details = reason

    text = (
        f"{titles.get(failure_type, '🔴 ПРОБЛЕМА СИСТЕМЫ')}\n"
        "\n"
        "⚠️ ВНИМАНИЕ!\n"
        "\n"
        "БОТ НЕ РАБОТАЕТ.\n"
        "НА ЕГО УВЕДОМЛЕНИЯ НЕЛЬЗЯ РАССЧИТЫВАТЬ.\n"
        "\n"
        f"{details}\n"
        "\n"
        f"Проблема длится более "
        f"{FAILURE_NOTIFICATION_AFTER_SECONDS} секунд."
    )

    message_id = send_telegram_message(text)

    if message_id:
        with state_lock:
            state[key_notified] = True

        print(
            f"ОТПРАВЛЕНО УВЕДОМЛЕНИЕ О СБОЕ: {failure_type}",
            flush=True,
        )


def watchdog_check_recovery(failure_type, recovery_reason):
    key_since = f"{failure_type}_failure_since"
    key_notified = f"{failure_type}_failure_notified"

    with state_lock:
        since = state[key_since]
        notified = state[key_notified]

    if since is None or not notified:
        # Если инцидент был кратким и уведомление о сбое
        # не отправлялось, просто закрываем его.
        if since is not None:
            clear_failure_state(failure_type)
        return

    text = (
        "🟢 СИСТЕМА ВОССТАНОВЛЕНА\n"
        "\n"
        "✅ БОТ СНОВА АКТИВЕН.\n"
        "НА ЕГО УВЕДОМЛЕНИЯ СНОВА МОЖНО РАССЧИТЫВАТЬ.\n"
        "\n"
        f"{recovery_reason}\n"
        f"Длительность сбоя: {format_duration(info['duration'])}"
    )

    message_id = send_telegram_message(text)

    if message_id:
        # Закрываем инцидент только после успешной доставки
        # сообщения о восстановлении. Если Telegram временно
        # недоступен, watchdog попробует отправить recovery снова.
        clear_failure_state(failure_type)

        print(
            f"ОТПРАВЛЕНО УВЕДОМЛЕНИЕ О ВОССТАНОВЛЕНИИ: "
            f"{failure_type}",
            flush=True,
        )


def watchdog_loop():
    print(
        "Запущен контроль состояния системы",
        flush=True,
    )

    while True:
        try:
            now = now_utc()

            with state_lock:
                parser_running = state["parser_running"]
                telegram_api_ok = state["telegram_api_ok"]
                pszsu_ok = state["pszsu_ok"]
                monitor_ok = state["monitor_ok"]
                started_at = state["started_at"]

            if (
                started_at is None
                or (now - started_at).total_seconds()
                < STARTUP_GRACE_SECONDS
            ):
                time.sleep(WATCHDOG_INTERVAL_SECONDS)
                continue

            # ------------------------------------------------
            # ПАРСЕР
            # ------------------------------------------------

            heartbeat_age = get_parser_heartbeat_age()

            parser_alive = (
                parser_running
                and heartbeat_age is not None
                and heartbeat_age <= PARSER_STALE_AFTER_SECONDS
            )

            if parser_alive:
                watchdog_check_recovery(
                    "parser",
                    "Парсер снова выполняет проверки.",
                )
            else:
                set_failure_state(
                    "parser",
                    "Парсер не обновляет heartbeat.",
                )

                watchdog_send_failure(
                    "parser",
                    "Парсер не обновляет heartbeat.",
                )

            # ------------------------------------------------
            # PSZSU
            # ------------------------------------------------

            if pszsu_ok:
                watchdog_check_recovery(
                    "pszsu",
                    "Источник PSZSU снова доступен.",
                )
            else:
                set_failure_state(
                    "pszsu",
                    "Источник PSZSU не отвечает или произошла "
                    "ошибка при его обработке.",
                )

                watchdog_send_failure(
                    "pszsu",
                    "Источник PSZSU не отвечает или произошла "
                    "ошибка при его обработке.",
                )

            # ------------------------------------------------
            # MONITOR
            # ------------------------------------------------

            if monitor_ok:
                watchdog_check_recovery(
                    "monitor",
                    "Источник monitor снова доступен.",
                )
            else:
                set_failure_state(
                    "monitor",
                    "Источник monitor не отвечает или произошла "
                    "ошибка при его обработке.",
                )

                watchdog_send_failure(
                    "monitor",
                    "Источник monitor не отвечает или произошла "
                    "ошибка при его обработке.",
                )

            # ------------------------------------------------
            # TELEGRAM API
            # ------------------------------------------------

            if telegram_api_ok:
                watchdog_check_recovery(
                    "telegram",
                    "Telegram Bot API снова доступен.",
                )
            else:
                set_failure_state(
                    "telegram",
                    "Telegram Bot API не отвечает или "
                    "возвращает ошибку.",
                )

                watchdog_send_failure(
                    "telegram",
                    "Telegram Bot API не отвечает или "
                    "возвращает ошибку.",
                )

        except Exception as e:
            print(
                f"Ошибка контроля состояния: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

        time.sleep(WATCHDOG_INTERVAL_SECONDS)


# ============================================================
# PINNED STATUS
# ============================================================

STATUS_TITLE_MARKER = "🛠️ СОСТОЯНИЕ СИСТЕМЫ"


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

        if STATUS_TITLE_MARKER in text:
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
        pszsu_ok = state["pszsu_ok"]
        monitor_ok = state["monitor_ok"]

        last_check = state["last_check"]
        last_alert = state["last_alert"]

    heartbeat_age = get_parser_heartbeat_age()

    parser_alive = (
        parser_running
        and heartbeat_age is not None
        and heartbeat_age <= PARSER_STALE_AFTER_SECONDS
    )

    parser_icon = "🟢" if parser_alive else "🔴"
    parser_text = "РАБОТАЕТ" if parser_alive else "НЕТ ПРОВЕРКИ"

    telegram_icon = "🟢" if telegram_api_ok else "🔴"
    telegram_text = "OK" if telegram_api_ok else "ОШИБКА"

    pszsu_icon = "🟢" if pszsu_ok else "🔴"
    pszsu_text = "OK" if pszsu_ok else "ОШИБКА"

    monitor_icon = "🟢" if monitor_ok else "🔴"
    monitor_text = "OK" if monitor_ok else "ОШИБКА"

    title = (
        f"{parser_icon}{telegram_icon}{pszsu_icon}{monitor_icon} "
        f"🛠️ СОСТОЯНИЕ СИСТЕМЫ"
    )

    return (
        f"{title}\n"
        "\n"
        f"{parser_icon} Парсер: {parser_text}\n"
        f"{telegram_icon} Telegram API: {telegram_text}\n"
        f"{pszsu_icon} Источник PSZSU: {pszsu_text}\n"
        f"{monitor_icon} Источник monitor: {monitor_text}\n"
        "\n"
        f"🔎 Ключевое слово: {KEYWORD}\n"
        f"⏱ Проверка: каждые {CHECK_INTERVAL_SECONDS} сек.\n"
        "\n"
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
            f"Команда /test отклонена: "
            f"пользователь {user_id} не администратор.",
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

    telegram_request(
        "deleteWebhook",
        {
            "drop_pending_updates": False,
        },
    )

    offset = None

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

                command = text.split()[0].lower()

                if (
                    command == "/test"
                    or command.startswith("/test@")
                ):
                    handle_test_command(message)

        except Exception as e:
            print(
                f"Ошибка обработчика Telegram-команд: "
                f"{type(e).__name__}: {e}",
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
                f"Ошибка обновления статуса: "
                f"{type(e).__name__}: {e}",
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
# ФИЛЬТРЫ
# ============================================================

def normalize_text(text):
    return " ".join(
        text.lower().replace("ё", "е").split()
    )


def has_kremenchuk(text):
    return KEYWORD in normalize_text(text)


def has_impact(text):
    normalized = normalize_text(text)

    return any(
        pattern in normalized
        for pattern in IMPACT_PATTERNS
    )


def has_high_speed_threat(text):
    normalized = normalize_text(text)

    if any(
        pattern in normalized
        for pattern in HIGH_SPEED_PATTERNS
    ):
        return True

    # Отдельная проверка БР как отдельного слова.
    return re.search(r"\b" + re.escape(BR_PATTERN) + r"\b", normalized) is not None


def is_continuing_threat(text):
    normalized = normalize_text(text)

    return any(
        pattern in normalized
        for pattern in CONTINUING_PATTERNS
    )


def is_post_event_report(text):
    normalized = normalize_text(text)

    return any(
        pattern in normalized
        for pattern in POST_EVENT_PATTERNS
    )


def classify_monitor_message(text):
    """
    Возвращает:
      IMPACT_CONFIRMED
      HIGH_SPEED_THREAT
      IGNORE

    Приоритет:
      1. Взрыв + Кременчугская география.
      2. Высокоскоростная угроза + география.
      3. Всё остальное игнорируем.

    Continuing threat и post-event сообщения не создают тревогу.
    """

    if not has_kremenchuk(text):
        return "IGNORE"

    # Подтверждение события имеет приоритет над остальными
    # признаками. Поэтому оперативный пост:
    # "Вибух ... Було застосовано ..."
    # не потеряется из-за слова "було застосовано".
    if has_impact(text):
        return "IMPACT_CONFIRMED"

    if is_continuing_threat(text):
        return "IGNORE"

    if has_high_speed_threat(text):
        if is_post_event_report(text):
            return "IGNORE"

        return "HIGH_SPEED_THREAT"

    return "IGNORE"


# ============================================================
# ОТПРАВКА ОПЕРАТИВНОГО СООБЩЕНИЯ
# ============================================================

def build_alert_text(
    source_name,
    source_link,
    original_text,
    classification,
):
    safe_text = original_text[:3500]

    escaped_text = html.escape(
        safe_text,
        quote=False,
    )

    if classification == "IMPACT_CONFIRMED":
        title = (
            "🟡 внимание 🟡\n"
            "\n"
            "<b>ПОДТВЕРЖДЕНИЕ АТАКИ "
            "НА КРЕМЕНЧУГ / РАЙОН</b>"
        )
    else:
        title = (
            "🚨 внимание 🚨\n"
            "\n"
            "<b>УГРОЗА ДЛЯ КРЕМЕНЧУГА</b>"
        )

    return (
        f"{title}\n"
        "\n"
        f'<a href="{source_link}">📡 {source_name}</a>\n'
        "\n"
        f"<blockquote><b>{escaped_text}</b></blockquote>"
    )


def send_alert(
    source_name,
    source_link,
    post_id,
    original_text,
    classification,
):
    dedup_key = f"{source_name}:{post_id}"

    with sent_messages_lock:
        if dedup_key in sent_messages:
            return False

    alert_text = build_alert_text(
        source_name=source_name,
        source_link=source_link,
        original_text=original_text,
        classification=classification,
    )

    message_id = send_telegram_message(
        alert_text,
        parse_mode="HTML",
        disable_link_preview=True,
    )

    # Только после успешной отправки считаем сообщение доставленным.
    if not message_id:
        return False

    with sent_messages_lock:
        sent_messages.add(dedup_key)

        if len(sent_messages) > MAX_SENT_MESSAGES:
            sent_messages.pop()

    with state_lock:
        state["last_alert"] = now_utc()

    print(
        f"ОТПРАВЛЕНО: {source_name}; "
        f"{classification}; {post_id}",
        flush=True,
    )

    return True


# ============================================================
# ПРОВЕРКА ОДНОГО ИСТОЧНИКА
# ============================================================

def check_source(
    source_url,
    source_name,
    source_link,
    is_monitor=False,
):
    try:
        response = session.get(
            source_url,
            timeout=SOURCE_REQUEST_TIMEOUT,
        )

        if response.status_code != 200:
            print(
                f"{source_name}: HTTP {response.status_code}",
                flush=True,
            )

            return False

        soup = BeautifulSoup(
            response.text,
            "html.parser",
        )

        posts = soup.select(
            ".tgme_widget_message"
        )

        if not posts:
            # Сам источник ответил, страница разобрана.
            return True

        current_time = now_utc()

        # Сначала самые свежие.
        posts = list(reversed(posts))

        for post in posts:
            post_datetime = get_post_datetime(post)

            if not post_datetime:
                continue

            age_seconds = (
                current_time - post_datetime
            ).total_seconds()

            if age_seconds < 0:
                age_seconds = 0

            if age_seconds > MAX_MESSAGE_AGE_MINUTES * 60:
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

            # ------------------------------------------------
            # PSZSU
            # ------------------------------------------------

            if not is_monitor:
                if not has_kremenchuk(text):
                    continue

                # PSZSU: рабочую логику Кременчуга сохраняем,
                # но постфактумные сводки/итоги не отправляем.
                if is_post_event_report(text):
                    continue

                post_id = post.get("data-post")

                if not post_id:
                    continue

                classification = "HIGH_SPEED_THREAT"

                send_alert(
                    source_name=source_name,
                    source_link=source_link,
                    post_id=post_id,
                    original_text=text,
                    classification=classification,
                )

                continue

            # ------------------------------------------------
            # MONITOR
            # ------------------------------------------------

            classification = classify_monitor_message(text)

            if classification == "IGNORE":
                continue

            post_id = post.get("data-post")

            if not post_id:
                continue

            send_alert(
                source_name=source_name,
                source_link=source_link,
                post_id=post_id,
                original_text=text,
                classification=classification,
            )

        return True

    except Exception as e:
        print(
            f"Ошибка проверки {source_name}: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )

        return False


# ============================================================
# ОСНОВНАЯ ПРОВЕРКА
# ============================================================

def check_updates():
    check_time = now_utc()

    # Heartbeat ДО сетевых запросов.
    write_parser_heartbeat()

    with state_lock:
        state["last_check"] = check_time

    # Каждый источник имеет собственный статус.
    pszsu_ok = check_source(
        source_url=PSZSU_URL,
        source_name=PSZSU_NAME,
        source_link=PSZSU_LINK,
        is_monitor=False,
    )

    with state_lock:
        state["pszsu_ok"] = pszsu_ok

    monitor_ok = check_source(
        source_url=MONITOR_URL,
        source_name=MONITOR_NAME,
        source_link=MONITOR_LINK,
        is_monitor=True,
    )

    with state_lock:
        state["monitor_ok"] = monitor_ok


# ============================================================
# ОСНОВНОЙ ПАРСЕР
# ============================================================

def run_bot():
    print(
        "==============================",
        flush=True,
    )

    print(
        "ФОНОВЫЙ ПАРСЕР ЗАПУЩЕН",
        flush=True,
    )

    print(
        f"PSZSU: {PSZSU_URL}",
        flush=True,
    )

    print(
        f"monitor: {MONITOR_URL}",
        flush=True,
    )

    print(
        f"Ключевое слово: {KEYWORD}",
        flush=True,
    )

    print(
        f"Проверка каждые {CHECK_INTERVAL_SECONDS} секунд",
        flush=True,
    )

    print(
        f"Максимальный возраст сообщения: "
        f"{MAX_MESSAGE_AGE_MINUTES} минут",
        flush=True,
    )

    print(
        "==============================",
        flush=True,
    )

    startup_time = now_utc()

    with state_lock:
        state["parser_running"] = True
        state["started_at"] = startup_time
        state["last_check"] = startup_time

    write_parser_heartbeat()

    print(
        "Heartbeat парсера зафиксирован.",
        flush=True,
    )

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
            # Ошибка одного цикла не должна остановить parser.
            with state_lock:
                state["pszsu_ok"] = False
                state["monitor_ok"] = False

            print(
                f"Ошибка цикла парсера: "
                f"{type(e).__name__}: {e}",
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


watchdog_thread = threading.Thread(
    target=watchdog_loop,
    name="system-watchdog",
    daemon=True,
)

watchdog_thread.start()


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
