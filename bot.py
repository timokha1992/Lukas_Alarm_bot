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

# Ищем корень "кременч" без учёта регистра.
# Это покрывает Кременчук, Кременчуга, Кременчуку, Кременчуці и т.д.
KEYWORD = "кременч"

CHECK_INTERVAL_SECONDS = 15
STATUS_UPDATE_INTERVAL_SECONDS = 60
WATCHDOG_INTERVAL_SECONDS = 10

# Если основной парсер не обновлял heartbeat дольше этого времени,
# считаем его фактически остановившимся.
PARSER_STALE_AFTER_SECONDS = 60

# Кратковременные сбои не должны сразу засорять рабочую группу.
FAILURE_NOTIFICATION_AFTER_SECONDS = 60

# После запуска даём потокам время спокойно инициализироваться.
STARTUP_GRACE_SECONDS = 90

MAX_MESSAGE_AGE_MINUTES = 5

# Дедупликация только в рамках текущего запуска.
MAX_SENT_MESSAGES = 1000

# Telegram /test использует long polling до 20 секунд.
# HTTP timeout должен быть больше этого значения.
REQUEST_TIMEOUT = (5, 35)

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
    """
    Health для UptimeRobot / внешнего контроля.

    Проверяем три независимых компонента:

    1. основной parser действительно выполняет проверки;
    2. официальный источник доступен;
    3. Telegram Bot API действительно доступен.

    Важно:
    состояние Telegram API не хранится одной простой переменной,
    которая может конфликтовать между несколькими потоками.
    Вместо этого используется время последнего успешного запроса
    и начало непрерывного сбоя.
    """

    now = now_utc()

    with state_lock:
        parser_running = state["parser_running"]
        parser_heartbeat = state["parser_heartbeat"]

        source_ok = state["source_ok"]
        source_failure_since = state["source_failure_since"]

        telegram_last_success = state["telegram_last_success"]
        telegram_failure_since = state["telegram_failure_since"]

    # --------------------------------------------------------
    # ПАРСЕР
    # --------------------------------------------------------

    parser_alive = (
        parser_running
        and parser_heartbeat is not None
        and (now - parser_heartbeat).total_seconds()
        <= PARSER_STALE_AFTER_SECONDS
    )

    parser_stale_seconds = None

    if parser_heartbeat is not None:
        parser_stale_seconds = (
            now - parser_heartbeat
        ).total_seconds()

    # --------------------------------------------------------
    # TELEGRAM API
    # --------------------------------------------------------

    telegram_failure_long = (
        telegram_failure_since is not None
        and (
            now - telegram_failure_since
        ).total_seconds()
        >= FAILURE_NOTIFICATION_AFTER_SECONDS
    )

    # --------------------------------------------------------
    # ИСТОЧНИК
    # --------------------------------------------------------

    source_failure_long = (
        not source_ok
        and source_failure_since is not None
        and (
            now - source_failure_since
        ).total_seconds()
        >= FAILURE_NOTIFICATION_AFTER_SECONDS
    )

    # --------------------------------------------------------
    # HEALTH RESULT
    # --------------------------------------------------------

    problems = []

    if not parser_alive:
        if parser_heartbeat is None:
            problems.append("parser heartbeat отсутствует")
        elif parser_stale_seconds is not None:
            problems.append(
                f"parser heartbeat устарел "
                f"({int(parser_stale_seconds)} сек.)"
            )
        else:
            problems.append("parser не работает")

    if source_failure_long:
        problems.append("источник недоступен более 60 сек.")

    if telegram_failure_long:
        if telegram_last_success is None:
            problems.append(
                "Telegram Bot API недоступен более 60 сек."
            )
        else:
            telegram_age = (
                now - telegram_last_success
            ).total_seconds()

            problems.append(
                "Telegram Bot API: "
                f"нет успешного запроса {int(telegram_age)} сек."
            )

    if not problems:
        return "OK", 200

    reason = "; ".join(problems)

    print(
        f"HEALTH 503: {reason}",
        flush=True,
    )

    return f"NOT OK: {reason}", 503


# ============================================================
# СОСТОЯНИЕ
# ============================================================

state = {
    "parser_running": False,

    "source_ok": False,

    "last_check": None,
    "last_alert": None,

    # Время последнего живого шага основного парсера.
    "parser_heartbeat": None,

    # Telegram API:
    # вместо одного telegram_api_ok используем:
    # - время последнего успешного запроса;
    # - начало непрерывного сбоя.
    "telegram_last_success": None,
    "telegram_failure_since": None,

    # Состояние источника.
    "source_failure_since": None,

    # Состояния инцидентов watchdog.
    "parser_failure_since": None,

    "parser_failure_notified": False,
    "source_failure_notified": False,
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

def mark_telegram_success():
    """
    Фиксируем успешный контакт с Telegram Bot API.
    """

    now = now_utc()

    with state_lock:
        state["telegram_last_success"] = now
        state["telegram_failure_since"] = None


def mark_telegram_failure():
    """
    Фиксируем начало непрерывной проблемы Telegram API.

    Повторные ошибки не меняют время начала инцидента.
    """

    now = now_utc()

    with state_lock:
        if state["telegram_failure_since"] is None:
            state["telegram_failure_since"] = now

            print(
                "НАЧАЛО ПРОБЛЕМЫ TELEGRAM API",
                flush=True,
            )


def telegram_request(method, data=None, retries=3):
    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/{method}"
    )

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

            # Telegram rate limit.
            if response.status_code == 429:
                try:
                    retry_after = response.json().get(
                        "parameters", {}
                    ).get("retry_after", 5)
                except Exception:
                    retry_after = 5

                last_error = (
                    f"Telegram rate limit: "
                    f"retry_after={retry_after}"
                )

                if attempt < retries - 1:
                    time.sleep(
                        min(int(retry_after) + 1, 30)
                    )
                    continue

                mark_telegram_failure()
                break

            # Временные ошибки сервера Telegram.
            if response.status_code >= 500:
                last_error = (
                    f"Telegram Bot API вернул HTTP "
                    f"{response.status_code}"
                )

                if attempt < retries - 1:
                    time.sleep(2 + attempt)
                    continue

                mark_telegram_failure()
                break

            response.raise_for_status()

            result = response.json()

            if not result.get("ok"):
                last_error = (
                    "Telegram Bot API вернул ошибку"
                )

                if attempt < retries - 1:
                    time.sleep(2 + attempt)
                    continue

                mark_telegram_failure()
                break

            # Любой успешный Telegram API запрос означает,
            # что сам API доступен.
            mark_telegram_success()

            return result

        except Exception as e:
            last_error = str(e)

            if attempt < retries - 1:
                time.sleep(2 + attempt)
                continue

            mark_telegram_failure()

            print(
                "Telegram API ошибка после "
                f"{retries} попыток: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

            return None

    mark_telegram_failure()

    print(
        "Telegram API ошибка после "
        f"{retries} попыток: {last_error}",
        flush=True,
    )

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
# КОНТРОЛЬ СОСТОЯНИЯ / WATCHDOG
# ============================================================

def set_failure_state(failure_type, reason):
    """
    Запоминает начало проблемы.

    Повторные проверки того же инцидента
    состояние не сбрасывают.
    """

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
    """
    Завершает состояние проблемы.
    Возвращает информацию об инциденте.
    """

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
        "duration": (
            now - since
        ).total_seconds(),
    }


def watchdog_send_failure(failure_type, reason):
    """
    Отправляет одно подробное служебное сообщение
    после 60 секунд непрерывной проблемы.

    В beta/alpha стадии сохраняем техническую
    информацию для диагностики.
    """

    key_since = f"{failure_type}_failure_since"
    key_notified = f"{failure_type}_failure_notified"

    with state_lock:
        since = state[key_since]
        already_notified = state[key_notified]
        last_check = state["last_check"]

    if since is None or already_notified:
        return

    duration = (
        now_utc() - since
    ).total_seconds()

    if duration < FAILURE_NOTIFICATION_AFTER_SECONDS:
        return

    titles = {
        "parser": "🔴 ПРОБЛЕМА СИСТЕМЫ",
        "source": "🔴 ПРОБЛЕМА ИСТОЧНИКА",
        "telegram": "🔴 ПРОБЛЕМА TELEGRAM API",
    }

    if failure_type == "parser":
        message = (
            "⚠️ ВНИМАНИЕ!\n"
            "\n"
            "БОТ НЕ РАБОТАЕТ.\n"
            "НА ЕГО УВЕДОМЛЕНИЯ НЕЛЬЗЯ РАССЧИТЫВАТЬ.\n"
            "\n"
            "Парсер не выполняет проверки.\n"
            f"Последняя проверка: "
            f"{format_time(last_check)}\n"
            "Причина: причина не определена."
        )

    elif failure_type == "source":
        message = (
            "⚠️ ВНИМАНИЕ!\n"
            "\n"
            "БОТ НЕ РАБОТАЕТ.\n"
            "НА ЕГО УВЕДОМЛЕНИЯ НЕЛЬЗЯ РАССЧИТЫВАТЬ.\n"
            "\n"
            "Официальный источник временно "
            "недоступен.\n"
            f"Причина: {reason}"
        )

    elif failure_type == "telegram":
        message = (
            "⚠️ ВНИМАНИЕ!\n"
            "\n"
            "БОТ НЕ РАБОТАЕТ.\n"
            "НА ЕГО УВЕДОМЛЕНИЯ НЕЛЬЗЯ РАССЧИТЫВАТЬ.\n"
            "\n"
            "Бот не может нормально связаться "
            "с Telegram Bot API.\n"
            f"Причина: {reason}"
        )

    else:
        message = (
            "⚠️ ВНИМАНИЕ!\n"
            "\n"
            "БОТ НЕ РАБОТАЕТ.\n"
            "НА ЕГО УВЕДОМЛЕНИЯ НЕЛЬЗЯ РАССЧИТЫВАТЬ.\n"
            "\n"
            f"{reason}"
        )

    text = (
        f"{titles.get(failure_type, '🔴 ПРОБЛЕМА СИСТЕМЫ')}\n"
        "\n"
        f"{message}\n"
        "\n"
        f"Проблема длится более "
        f"{FAILURE_NOTIFICATION_AFTER_SECONDS} секунд."
    )

    message_id = send_telegram_message(text)

    if message_id:
        with state_lock:
            state[key_notified] = True

        print(
            f"ОТПРАВЛЕНО УВЕДОМЛЕНИЕ О СБОЕ: "
            f"{failure_type}",
            flush=True,
        )


def watchdog_check_recovery(
    failure_type,
    recovery_reason,
):
    """
    После восстановления отправляет сообщение
    только если ранее по этому инциденту уже было
    отправлено сообщение о сбое.
    """

    info = clear_failure_state(failure_type)

    if not info or not info["notified"]:
        return

    text = (
        "🟢 СИСТЕМА ВОССТАНОВЛЕНА\n"
        "\n"
        "✅ БОТ СНОВА АКТИВЕН.\n"
        "НА ЕГО УВЕДОМЛЕНИЯ СНОВА МОЖНО РАССЧИТЫВАТЬ.\n"
        "\n"
        f"{recovery_reason}\n"
        f"Длительность сбоя: "
        f"{format_duration(info['duration'])}"
    )

    message_id = send_telegram_message(text)

    if message_id:
        print(
            "ОТПРАВЛЕНО УВЕДОМЛЕНИЕ "
            "О ВОССТАНОВЛЕНИИ: "
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
                parser_heartbeat = state["parser_heartbeat"]

                source_ok = state["source_ok"]

                telegram_last_success = (
                    state["telegram_last_success"]
                )

                started_at = state["started_at"]

            # После запуска даём системе спокойно
            # инициализироваться.
            if (
                started_at is None
                or (
                    now - started_at
                ).total_seconds()
                < STARTUP_GRACE_SECONDS
            ):
                time.sleep(
                    WATCHDOG_INTERVAL_SECONDS
                )
                continue

            # ------------------------------------------------
            # ПАРСЕР
            # ------------------------------------------------

            parser_alive = (
                parser_running
                and parser_heartbeat is not None
                and (
                    now - parser_heartbeat
                ).total_seconds()
                <= PARSER_STALE_AFTER_SECONDS
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
            # ИСТОЧНИК
            # ------------------------------------------------

            if source_ok:
                watchdog_check_recovery(
                    "source",
                    "Официальный источник снова доступен.",
                )
            else:
                set_failure_state(
                    "source",
                    "Источник не отвечает или произошла "
                    "ошибка при его обработке.",
                )

                watchdog_send_failure(
                    "source",
                    "Источник не отвечает или произошла "
                    "ошибка при его обработке.",
                )

            # ------------------------------------------------
            # TELEGRAM API
            # ------------------------------------------------

            telegram_problem = False

            if telegram_last_success is None:
                telegram_problem = True
                telegram_reason = (
                    "Telegram Bot API ещё не подтвердил "
                    "успешный запрос."
                )
            else:
                telegram_age = (
                    now - telegram_last_success
                ).total_seconds()

                if telegram_age >= FAILURE_NOTIFICATION_AFTER_SECONDS:
                    telegram_problem = True
                    telegram_reason = (
                        "Telegram Bot API не подтверждал "
                        f"успешный запрос {int(telegram_age)} сек."
                    )
                else:
                    telegram_reason = (
                        "Telegram Bot API снова доступен."
                    )

            if not telegram_problem:
                watchdog_check_recovery(
                    "telegram",
                    telegram_reason,
                )
            else:
                set_failure_state(
                    "telegram",
                    telegram_reason,
                )

                watchdog_send_failure(
                    "telegram",
                    telegram_reason,
                )

        except Exception as e:
            # Watchdog сам не должен умирать
            # из-за своей ошибки.
            print(
                "Ошибка контроля состояния: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

        time.sleep(WATCHDOG_INTERVAL_SECONDS)


# ============================================================
# PINNED STATUS
# ============================================================

STATUS_TITLE = "🟢🟢🟢 🛠️ СОСТОЯНИЕ СИСТЕМЫ"


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
        pinned = result["result"].get(
            "pinned_message"
        )

        if not pinned:
            return None

        message_id = pinned.get("message_id")
        text = pinned.get("text", "")

        # Ищем именно наше сообщение состояния.
        if "🛠️ СОСТОЯНИЕ СИСТЕМЫ" in text:
            return message_id

    except Exception as e:
        print(
            "Ошибка определения закреплённого "
            f"сообщения: {e}",
            flush=True,
        )

    return None


def build_status_text():
    now = now_utc()

    with state_lock:
        parser_running = state["parser_running"]
        parser_heartbeat = state["parser_heartbeat"]

        telegram_last_success = (
            state["telegram_last_success"]
        )

        source_ok = state["source_ok"]

        last_check = state["last_check"]
        last_alert = state["last_alert"]

    # --------------------------------------------------------
    # ПАРСЕР
    # --------------------------------------------------------

    parser_alive = (
        parser_running
        and parser_heartbeat is not None
        and (
            now - parser_heartbeat
        ).total_seconds()
        <= PARSER_STALE_AFTER_SECONDS
    )

    if parser_alive:
        parser_icon = "🟢"
        parser_text = "РАБОТАЕТ"
    else:
        parser_icon = "🔴"
        parser_text = "НЕТ ПРОВЕРКИ"

    # --------------------------------------------------------
    # TELEGRAM API
    # --------------------------------------------------------

    telegram_alive = False

    if telegram_last_success is not None:
        telegram_age = (
            now - telegram_last_success
        ).total_seconds()

        telegram_alive = (
            telegram_age
            < FAILURE_NOTIFICATION_AFTER_SECONDS
        )

    telegram_icon = (
        "🟢"
        if telegram_alive
        else "🔴"
    )

    telegram_text = (
        "OK"
        if telegram_alive
        else "ОШИБКА"
    )

    # --------------------------------------------------------
    # SOURCE
    # --------------------------------------------------------

    source_icon = (
        "🟢"
        if source_ok
        else "🔴"
    )

    source_text = (
        "OK"
        if source_ok
        else "ОШИБКА"
    )

    # --------------------------------------------------------
    # TITLE
    # --------------------------------------------------------

    title = (
        f"{parser_icon}"
        f"{telegram_icon}"
        f"{source_icon} "
        "🛠️ СОСТОЯНИЕ СИСТЕМЫ"
    )

    return (
        f"{title}\n"
        "\n"
        f"{parser_icon} Парсер: {parser_text}\n"
        f"{telegram_icon} Telegram API: "
        f"{telegram_text}\n"
        f"{source_icon} Источник: {source_text}\n"
        "\n"
        f"🔎 Ключевое слово: {KEYWORD}\n"
        f"⏱ Проверка: каждые "
        f"{CHECK_INTERVAL_SECONDS} сек.\n"
        "\n"
        f"🕐 Последняя проверка: "
        f"{format_time(last_check)}\n"
        f"🚨 Последняя тревога: "
        f"{format_time(last_alert)}"
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
            "Сообщение состояния создано, "
            "но закрепить его не удалось.",
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

    # Команда принимается только из нашей рабочей группы.
    if chat_id != CHAT_ID:
        return

    if not user_id:
        return

    # Только администратор может запускать /test.
    if not is_group_admin(user_id):
        print(
            "Команда /test отклонена: "
            f"пользователь {user_id} не администратор.",
            flush=True,
        )
        return

    print(
        "Получена команда /test от "
        f"администратора {user_id}.",
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

    # Переключаем Telegram на long polling.
    telegram_request(
        "deleteWebhook",
        {
            "drop_pending_updates": False,
        },
    )

    offset = None

    # Не даём старым командам после перезапуска
    # внезапно выполнить /test.
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
            "Ошибка очистки старых "
            f"Telegram-команд: {e}",
            flush=True,
        )

    while True:
        try:
            data = {
                # Long polling Telegram.
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
                offset = (
                    update["update_id"] + 1
                )

                message = update.get("message")

                if not message:
                    continue

                text = message.get(
                    "text",
                    "",
                ).strip()

                if not text:
                    continue

                command = (
                    text.split()[0].lower()
                )

                if (
                    command == "/test"
                    or command.startswith("/test@")
                ):
                    handle_test_command(message)

        except Exception as e:
            print(
                "Ошибка обработчика "
                "Telegram-команд: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

            time.sleep(5)


# ============================================================
# STATUS LOOP
# ============================================================

def status_loop():
    print(
        "Запущено обновление статуса "
        "каждые 60 секунд",
        flush=True,
    )

    while True:
        try:
            update_status_message()

        except Exception as e:
            print(
                "Ошибка обновления статуса: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

        time.sleep(
            STATUS_UPDATE_INTERVAL_SECONDS
        )


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
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt.astimezone(timezone.utc)

    except Exception:
        return None


# ============================================================
# ПРОВЕРКА ИСТОЧНИКА
# ============================================================

def mark_source_success():
    with state_lock:
        state["source_ok"] = True
        state["source_failure_since"] = None


def mark_source_failure(reason):
    now = now_utc()

    with state_lock:
        state["source_ok"] = False

        if state["source_failure_since"] is None:
            state["source_failure_since"] = now

    print(
        f"Источник: проблема — {reason}",
        flush=True,
    )


def check_updates():
    check_time = now_utc()

    # Heartbeat обновляется в начале каждой проверки.
    # Watchdog видит, что основной parser жив.
    with state_lock:
        state["last_check"] = check_time
        state["parser_heartbeat"] = check_time

    try:
        response = session.get(
            SOURCE_URL,
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code != 200:
            mark_source_failure(
                f"HTTP {response.status_code}"
            )
            return

        mark_source_success()

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

        # Сначала самые свежие сообщения.
        posts = list(reversed(posts))

        for post in posts:
            post_datetime = get_post_datetime(post)

            if not post_datetime:
                continue

            age_seconds = (
                current_time - post_datetime
            ).total_seconds()

            # Если дата сообщения немного опережает
            # наше время, не считаем его старым.
            if age_seconds < 0:
                age_seconds = 0

            # Не рассматриваем сообщения старше 5 минут.
            if (
                age_seconds
                > MAX_MESSAGE_AGE_MINUTES * 60
            ):
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

            # Ищем корень "кременч"
            # без учёта регистра.
            if KEYWORD not in text.lower():
                continue

            # Получаем ID конкретного сообщения канала.
            post_id = None

            try:
                post_id = post.get(
                    "data-post"
                )
            except Exception:
                pass

            if not post_id:
                continue

            # Не отправляем один и тот же пост
            # повторно в рамках текущего запуска.
            with sent_messages_lock:
                if post_id in sent_messages:
                    continue

            # Ограничение размера сообщения.
            safe_text = text[:3500]

            alert_text = (
                "🔴🚨 УГРОЗА ДЛЯ КРЕМЕНЧУГА\n"
                "\n"
                f"{safe_text}"
            )

            # ВАЖНО:
            # post_id добавляем в dedup ТОЛЬКО
            # после успешной отправки.
            message_id = send_telegram_message(
                alert_text
            )

            if message_id:
                with sent_messages_lock:
                    sent_messages.add(post_id)

                    if (
                        len(sent_messages)
                        > MAX_SENT_MESSAGES
                    ):
                        sent_messages.pop()

                with state_lock:
                    state["last_alert"] = now_utc()

                print(
                    f"ОТПРАВЛЕНА ТРЕВОГА: "
                    f"{post_id}",
                    flush=True,
                )

    except Exception as e:
        mark_source_failure(
            f"{type(e).__name__}: {e}"
        )

        print(
            "Ошибка проверки источника: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )


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
        f"Ключевое слово: {KEYWORD}",
        flush=True,
    )

    print(
        f"Проверка каждые "
        f"{CHECK_INTERVAL_SECONDS} секунд",
        flush=True,
    )

    print(
        "Максимальный возраст сообщения: "
        f"{MAX_MESSAGE_AGE_MINUTES} минут",
        flush=True,
    )

    print(
        "==============================",
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
            # Ошибка одной проверки не должна
            # остановить весь фоновый parser.
            mark_source_failure(
                f"{type(e).__name__}: {e}"
            )

            print(
                "Ошибка цикла парсера: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

        time.sleep(
            CHECK_INTERVAL_SECONDS
        )


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
        port=int(
            os.getenv("PORT", "10000")
        ),
    )
