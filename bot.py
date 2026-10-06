import os
import time
import html
import json
import queue
import re
import secrets
import sys
import threading
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request

# Конфигурация фильтров вынесена в пакет filters/ (данные, без логики):
#   filters/common.py  - только то, что реально общее для обоих источников;
#   filters/pszsu.py   - всё, что относится к PSZSU;
#   filters/monitor.py - всё, что относится к MONITOR.
# Логика классификации остаётся здесь, в bot.py.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from filters.common import (
    KEYWORD,
    KREMENCHUK_VARIANTS,
    POST_EVENT_PATTERNS,
    NORMALIZE_MP_REGEX,
    RU_ADJECTIVE_REGEX,
    RU_ADJECTIVE_REPLACEMENT,
)

from filters.pszsu import (
    CONTINUING_PATTERNS,
    IMPACT_PATTERNS,
    KREMENCHUK_PSZSU_COURSE_TARGET_REGEX,
    KREMENCHUK_PSZSU_CITY_FIRST_ALERT_REGEX,
    KREMENCHUK_RESERVOIR_PATTERNS,
    PSZSU_KREMENCHUK_MINUS_REGEX,
    PSZSU_ARCHIVE_HIGH_REGEX,
    PSZSU_ARCHIVE_HIGH_STRIP_REGEX,
    PSZSU_ARCHIVE_DIRECTION_REGEX,
    PSZSU_CITY_WITH_OTHER_PLACE_REGEX,
    PSZSU_DIRECT_ALARM_REGEX,
    PSZSU_NORMALIZE_MP_REGEX,
    PSZSU_STANDALONE_CITY_REGEX,
)

from filters.monitor import (
    KREMENCHUK_CITY_PATTERNS,
    BANDEROL_PATTERNS,
    MONITOR_UAV_ARCHIVE_PATTERNS,
    HIGH_SPEED_PATTERNS,
    CRUISE_MISSILE_PATTERNS,
    KREMENCHUK_DIRECT_ALERT_PATTERNS,
    KREMENCHUK_DIRECT_ALERT_REGEX,
    KREMENCHUK_CITY_FIRST_TARGET_REGEX,
    MONITOR_OPERATIONAL_MARKER_REGEX,
    MONITOR_CITY_WITH_OTHER_PLACE_REGEX,
    MONITOR_PASS_BY_REGEX,
    MONITOR_RECONNAISSANCE_MARKER,
    MONITOR_CRUISE_ABBREVIATION_REGEX,
    MONITOR_BR_REGEX,
    MONITOR_LOCAL_TARGET_REGEX,
    MONITOR_SEGMENT_SEPARATOR_REGEX,
    MONITOR_SENTENCE_SPLIT_REGEX,
    MONITOR_ARCHIVE_DIRECT_RELATION_REGEX,
    MONITOR_LEGACY_PASS_BY_REGEX,
)


# ============================================================
# НАСТРОЙКИ
# ============================================================

PSZSU_URL = "https://t.me/s/kpszsu"
MONITOR_URL = "https://t.me/s/war_monitor"

PSZSU_NAME = "Повітряні Сили ЗС України"
PSZSU_LINK = "https://t.me/kpszsu"

MONITOR_NAME = "monitor"
MONITOR_LINK = "https://t.me/war_monitor"


CHECK_INTERVAL_SECONDS = 15
STATUS_UPDATE_INTERVAL_SECONDS = 60
WATCHDOG_INTERVAL_SECONDS = 10

PARSER_STALE_AFTER_SECONDS = 60
FAILURE_NOTIFICATION_AFTER_SECONDS = 60
STARTUP_GRACE_SECONDS = 90

# Внутренний процесс-уровневый watchdog.
# Не связан с Cloudflare Watchdog и ничего не пишет в Telegram.
# Его единственная задача — обнаружить РЕАЛЬНУЮ гибель/зависание
# фонового парсера и принудительно завершить процесс, чтобы
# gunicorn поднял новый рабочий процесс автоматически.
INTERNAL_WATCHDOG_INTERVAL_SECONDS = 20
PROCESS_HANG_THRESHOLD_SECONDS = 120

MAX_MESSAGE_AGE_MINUTES = 5

# Предел длины одного сообщения Telegram (символы).
TELEGRAM_MAX_MESSAGE_LENGTH = 4096

# Единый каталог персистентного состояния (ENV STATE_DIR).
# На Render его нужно указать на persistent disk, иначе состояние
# пропадёт при пересоздании контейнера. Внутри — один JSON-файл
# (реестр отправленных сообщений + ID статус-сообщения).
STATE_DIR = os.getenv("STATE_DIR", "/tmp")
STATE_FILE = os.path.join(STATE_DIR, "lukas_alarm_state.json")

# Записи реестра старше этого срока бесполезны (посты старше
# MAX_MESSAGE_AGE_MINUTES никогда не обрабатываются) и удаляются.
SENT_MESSAGES_RETENTION_SECONDS = 24 * 60 * 60

# Старые отдельные файлы (до Stage 3): читаются один раз для миграции,
# если нового STATE_FILE ещё нет. Дальше не используются.
SENT_MESSAGES_FILE = os.getenv(
    "SENT_MESSAGES_FILE",
    "/tmp/lukas_alarm_sent_messages.json",
)
LEGACY_STATUS_MESSAGE_FILE = os.getenv(
    "STATUS_MESSAGE_FILE",
    "/tmp/lukas_alarm_status_message_id",
)

REQUEST_TIMEOUT = (5, 35)
SOURCE_REQUEST_TIMEOUT = (5, 15)

# Короткий таймаут исключительно для быстрой изолированной
# отправки recovery-сообщения в /external-recovered.
# Не используется нигде больше и никак не влияет на
# REQUEST_TIMEOUT / telegram_request().
RECOVERY_TELEGRAM_TIMEOUT = (3, 5)

# Если recovery-сообщение уже было успешно отправлено недавно —
# повторный вызов /external-recovered (например, если Cloudflare
# Watchdog сам повторяет запрос после 502) не должен создавать
# второе сообщение UP. Окно намеренно небольшое: реальный новый
# инцидент DOWN->UP физически не может произойти настолько быстро
# после предыдущего восстановления (см. FAILURE_NOTIFICATION_AFTER_SECONDS
# и интервалы проверок), а Cloudflare-ретраи одного и того же
# события укладываются в это окно.
EXTERNAL_RECOVERY_DEDUP_WINDOW_SECONDS = 60

# Если самая первая быстрая попытка отправки recovery-сообщения
# не удалась (например, сеть/Telegram ещё не готовы сразу после
# автоматического рестарта Render), выполняются несколько
# ДОГОНЯЮЩИХ попыток в фоне, не блокируя ответ Cloudflare Watchdog.
# Каждая попытка — это тот же самый короткий одиночный запрос
# (send_telegram_message_recovery_fast), без telegram_request().
EXTERNAL_RECOVERY_RETRY_ATTEMPTS = 3
EXTERNAL_RECOVERY_RETRY_DELAY_SECONDS = 5

PARSER_HEARTBEAT_FILE = "/tmp/lukas_alarm_parser_heartbeat"

KYIV_TZ = ZoneInfo("Europe/Kyiv")


# ============================================================
# ТЕКСТЫ И ШАБЛОНЫ СООБЩЕНИЙ
# ============================================================
#
# Единый конфигурационный блок для ВСЕХ пользовательских текстов,
# формулировок и оформления закреплённого сообщения.
#
# Правило: чтобы изменить текст, формулировку, эмодзи или порядок
# строк в любом сообщении бота — правьте только словарь MESSAGES
# ниже. Логика ниже по файлу (кто, когда и почему отправляет
# сообщение) остаётся неизменной и не должна трогаться при
# правках текста.
#
# Шаблоны используют Python str.format() с именованными
# плейсхолдерами {в_фигурных_скобках}. Подставляемые значения
# (например, текст поста из Telegram-канала) вставляются как
# обычные строки и не интерпретируются как часть шаблона, даже
# если сами содержат символы "{" или "}".

MESSAGES = {

    # --------------------------------------------------------
    # Оперативные тревожные сообщения (PSZSU / monitor)
    # --------------------------------------------------------

    "alert_title_impact_confirmed": (
        "🟡 внимание 🟡\n"
        "\n"
        "<b>ПОДТВЕРЖДЕНИЕ АТАКИ НА КРЕМЕНЧУГ / РАЙОН</b>"
    ),

    "alert_title_threat": (
        "🚨 внимание 🚨\n"
        "\n"
        "<b>УГРОЗА ДЛЯ КРЕМЕНЧУГА</b>"
    ),

    # {source_label} — явная визуальная маркировка источника.
    # {title} — один из alert_title_* выше.
    # {post_link} — прямая ссылка на конкретный исходный пост.
    # {escaped_text} — экранированный текст исходного поста.
    # post_id в тревогу намеренно НЕ выводится.
    "alert_body": (
        "{title}\n"
        "\n"
        "{source_label}\n"
        "\n"
        "<blockquote><b>{escaped_text}</b></blockquote>\n"
        "\n"
        '<a href="{post_link}">🔗 Оригинальный текст сообщения в первоисточнике</a>'
    ),

    # --------------------------------------------------------
    # Картотека Кременчуга
    # --------------------------------------------------------

    # Архивная карточка содержит время получения ботом,
    # время публикации исходного поста, источник, уровень внимания,
    # ID исходного сообщения, прямую ссылку на исходный пост
    # и текст самого сообщения.
    "archive_body": (
        "📚 КАРТОТЕКА\n"
        "\n"
        "🕐 Получено ботом: {received_at}\n"
        "🕐 Время сообщения: {published_at}\n"
        "📡 Источник: {source_name}\n"
        "🆔 ID исходного сообщения: {post_id}\n"
        '<a href="{post_link}">🔗 Оригинальное сообщение</a>\n'
        "\n"
        "📝 Сообщение:\n"
        "<blockquote>{escaped_text}</blockquote>"
    ),

    # --------------------------------------------------------
    # Команда /test
    # --------------------------------------------------------

    "test_command_response": (
        "🔔 ТЕСТОВОЕ УВЕДОМЛЕНИЕ\n"
        "\n"
        "Бот получил команду /test.\n"
        "Связь с Telegram и рабочей группой проверена."
    ),

    # --------------------------------------------------------
    # Watchdog: заголовки и формулировки по типу сбоя
    # (parser / pszsu / monitor / telegram)
    # --------------------------------------------------------

    "watchdog_failure_titles": {
        "parser": "🔴 ПРОБЛЕМА СИСТЕМЫ",
        "pszsu": "🔴 ПРОБЛЕМА ИСТОЧНИКА PSZSU",
        "monitor": "🔴 ПРОБЛЕМА ИСТОЧНИКА MONITOR",
        "telegram": "🔴 ПРОБЛЕМА TELEGRAM API",
        "default": "🔴 ПРОБЛЕМА СИСТЕМЫ",
    },

    # {reason} и {last_check} подставляются там, где присутствуют
    # в конкретном шаблоне; лишние именованные аргументы str.format()
    # игнорирует, поэтому шаблон "parser" ниже намеренно не использует
    # {reason} (сохранено исходное поведение).
    "watchdog_failure_details": {
        "parser": (
            "Парсер не выполняет проверки.\n"
            "Последняя проверка: {last_check}\n"
            "Причина: причина не определена."
        ),
        "pszsu": (
            "Источник PSZSU временно недоступен "
            "или произошла ошибка при его обработке.\n"
            "Причина: {reason}"
        ),
        "monitor": (
            "Источник monitor временно недоступен "
            "или произошла ошибка при его обработке.\n"
            "Причина: {reason}"
        ),
        "telegram": (
            "Бот не может нормально связаться с Telegram Bot API.\n"
            "Причина: {reason}"
        ),
    },

    # {failure_title}, {details}, {threshold_seconds}
    # Используется только для реальной гибели/неработоспособности
    # самого бота (parser / telegram). Не используется для
    # временной недоступности отдельного источника (pszsu / monitor).
    "watchdog_failure_body": (
        "{failure_title}\n"
        "\n"
        "⚠️ ВНИМАНИЕ!\n"
        "\n"
        "БОТ НЕ РАБОТАЕТ.\n"
        "НА ЕГО УВЕДОМЛЕНИЯ НЕЛЬЗЯ РАССЧИТЫВАТЬ.\n"
        "\n"
        "{details}\n"
        "\n"
        "Проблема длится более {threshold_seconds} секунд."
    ),

    # {recovery_reason}, {duration}
    # Пара к watchdog_failure_body: восстановление именно
    # самого бота (parser / telegram).
    "watchdog_recovery_body": (
        "🟢 СИСТЕМА ВОССТАНОВЛЕНА\n"
        "\n"
        "✅ БОТ СНОВА АКТИВЕН.\n"
        "НА ЕГО УВЕДОМЛЕНИЯ СНОВА МОЖНО РАССЧИТЫВАТЬ.\n"
        "\n"
        "{recovery_reason}\n"
        "Длительность сбоя: {duration}"
    ),

    # {failure_title}, {details}, {threshold_seconds}
    # Используется для временной недоступности отдельного
    # источника (pszsu / monitor). Это диагностика источника,
    # а не сообщение о гибели бота, и не должно пересекаться
    # по смыслу с сообщениями Cloudflare Watchdog.
    "watchdog_source_issue_body": (
        "{failure_title}\n"
        "\n"
        "ℹ️ ДИАГНОСТИКА ИСТОЧНИКА\n"
        "\n"
        "Источник временно недоступен "
        "или обработан с ошибкой.\n"
        "Сам бот продолжает работать.\n"
        "\n"
        "{details}\n"
        "\n"
        "Проблема длится более {threshold_seconds} секунд."
    ),

    # {recovery_reason}, {duration}
    # Пара к watchdog_source_issue_body: восстановление
    # доступности отдельного источника.
    "watchdog_source_recovery_body": (
        "🟢 ИСТОЧНИК ВОССТАНОВЛЕН\n"
        "\n"
        "{recovery_reason}\n"
        "Длительность сбоя: {duration}"
    ),

    # Отправляется самим ботом (не Cloudflare), когда внешний
    # Cloudflare Watchdog обнаружил восстановление (переход из
    # SUSPECTED или из DEAD обратно в NORMAL) и вызвал защищённый
    # endpoint /external-recovered.
    "external_check_recovered_body": (
        "✅ СИСТЕМА РАБОТАЕТ\n"
        "\n"
        "🔰 Диагностика системы проведена.\n"
        "\n"
        "🔰 Сообщения об угрозах доступны."
    ),

    "watchdog_recovery_reasons": {
        "parser": "Парсер снова выполняет проверки.",
        "pszsu": "Источник PSZSU снова доступен.",
        "monitor": "Источник monitor снова доступен.",
        "telegram": "Telegram Bot API снова доступен.",
    },

    "watchdog_failure_reasons": {
        "parser": "Парсер не обновляет heartbeat.",
        "pszsu": (
            "Источник PSZSU не отвечает или произошла ошибка "
            "при его обработке."
        ),
        "monitor": (
            "Источник monitor не отвечает или произошла ошибка "
            "при его обработке."
        ),
        "telegram": (
            "Telegram Bot API не отвечает или возвращает ошибку."
        ),
    },

    # --------------------------------------------------------
    # Закреплённое сообщение о состоянии системы
    # --------------------------------------------------------

    # Невидимый технический маркер для того, чтобы бот узнавал
    # своё собственное закреплённое сообщение после перезапуска.
    # Не показывается пользователю, но является частью оформления
    # закреплённого сообщения — поэтому вынесен именно сюда.
    # \u0422\u043e\u043b\u044c\u043a\u043e \u043d\u0435\u0432\u0438\u0434\u0438\u043c\u044b\u0435 \u0441\u0438\u043c\u0432\u043e\u043b\u044b (zero-width), \u0431\u0435\u0437 \u0432\u0438\u0434\u0438\u043c\u044b\u0445 \u0431\u0443\u043a\u0432.
    # \u0415\u0441\u043b\u0438 Telegram \u0438\u0445 \u043e\u0431\u0440\u0435\u0436\u0435\u0442, \u0431\u043e\u0442 \u0443\u0437\u043d\u0430\u0451\u0442 \u0441\u0432\u043e\u0451 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u043f\u043e \u0444\u0440\u0430\u0437\u0430\u043c
    # \u0448\u0430\u0431\u043b\u043e\u043d\u0430 (\u0441\u043c. is_own_status_text) \u0438 \u043f\u043e \u0441\u043e\u0445\u0440\u0430\u043d\u0451\u043d\u043d\u043e\u043c\u0443 ID.
    "status_marker": "\u200b\u2060\u200c\u200b\u2060\u200c\u200b",

    "icon_ok": "🟢",
    "icon_error": "🔴",

    "status_value_alive": "РАБОТАЕТ",
    "status_value_dead": "НЕТ ПРОВЕРКИ",
    "status_value_ok": "OK",
    "status_value_error": "ОШИБКА",

    "status_labels": {
        "parser": "Парсер",
        "telegram": "Telegram API",
        "pszsu": "Источник PSZSU",
        "monitor": "Источник monitor",
    },

    # Полный шаблон закреплённого сообщения. Порядок строк,
    # эмодзи и формулировки можно менять здесь свободно —
    # build_status_text() лишь вычисляет значения плейсхолдеров.
    "status_body": (
        "{marker}{last_check} 🕐 Последняя проверка   "
        "{parser_icon}{telegram_icon}{pszsu_icon}{monitor_icon}\n"
        "\n"
        "{parser_icon} {parser_label}: {parser_value}\n"
        "<i>(обрабатывает сообщения и ищет информацию об угрозах)</i>\n"
        "\n"
        "{telegram_icon} {telegram_label}: {telegram_value}\n"
        "<i>(обеспечивает связь бота с Telegram)</i>\n"
        "\n"
        "{pszsu_icon} {pszsu_label}: {pszsu_value}\n"
        "<i>(получает сообщения с официального источника)</i>\n"
        "\n"
        "{monitor_icon} {monitor_label}: {monitor_value}\n"
        "<i>(получает данные из дополнительного источника)</i>\n"
        "\n"
        "🔎 Отслеживание угроз для города Кременчуг\n"
        "⏱ Проверка: каждые {check_interval} сек.\n"
        "\n"
        "🚨 Последняя тревога: {last_alert}"
    ),
}


# ============================================================
# ФИЛЬТРЫ
# ============================================================
# Данные фильтров (списки слов, regex) вынесены в пакет filters/
# (см. импорты вверху файла: common / pszsu / monitor). В этом файле
# остаётся только логика классификации.


# ============================================================
# ENV
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID_RAW = os.getenv("CHAT_ID")
EXTERNAL_CHECK_TOKEN = os.getenv("EXTERNAL_CHECK_TOKEN")

# Отдельная тихая группа-картотека (ENV ARCHIVE_CHAT_ID). Архивные
# сообщения НИКОГДА не отправляются в основной рабочий чат CHAT_ID.
ARCHIVE_CHAT_ID_RAW = os.getenv("ARCHIVE_CHAT_ID")

if not TELEGRAM_TOKEN:
    raise RuntimeError("Не задан TELEGRAM_TOKEN")

if not CHAT_ID_RAW:
    raise RuntimeError("Не задан CHAT_ID")

try:
    CHAT_ID = int(CHAT_ID_RAW)
except ValueError:
    raise RuntimeError("CHAT_ID должен быть числом")

# Ошибка конфигурации картотеки НЕ валит бот: тревоги работают,
# архив просто не отправляется (и ошибка явно пишется в лог).
ARCHIVE_CHAT_ID = None
ARCHIVE_CONFIG_ERROR = None

if not ARCHIVE_CHAT_ID_RAW or not ARCHIVE_CHAT_ID_RAW.strip():
    ARCHIVE_CONFIG_ERROR = "переменная ARCHIVE_CHAT_ID не задана"
else:
    try:
        ARCHIVE_CHAT_ID = int(ARCHIVE_CHAT_ID_RAW.strip())
    except ValueError:
        ARCHIVE_CONFIG_ERROR = "ARCHIVE_CHAT_ID должен быть целым числом"

if ARCHIVE_CHAT_ID is not None and ARCHIVE_CHAT_ID == CHAT_ID:
    ARCHIVE_CHAT_ID = None
    ARCHIVE_CONFIG_ERROR = (
        "ARCHIVE_CHAT_ID совпадает с CHAT_ID: картотека не должна "
        "быть рабочим чатом"
    )


# ============================================================
# ОЧИСТКА СЕКРЕТОВ (токен бота не должен попадать ни в логи, ни в HTTP)
# ============================================================
#
# Исключения requests содержат полный URL запроса, а в нём Bot Token:
# https://api.telegram.org/bot<TOKEN>/getMe
# Единый механизм: sanitize_secrets() маскирует токен в любой строке.
# Весь вывод модуля идёт через print() ниже (он пропускает строки через
# sanitize_secrets), а всё, что уходит в HTTP-ответы, очищается явно.

_BOT_URL_TOKEN_RE = re.compile(r"(/bot)[^/\s'\"<>)]+")
_BOT_TOKEN_SHAPE_RE = re.compile(r"\bbot\d{6,}:[A-Za-z0-9_-]{20,}")


def sanitize_secrets(value):
    text = str(value)

    for secret in (TELEGRAM_TOKEN, EXTERNAL_CHECK_TOKEN):
        if secret:
            text = text.replace(secret, "***")

    text = _BOT_URL_TOKEN_RE.sub(r"\1***", text)
    text = _BOT_TOKEN_SHAPE_RE.sub("bot***", text)

    return text


_builtin_print = print


def print(*args, **kwargs):  # noqa: A001 - намеренная замена print модуля
    _builtin_print(
        *(
            sanitize_secrets(arg) if isinstance(arg, str) else arg
            for arg in args
        ),
        **kwargs,
    )


def _safe_threading_excepthook(args):
    # Необработанное исключение потока тоже может содержать URL с токеном.
    import traceback

    print(
        f"Необработанное исключение в потоке {args.thread.name if args.thread else '?'}: "
        + "".join(
            traceback.format_exception(
                args.exc_type,
                args.exc_value,
                args.exc_traceback,
            )
        ),
        flush=True,
    )


threading.excepthook = _safe_threading_excepthook


# ============================================================
# СОСТОЯНИЕ
# ============================================================

state = {
    "parser_running": False,
    "telegram_api_ok": False,

    "pszsu_ok": False,
    "monitor_ok": False,

    "last_check": None,
    "last_pszsu_check": None,
    "last_monitor_check": None,
    "last_alert": None,

    "parser_heartbeat": None,

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

    # Момент последней УСПЕШНОЙ отправки recovery-сообщения через
    # /external-recovered. Используется только для дедупликации
    # UP-сообщений (см. EXTERNAL_RECOVERY_DEDUP_WINDOW_SECONDS) и
    # никак не пересекается с parser/pszsu/monitor/telegram
    # failure-состояниями выше.
    "last_external_recovery_sent_at": None,
}

state_lock = threading.Lock()

# Отдельный лок для recovery-доставки в /external-recovered.
# Гарантирует, что параллельные вызовы этого endpoint (например,
# повтор со стороны Cloudflare Watchdog после 502, или фоновые
# догоняющие попытки) не запускают несколько одновременных
# попыток отправки одного и того же UP-сообщения. Не связан с
# state_lock и не используется больше нигде в боте.
#
# ВАЖНО (исправление зависания "already in progress"):
# ответственность за release() этого лока может передаваться
# из обработчика /external-recovered в фоновый поток
# _external_recovery_background_retry(). Раньше эта передача не
# была защищена try/finally: если между захватом лока и стартом
# фонового потока происходило любое непредвиденное исключение,
# лок оставался заблокированным НАВСЕГДА (освобождать его после
# этого было уже некому), и все последующие вызовы
# /external-recovered бесконечно получали 503 "recovery send
# already in progress". Теперь вся секция обёрнута в
# try/except/finally с явным флагом handed_off — см.
# external_recovered().
external_recovery_lock = threading.Lock()


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

# Единое персистентное состояние (STATE_FILE в STATE_DIR):
#   sent_messages       - реестр уже обработанных постов (ключ -> время);
#   status_message_id   - ID собственного статус-сообщения.
# Реестр отвечает ТОЛЬКО на вопрос «этот пост уже обработан?».
# Он НЕ влияет на срок актуальности: возраст поста проверяется
# отдельно (MAX_MESSAGE_AGE_MINUTES) и до классификации, поэтому
# отсутствие post_id в реестре не может оживить старую тревогу.

sent_messages = set()
sent_messages_ts = {}
sent_messages_lock = threading.Lock()
state_file_lock = threading.Lock()
persisted_status_message_id = None


def _parse_state_payload(data):
    """Разбирает содержимое state-файла -> (реестр {ключ: время}, status_id)."""
    now_ts = time.time()
    sent = {}
    status_id = None

    if isinstance(data, list):
        # Старый формат: просто список ключей.
        for item in data:
            if isinstance(item, str):
                sent[item] = now_ts

        return sent, None

    if not isinstance(data, dict):
        raise ValueError("неожиданная структура state-файла")

    raw = data.get("sent_messages", {})

    if isinstance(raw, dict):
        for key, value in raw.items():
            if isinstance(key, str):
                try:
                    sent[key] = float(value)
                except (TypeError, ValueError):
                    sent[key] = now_ts

    elif isinstance(raw, list):
        for item in raw:
            if isinstance(item, str):
                sent[item] = now_ts

    raw_status = data.get("status_message_id")

    if isinstance(raw_status, int) and raw_status > 0:
        status_id = raw_status

    return sent, status_id


def _read_legacy_state():
    """Одноразовая миграция из старых отдельных файлов (если есть)."""
    sent = {}
    status_id = None

    try:
        with open(SENT_MESSAGES_FILE, "r", encoding="utf-8") as f:
            sent, _ = _parse_state_payload(json.load(f))
    except Exception:
        sent = {}

    try:
        with open(LEGACY_STATUS_MESSAGE_FILE, "r", encoding="utf-8") as f:
            value = int(f.read().strip())

        status_id = value if value > 0 else None
    except Exception:
        status_id = None

    return sent, status_id


def read_state_file():
    """
    Читает state-файл -> (реестр, status_id).
    Нет файла - не ошибка (пробуем старые файлы, иначе пусто).
    Повреждённый файл не валит бот: он откладывается в сторону
    (.corrupt-<время>), бот стартует с пустым состоянием.
    """
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        return _parse_state_payload(data)

    except FileNotFoundError:
        return _read_legacy_state()

    except Exception as e:
        print(
            "Ошибка загрузки состояния, файл будет отложен: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )

        try:
            os.replace(
                STATE_FILE,
                f"{STATE_FILE}.corrupt-{int(time.time())}",
            )
        except Exception:
            pass

        return {}, None


def load_sent_messages():
    """Совместимость: множество ключей реестра из state-файла."""
    return set(read_state_file()[0])


def load_state():
    """Загружает состояние с диска в память (вызывается при старте)."""
    global persisted_status_message_id

    sent, status_id = read_state_file()

    with sent_messages_lock:
        sent_messages.clear()
        sent_messages.update(sent)
        sent_messages_ts.clear()
        sent_messages_ts.update(sent)
        persisted_status_message_id = status_id


def persist_state():
    """
    Безопасно сохраняет состояние: временный файл -> fsync ->
    atomic replace. Ошибка записи логируется и бот не останавливает.
    """
    try:
        cutoff = time.time() - SENT_MESSAGES_RETENTION_SECONDS

        with sent_messages_lock:
            for key in [
                k for k, t in sent_messages_ts.items()
                if t < cutoff or k not in sent_messages
            ]:
                sent_messages_ts.pop(key, None)
                sent_messages.discard(key)

            for key in sent_messages:
                sent_messages_ts.setdefault(key, time.time())

            payload = {
                "version": 1,
                "sent_messages": {
                    key: sent_messages_ts[key]
                    for key in sorted(sent_messages)
                },
                "status_message_id": persisted_status_message_id,
            }

        with state_file_lock:
            os.makedirs(STATE_DIR, exist_ok=True)

            tmp_file = f"{STATE_FILE}.tmp"

            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())

            os.replace(tmp_file, STATE_FILE)

    except Exception as e:
        print(
            "Ошибка сохранения состояния: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )


def register_sent(key):
    """Фиксирует ключ как обработанный и сохраняет состояние."""
    with sent_messages_lock:
        sent_messages.add(key)
        sent_messages_ts[key] = time.time()

    persist_state()


load_state()

if STATE_DIR.startswith("/tmp"):
    print(
        "ВНИМАНИЕ: STATE_DIR находится в /tmp - состояние не переживёт "
        "пересоздание контейнера. Укажите STATE_DIR на persistent disk.",
        flush=True,
    )

if ARCHIVE_CONFIG_ERROR:
    print(
        "ОШИБКА КОНФИГУРАЦИИ: картотека отключена - "
        f"{ARCHIVE_CONFIG_ERROR}. Тревоги продолжают работать.",
        flush=True,
    )


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


def format_time_hm(dt):
    if not dt:
        return "—"

    try:
        return dt.astimezone(KYIV_TZ).strftime("%H:%M")
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
# HEARTBEAT
# ============================================================

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


PARSER_THREAD_NAME = "telegram-monitor"
PARSER_BEAT_MIN_INTERVAL_SECONDS = 1.0
_last_parser_beat = 0.0


def parser_progress_beat():
    """
    Heartbeat «парсер жив и продвигается», а не только «цикл завершён».

    Вызывается из мест, где поток парсера может надолго блокироваться
    (запросы к Telegram с retry/429, загрузка источников, обработка
    постов). Срабатывает ТОЛЬКО в потоке парсера: вызовы
    telegram_request() из status/commands/watchdog-потоков heartbeat
    не обновляют, поэтому реальное зависание или гибель парсера
    по-прежнему видны (heartbeat перестаёт обновляться). Пороги
    watchdog не менялись.
    """
    global _last_parser_beat

    if threading.current_thread().name != PARSER_THREAD_NAME:
        return

    now_monotonic = time.monotonic()

    if now_monotonic - _last_parser_beat < PARSER_BEAT_MIN_INTERVAL_SECONDS:
        return

    _last_parser_beat = now_monotonic
    write_parser_heartbeat()


def get_parser_heartbeat_age():
    try:
        mtime = os.path.getmtime(PARSER_HEARTBEAT_FILE)

        return max(
            0,
            time.time() - mtime,
        )

    except (FileNotFoundError, OSError):
        return None


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
    /health теперь использует ровно ту же модель определения
    работоспособности, что и perform_external_self_check()
    (см. /external-check ниже): тот же учёт STARTUP_GRACE_SECONDS
    и те же реальные критерии аварии (heartbeat отсутствует/устарел,
    parser не запущен, Telegram API недоступен). Временная
    недоступность PSZSU или monitor (в т.ч. last_pszsu_check is
    None / last_monitor_check is None) больше не считается
    аварией и не может дать здесь ложный 503 — это диагностика
    источника, а не здоровье процесса.
    """

    ok, reason = perform_external_self_check()

    if ok:
        return "OK", 200

    print(
        f"HEALTH 503: {reason}",
        flush=True,
    )

    return (
        f"NOT OK: {sanitize_secrets(reason)}",
        503,
    )


# ============================================================
# ВНЕШНЯЯ САМОПРОВЕРКА
# ============================================================

def telegram_api_fast_check():
    """
    Быстрая независимая проверка Telegram Bot API для /external-check.
    Не использует обычный telegram_request(), чтобы не ждать до 35 секунд
    и не запускать несколько повторных попыток.
    """

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getMe"

    try:
        response = session.post(
            url,
            data={},
            timeout=(2, 2),
        )

        if response.status_code != 200:
            with state_lock:
                state["telegram_api_ok"] = False

            return False, (
                f"Telegram Bot API вернул HTTP "
                f"{response.status_code}"
            )

        result = response.json()

        if not result.get("ok"):
            with state_lock:
                state["telegram_api_ok"] = False

            return False, "Telegram Bot API вернул ошибку"

        with state_lock:
            state["telegram_api_ok"] = True

        return True, None

    except Exception as e:
        with state_lock:
            state["telegram_api_ok"] = False

        # Полный текст исключения содержит URL с токеном бота, поэтому
        # наружу (в причину для /health) уходит только тип ошибки;
        # очищенный подробный текст пишется в лог.
        print(
            "Telegram API fast check: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )

        return False, f"{type(e).__name__} (Telegram API unavailable)"


def perform_external_self_check():
    """
    Немедленная самопроверка по запросу внешнего контроля.

    Исправление №1 (ТЗ): использует ту же стартовую логику,
    что и /health. Во время STARTUP_GRACE_SECONDS бот не может
    считаться аварийным.

    Правильные критерии смерти процесса (и только они):
      - heartbeat парсера отсутствует;
      - heartbeat парсера слишком старый;
      - parser не запущен;
      - Telegram API действительно недоступен.

    Временная недоступность отдельного источника
    (last_pszsu_check is None, last_monitor_check is None,
    pszsu_ok == False, monitor_ok == False) сама по себе НЕ
    считается смертью бота — это диагностика источника, а не
    авария процесса, и не должна использоваться этой функцией.
    """

    with state_lock:
        started_at = state["started_at"]

    if (
        started_at is None
        or (
            now_utc() - started_at
        ).total_seconds()
        < STARTUP_GRACE_SECONDS
    ):
        return True, "Стартовый период, самопроверка пропущена"

    problems = []

    heartbeat_age = get_parser_heartbeat_age()

    with state_lock:
        parser_running = state["parser_running"]

    if not parser_running:
        problems.append("парсер не запущен")

    if heartbeat_age is None:
        problems.append("heartbeat парсера отсутствует")

    elif heartbeat_age > PARSER_STALE_AFTER_SECONDS:
        problems.append(
            f"heartbeat парсера устарел ({int(heartbeat_age)} сек.)"
        )

    telegram_ok, telegram_reason = telegram_api_fast_check()

    if not telegram_ok:
        problems.append(
            "Telegram Bot API недоступен"
            + (
                f": {telegram_reason}"
                if telegram_reason
                else ""
            )
        )

    if problems:
        return False, "; ".join(problems)

    return True, "Самопроверка пройдена"


def perform_fast_internal_check():
    """
    Быстрая самопроверка ТОЛЬКО по внутреннему состоянию процесса,
    без единого сетевого запроса наружу (без Telegram API, без
    PSZSU, без monitor). Предназначена исключительно для
    /external-check, который должен отвечать почти мгновенно,
    чтобы не упираться в таймаут Cloudflare Worker'а.

    В отличие от perform_external_self_check() (используется
    /health), здесь НЕТ вызова telegram_api_fast_check() — именно
    он был источником задержки в несколько секунд, которая
    приводила к обрыву запроса по AbortController на стороне
    Cloudflare.

    Критерии те же по смыслу, что и раньше, но полностью
    локальные:
      - стартовый период (STARTUP_GRACE_SECONDS) — как и везде;
      - запущен ли парсер (state["parser_running"]);
      - свежий ли heartbeat парсера.

    Важно: НЕ используется parser_thread.is_alive(). Ссылка на
    объект потока, захваченная на уровне модуля, может стать
    неактуальной после запуска/перезапуска (например, если
    процесс-уровневый watchdog перезапускал поток или произошла
    любая другая внутренняя пересборка), из-за чего is_alive()
    ложно показывал "поток мёртв", хотя парсер реально работает
    и heartbeat свежий. Источник истины — state["parser_running"]
    (выставляется самим циклом парсера) и heartbeat-файл, а не
    объект потока.
    """

    with state_lock:
        started_at = state["started_at"]
        parser_running = state["parser_running"]

    if (
        started_at is None
        or (
            now_utc() - started_at
        ).total_seconds()
        < STARTUP_GRACE_SECONDS
    ):
        return True, "Стартовый период, самопроверка пропущена"

    problems = []

    if not parser_running:
        problems.append("парсер не запущен")

    heartbeat_age = get_parser_heartbeat_age()

    if heartbeat_age is None:
        problems.append("heartbeat парсера отсутствует")

    elif heartbeat_age > PARSER_STALE_AFTER_SECONDS:
        problems.append(
            f"heartbeat парсера устарел ({int(heartbeat_age)} сек.)"
        )

    if problems:
        return False, "; ".join(problems)

    return True, "Самопроверка пройдена (без проверки Telegram API)"


@app.route("/external-check")
def external_check():
    """
    Защищённый endpoint для независимого внешнего контроля.

    Должен отвечать почти мгновенно, поэтому использует только
    perform_fast_internal_check() — без каких-либо сетевых
    запросов наружу. Проверка доступности Telegram API осталась
    только в /health (perform_external_self_check), который таким
    таймингом не ограничен.
    """

    if not EXTERNAL_CHECK_TOKEN:
        return jsonify({
            "ok": False,
            "reason": "EXTERNAL_CHECK_TOKEN не настроен",
        }), 503

    supplied_token = request.headers.get(
        "X-External-Check-Token",
        "",
    )

    if not supplied_token or not secrets.compare_digest(
        supplied_token,
        EXTERNAL_CHECK_TOKEN,
    ):
        return jsonify({
            "ok": False,
            "reason": "Unauthorized",
        }), 401

    ok, reason = perform_fast_internal_check()

    payload = {
        "ok": ok,
        "reason": sanitize_secrets(reason),
        "checked_at": now_utc().astimezone(
            KYIV_TZ
        ).isoformat(),
    }

    return jsonify(payload), 200 if ok else 503


def send_telegram_message_recovery_fast(text):
    """
    Быстрая, полностью изолированная отправка Telegram-сообщения,
    предназначенная ИСКЛЮЧИТЕЛЬНО для /external-recovered.

    Причина существования этой функции: /external-recovered
    вызывается Cloudflare Watchdog, у которого короткий fetch
    timeout. Обычный send_telegram_message() использует
    telegram_request(), рассчитанный на фоновые циклы — до 3
    попыток с таймаутом REQUEST_TIMEOUT = (5, 35) и паузами между
    попытками, то есть один вызов в худшем случае может занимать
    более 100 секунд. Из-за этого при медленном ответе Telegram
    сразу после рестарта Render запрос Cloudflare обрывался по
    таймауту раньше, чем telegram_request() успевал завершиться,
    и recovery-сообщение не уходило.

    По аналогии с уже существующей telegram_api_fast_check() —
    один запрос, короткий таймаут, без повторных попыток.

    ВАЖНО: эта функция НЕ использует telegram_request() и НЕ
    используется больше нигде в боте. send_telegram_message() и
    telegram_request() остаются полностью без изменений и
    по-прежнему используются для тревог, /test, статуса и
    остальных Watchdog-уведомлений.

    Возвращает message_id при успешной отправке или None при
    ошибке. Успех/ошибка однозначно логируются (без токенов).
    """

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    data = {
        "chat_id": CHAT_ID,
        "text": text,
    }

    try:
        response = session.post(
            url,
            data=data,
            timeout=RECOVERY_TELEGRAM_TIMEOUT,
        )

        if response.status_code != 200:
            print(
                "RECOVERY TELEGRAM FAILED: "
                f"HTTP {response.status_code}",
                flush=True,
            )
            return None

        try:
            result = response.json()
        except Exception as e:
            print(
                "RECOVERY TELEGRAM FAILED: "
                f"не удалось разобрать ответ Telegram: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )
            return None

        if not result.get("ok"):
            print(
                "RECOVERY TELEGRAM FAILED: "
                "Telegram Bot API вернул ошибку "
                f"(error_code={result.get('error_code')}, "
                f"description={result.get('description')})",
                flush=True,
            )
            return None

        message_id = result.get("result", {}).get("message_id")

        if not message_id:
            print(
                "RECOVERY TELEGRAM FAILED: "
                "ответ Telegram не содержит message_id",
                flush=True,
            )
            return None

        print(
            f"RECOVERY TELEGRAM SENT (message_id={message_id})",
            flush=True,
        )

        return message_id

    except Exception as e:
        print(
            "RECOVERY TELEGRAM FAILED: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )
        return None


def _mark_external_recovery_sent():
    with state_lock:
        state["last_external_recovery_sent_at"] = now_utc()


def _external_recovery_recently_sent():
    """
    True, если recovery-сообщение уже было успешно отправлено в
    пределах EXTERNAL_RECOVERY_DEDUP_WINDOW_SECONDS. Используется
    для защиты от повторного UP-сообщения по одному и тому же
    инциденту (см. docstring external_recovered()).
    """

    with state_lock:
        last_sent = state["last_external_recovery_sent_at"]

    if last_sent is None:
        return False

    return (
        now_utc() - last_sent
    ).total_seconds() < EXTERNAL_RECOVERY_DEDUP_WINDOW_SECONDS


def _external_recovery_background_retry():
    """
    Догоняющие попытки отправки recovery-сообщения ПОСЛЕ того,
    как самая первая быстрая попытка внутри /external-recovered
    не удалась.

    Зачем это нужно: после автоматического рестарта Render HTTP
    endpoint может начать отвечать раньше, чем полностью
    восстановится сетевое окружение процесса (например, исходящие
    запросы к Telegram API временно нестабильны в первые секунды
    после рестарта). Раньше единственная быстрая попытка отправки
    recovery была одноразовой без какого-либо запасного варианта:
    при её неудаче сообщение UP терялось насовсем, пока не
    случался и не разрешался ещё один отдельный DOWN/UP-инцидент.

    Работает полностью в фоне, отдельным демон-потоком, и НИКАК
    не влияет на HTTP-ответ, который Cloudflare Watchdog уже
    получил (502) — таймаут Cloudflare этим потоком не
    затрагивается.

    Каждая попытка — это та же самая короткая изолированная
    функция send_telegram_message_recovery_fast() (один запрос,
    короткий timeout). telegram_request() здесь по-прежнему не
    используется и не изменяется.

    Лок external_recovery_lock захватывается вызывающим кодом
    (в external_recovered(), только когда этот поток УСПЕШНО
    запущен — см. флаг handed_off там) и освобождается здесь же,
    в finally, после первого успеха либо после исчерпания всех
    попыток — это гарантирует, что владение локом однозначно
    находится либо у обработчика запроса, либо у этого потока,
    но никогда не "теряется" между ними.
    """

    try:
        for attempt in range(1, EXTERNAL_RECOVERY_RETRY_ATTEMPTS + 1):
            time.sleep(EXTERNAL_RECOVERY_RETRY_DELAY_SECONDS)

            if _external_recovery_recently_sent():
                # Успех уже случился — например, эта же цепочка
                # успела отправить сообщение на предыдущей
                # итерации до записи состояния, либо (в теории)
                # параллельный путь уже всё отправил.
                return

            message_id = send_telegram_message_recovery_fast(
                MESSAGES["external_check_recovered_body"],
            )

            if message_id:
                _mark_external_recovery_sent()

                print(
                    "RECOVERY TELEGRAM SENT "
                    f"(фоновая попытка №{attempt}, "
                    f"message_id={message_id})",
                    flush=True,
                )

                return

            print(
                "RECOVERY TELEGRAM FAILED "
                f"(фоновая попытка №{attempt} из "
                f"{EXTERNAL_RECOVERY_RETRY_ATTEMPTS})",
                flush=True,
            )

        print(
            "RECOVERY TELEGRAM FAILED: все фоновые повторные "
            "попытки отправки recovery-сообщения исчерпаны.",
            flush=True,
        )

    finally:
        external_recovery_lock.release()


@app.route("/external-recovered", methods=["GET", "POST"])
def external_recovered():
    """
    Связка Cloudflare Watchdog и бота.

    Вызывается Cloudflare Watchdog в момент, когда он обнаружил
    восстановление внешней проверки — как при переходе
    SUSPECTED -> NORMAL, так и при переходе DEAD -> NORMAL. Сам
    факт вызова этого endpoint означает, что Cloudflare Watchdog
    уже сбросил своё состояние обратно в NORMAL — Watchdog при
    этом сам НИЧЕГО не пишет в Telegram про восстановление,
    сообщение отправляет именно bot.py.

    ИСПРАВЛЕНИЕ (быстрая отправка, сохранено без изменений):
    раньше отправка recovery-сообщения шла через
    send_telegram_message() -> telegram_request(), у которой в
    худшем случае (несколько попыток по 35 секунд с паузами)
    вызов мог занимать более 100 секунд. Cloudflare Watchdog
    вызывает этот endpoint с коротким fetch timeout, поэтому
    запрос обрывался раньше, чем telegram_request() успевал
    закончить попытки, и recovery-сообщение не доходило до
    группы, хотя сам POST на /external-recovered в логах Render
    был виден (200 OK). Для этого конкретного endpoint по-прежнему
    используется отдельная быстрая функция
    send_telegram_message_recovery_fast() — один короткий запрос,
    без длинных повторных попыток. send_telegram_message() и
    telegram_request() не изменены и продолжают использоваться
    везде, кроме этого места.

    ДОПОЛНЕНИЕ (автоматический рестарт Render): единственной
    быстрой попытки достаточно в подавляющем большинстве случаев,
    но сразу после автоматического рестарта Render сетевой доступ
    к Telegram API может на секунды остаться нестабильным уже
    ПОСЛЕ того, как HTTP endpoint начал отвечать. Чтобы не терять
    recovery-сообщение в этом случае:
      - если первая быстрая попытка не удалась, запускается
        несколько догоняющих попыток в фоне (см.
        _external_recovery_background_retry()), не задерживая
        ответ Cloudflare Watchdog;
      - защита от дублей (EXTERNAL_RECOVERY_DEDUP_WINDOW_SECONDS +
        external_recovery_lock) гарантирует, что даже при
        повторных вызовах этого endpoint (например, если
        Cloudflare Watchdog сам повторяет запрос после 502) или
        при параллельно работающих догоняющих попытках наружу
        уйдёт не более одного сообщения UP на один инцидент.

    ИСПРАВЛЕНИЕ (утечка лока / вечные 503 "already in progress"):
    раньше секция между acquire() лока и либо его release(), либо
    стартом фонового потока-владельца НЕ была защищена
    try/finally. Если между этими точками возникало любое
    непредвиденное исключение (например, ошибка при старте
    threading.Thread из-за нехватки системных ресурсов сразу
    после рестарта Render), лок оставался захваченным навсегда —
    освобождать его после этого было уже некому, и все
    последующие вызовы /external-recovered бесконечно получали
    503 "recovery send already in progress". Теперь вся секция
    обёрнута в try/except/finally с явным флагом handed_off:
    если фоновый поток успешно запущен, ответственность за
    release() переходит к нему (как и раньше); в любом другом
    случае (ошибка старта потока, любое другое исключение) —
    лок release()-ится прямо здесь, в finally этого обработчика.
    Владелец лока в любой момент времени определён однозначно.

    Результат отправки по-прежнему проверяется явно: 200
    возвращается только если сообщение реально ушло (получен
    message_id) или уже было отправлено недавно по этому же
    инциденту; 502 — если быстрая попытка не удалась (при этом
    запускаются фоновые повторы); 500 — если внутри обработчика
    произошла непредвиденная ошибка (лок при этом всё равно
    гарантированно освобождается).

    Защищён тем же токеном, что и /external-check.
    """

    if not EXTERNAL_CHECK_TOKEN:
        return jsonify({
            "ok": False,
            "reason": "EXTERNAL_CHECK_TOKEN не настроен",
        }), 503

    supplied_token = request.headers.get(
        "X-External-Check-Token",
        "",
    )

    if not supplied_token or not secrets.compare_digest(
        supplied_token,
        EXTERNAL_CHECK_TOKEN,
    ):
        return jsonify({
            "ok": False,
            "reason": "Unauthorized",
        }), 401

    if _external_recovery_recently_sent():
        print(
            "/external-recovered: recovery-сообщение уже было "
            "отправлено недавно по этому инциденту — повторная "
            "отправка пропущена (защита от дублей).",
            flush=True,
        )

        return jsonify({
            "ok": True,
            "reason": "recovery already sent recently",
        }), 200

    acquired = external_recovery_lock.acquire(timeout=1.5)

    if not acquired:
        # Отправка recovery уже выполняется прямо сейчас — либо
        # синхронно по параллельному вызову, либо фоновыми
        # повторными попытками. Новую попытку не запускаем, чтобы
        # не отправить дублирующее сообщение.
        print(
            "/external-recovered: отправка recovery уже "
            "выполняется параллельно — новая попытка не "
            "запускается.",
            flush=True,
        )

        return jsonify({
            "ok": False,
            "reason": "recovery send already in progress",
        }), 503

    # С этого момента лок захвачен именно этим запросом.
    # handed_off отслеживает, кто отвечает за release():
    #   False -> сам этот обработчик, в блоке finally ниже;
    #   True  -> фоновый поток _external_recovery_background_retry(),
    #            который уже успешно запущен.
    # Ровно один из этих путей release()-ит лок ровно один раз.
    handed_off = False

    try:
        message_id = send_telegram_message_recovery_fast(
            MESSAGES["external_check_recovered_body"],
        )

        if message_id:
            _mark_external_recovery_sent()

            print(
                "ОТПРАВЛЕНО СООБЩЕНИЕ О ВОССТАНОВЛЕНИИ "
                "(/external-recovered).",
                flush=True,
            )

            return jsonify({"ok": True}), 200

        print(
            "ОШИБКА /external-recovered: не удалось отправить "
            "сообщение о восстановлении в Telegram с первой быстрой "
            "попытки. Запускаю фоновые повторные попытки.",
            flush=True,
        )

        try:
            threading.Thread(
                target=_external_recovery_background_retry,
                name="external-recovery-retry",
                daemon=True,
            ).start()

            # Поток успешно стартовал — теперь именно он владеет
            # локом и обязан освободить его в своём finally.
            handed_off = True

        except Exception as thread_error:
            # Не удалось даже запустить фоновый поток. handed_off
            # остаётся False, поэтому лок release()-ится ниже, в
            # finally этого обработчика — иначе он остался бы
            # заблокированным навсегда (это и была причина
            # исходного бага).
            print(
                "ОШИБКА /external-recovered: не удалось запустить "
                "фоновый поток повторных попыток: "
                f"{type(thread_error).__name__}: {thread_error}",
                flush=True,
            )

        return jsonify({
            "ok": False,
            "reason": (
                "Telegram sendMessage failed, retrying in background"
                if handed_off
                else (
                    "Telegram sendMessage failed, background retry "
                    "could not start"
                )
            ),
        }), 502

    except Exception as e:
        # Любое другое непредвиденное исключение внутри критической
        # секции. Раньше именно такой случай приводил к тому, что
        # лок оставался захваченным навсегда и /external-recovered
        # бесконечно отвечал 503 "already in progress". Теперь он
        # гарантированно освобождается в finally ниже.
        print(
            "ОШИБКА /external-recovered: непредвиденное исключение "
            f"внутри обработчика: {type(e).__name__}: {e}",
            flush=True,
        )

        return jsonify({
            "ok": False,
            "reason": sanitize_secrets(
                f"internal error: {type(e).__name__}"
            ),
        }), 500

    finally:
        if not handed_off:
            external_recovery_lock.release()


# ============================================================
# TELEGRAM API
# ============================================================

def telegram_request(method, data=None, retries=3):
    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/{method}"
    )

    if data is None:
        data = {}

    last_error = "неизвестная ошибка"

    for attempt in range(retries):
        parser_progress_beat()

        try:
            response = session.post(
                url,
                data=data,
                timeout=REQUEST_TIMEOUT,
            )

            parser_progress_beat()

            if response.status_code == 429:
                try:
                    retry_after = response.json().get(
                        "parameters",
                        {},
                    ).get(
                        "retry_after",
                        5,
                    )
                except Exception:
                    retry_after = 5

                time.sleep(
                    min(
                        int(retry_after) + 1,
                        30,
                    )
                )
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
                last_error = (
                    "Telegram Bot API вернул ошибку"
                )
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
                    "Telegram API ошибка после "
                    f"{retries} попыток: "
                    f"{type(e).__name__}: {e}",
                    flush=True,
                )

                return None

            time.sleep(2 + attempt)

    with state_lock:
        state["telegram_api_ok"] = False

    print(
        "Telegram API ошибка после "
        f"{retries} попыток: {last_error}",
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


# ============================================================
# WATCHDOG
# ============================================================

# Типы сбоев, относящиеся к отдельному источнику данных, а не
# к смерти самого бота. Недоступность источника — это
# диагностика источника (Исправление №3 ТЗ), она использует
# отдельные шаблоны сообщений и не должна звучать как
# "бот не работает".
SOURCE_FAILURE_TYPES = ("pszsu", "monitor")


def set_failure_state(
    failure_type,
    reason,
):
    key_since = (
        f"{failure_type}_failure_since"
    )

    key_notified = (
        f"{failure_type}_failure_notified"
    )

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


def clear_failure_state(
    failure_type,
):
    key_since = (
        f"{failure_type}_failure_since"
    )

    key_notified = (
        f"{failure_type}_failure_notified"
    )

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


def watchdog_send_failure(
    failure_type,
    reason,
):
    key_since = (
        f"{failure_type}_failure_since"
    )

    key_notified = (
        f"{failure_type}_failure_notified"
    )

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

    failure_title = MESSAGES["watchdog_failure_titles"].get(
        failure_type,
        MESSAGES["watchdog_failure_titles"]["default"],
    )

    detail_template = MESSAGES["watchdog_failure_details"].get(
        failure_type
    )

    if detail_template:
        details = detail_template.format(
            reason=reason,
            last_check=format_time(last_check),
        )
    else:
        details = reason

    if failure_type in SOURCE_FAILURE_TYPES:
        body_template = MESSAGES["watchdog_source_issue_body"]
    else:
        body_template = MESSAGES["watchdog_failure_body"]

    text = body_template.format(
        failure_title=failure_title,
        details=details,
        threshold_seconds=FAILURE_NOTIFICATION_AFTER_SECONDS,
    )

    message_id = send_telegram_message(
        text,
    )

    if message_id:
        with state_lock:
            state[key_notified] = True

        print(
            "ОТПРАВЛЕНО УВЕДОМЛЕНИЕ О СБОЕ: "
            f"{failure_type}",
            flush=True,
        )


def watchdog_check_recovery(
    failure_type,
    recovery_reason,
):
    key_since = (
        f"{failure_type}_failure_since"
    )

    key_notified = (
        f"{failure_type}_failure_notified"
    )

    with state_lock:
        since = state[key_since]
        notified = state[key_notified]

    if since is None or not notified:
        if since is not None:
            clear_failure_state(
                failure_type
            )

        return

    duration = (
        now_utc() - since
    ).total_seconds()

    if failure_type in SOURCE_FAILURE_TYPES:
        body_template = MESSAGES["watchdog_source_recovery_body"]
    else:
        body_template = MESSAGES["watchdog_recovery_body"]

    text = body_template.format(
        recovery_reason=recovery_reason,
        duration=format_duration(duration),
    )

    message_id = send_telegram_message(
        text,
    )

    if message_id:
        clear_failure_state(
            failure_type
        )

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
                parser_running = (
                    state["parser_running"]
                )

                telegram_api_ok = (
                    state["telegram_api_ok"]
                )

                pszsu_ok = (
                    state["pszsu_ok"]
                )

                monitor_ok = (
                    state["monitor_ok"]
                )

                started_at = (
                    state["started_at"]
                )

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

            heartbeat_age = (
                get_parser_heartbeat_age()
            )

            parser_alive = (
                parser_running
                and heartbeat_age is not None
                and heartbeat_age
                <= PARSER_STALE_AFTER_SECONDS
            )

            if parser_alive:
                watchdog_check_recovery(
                    "parser",
                    MESSAGES["watchdog_recovery_reasons"]["parser"],
                )

            else:
                set_failure_state(
                    "parser",
                    MESSAGES["watchdog_failure_reasons"]["parser"],
                )

                watchdog_send_failure(
                    "parser",
                    MESSAGES["watchdog_failure_reasons"]["parser"],
                )

            # ------------------------------------------------
            # PSZSU
            # ------------------------------------------------

            if pszsu_ok:
                watchdog_check_recovery(
                    "pszsu",
                    MESSAGES["watchdog_recovery_reasons"]["pszsu"],
                )

            else:
                set_failure_state(
                    "pszsu",
                    MESSAGES["watchdog_failure_reasons"]["pszsu"],
                )

                watchdog_send_failure(
                    "pszsu",
                    MESSAGES["watchdog_failure_reasons"]["pszsu"],
                )

            # ------------------------------------------------
            # MONITOR
            # ------------------------------------------------

            if monitor_ok:
                watchdog_check_recovery(
                    "monitor",
                    MESSAGES["watchdog_recovery_reasons"]["monitor"],
                )

            else:
                set_failure_state(
                    "monitor",
                    MESSAGES["watchdog_failure_reasons"]["monitor"],
                )

                watchdog_send_failure(
                    "monitor",
                    MESSAGES["watchdog_failure_reasons"]["monitor"],
                )

            # ------------------------------------------------
            # TELEGRAM API
            # ------------------------------------------------

            if telegram_api_ok:
                watchdog_check_recovery(
                    "telegram",
                    MESSAGES["watchdog_recovery_reasons"]["telegram"],
                )

            else:
                set_failure_state(
                    "telegram",
                    MESSAGES["watchdog_failure_reasons"]["telegram"],
                )

                watchdog_send_failure(
                    "telegram",
                    MESSAGES["watchdog_failure_reasons"]["telegram"],
                )

        except Exception as e:
            print(
                "Ошибка контроля состояния: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

        time.sleep(
            WATCHDOG_INTERVAL_SECONDS
        )


# ============================================================
# PINNED STATUS
# ============================================================


# Раз в столько итераций status_loop (по STATUS_UPDATE_INTERVAL_SECONDS)
# проверяется, что собственное статус-сообщение всё ещё закреплено.
STATUS_PIN_CHECK_EVERY = 10


def load_status_message_id():
    """ID собственного статус-сообщения из единого состояния (или None)."""
    with sent_messages_lock:
        return persisted_status_message_id


def save_status_message_id(message_id):
    """Сохраняет (или стирает при None) ID собственного статус-сообщения."""
    global persisted_status_message_id

    with sent_messages_lock:
        persisted_status_message_id = (
            int(message_id) if message_id else None
        )

    persist_state()


def is_own_status_text(text):
    """Наше ли это статус-сообщение: по маркеру или по фразам шаблона."""
    text = str(text or "")

    if MESSAGES["status_marker"] in text:
        return True

    # Пробелы и переносы строк не должны влиять на распознавание.
    normalized_text = " ".join(text.split()).lower()

    return (
        "последняя проверка" in normalized_text
        and "отслеживание угроз для города кременчуг"
        in normalized_text
    )


def inspect_pinned_message():
    """
    Возвращает (вид, message_id):
      "error"   - getChat не удался, ничего нельзя утверждать;
      "none"    - в чате нет закреплённого сообщения;
      "foreign" - закреплено чужое сообщение (НЕ наш статус);
      "own"     - закреплено наше статус-сообщение.
    """
    result = telegram_request(
        "getChat",
        {
            "chat_id": CHAT_ID,
        },
    )

    if not result:
        return "error", None

    try:
        pinned = result["result"].get("pinned_message")

        if not pinned:
            return "none", None

        message_id = pinned.get("message_id")

        text = pinned.get("text", "") or pinned.get("caption", "")

        if is_own_status_text(text):
            return "own", message_id

        return "foreign", message_id

    except Exception as e:
        print(
            "Ошибка определения "
            f"закреплённого сообщения: {e}",
            flush=True,
        )

    return "error", None


def get_pinned_message_id():
    """ID закреплённого сообщения, только если это наш статус."""
    kind, message_id = inspect_pinned_message()

    return message_id if kind == "own" else None


def build_status_text():
    with state_lock:
        parser_running = (
            state["parser_running"]
        )

        telegram_api_ok = (
            state["telegram_api_ok"]
        )

        pszsu_ok = (
            state["pszsu_ok"]
        )

        monitor_ok = (
            state["monitor_ok"]
        )

        last_check = (
            state["last_check"]
        )

        last_alert = (
            state["last_alert"]
        )

    heartbeat_age = (
        get_parser_heartbeat_age()
    )

    parser_alive = (
        parser_running
        and heartbeat_age is not None
        and heartbeat_age
        <= PARSER_STALE_AFTER_SECONDS
    )

    icon_ok = MESSAGES["icon_ok"]
    icon_error = MESSAGES["icon_error"]

    parser_icon = (
        icon_ok
        if parser_alive
        else icon_error
    )

    parser_text = (
        MESSAGES["status_value_alive"]
        if parser_alive
        else MESSAGES["status_value_dead"]
    )

    telegram_icon = (
        icon_ok
        if telegram_api_ok
        else icon_error
    )

    telegram_text = (
        MESSAGES["status_value_ok"]
        if telegram_api_ok
        else MESSAGES["status_value_error"]
    )

    pszsu_icon = (
        icon_ok
        if pszsu_ok
        else icon_error
    )

    pszsu_text = (
        MESSAGES["status_value_ok"]
        if pszsu_ok
        else MESSAGES["status_value_error"]
    )

    monitor_icon = (
        icon_ok
        if monitor_ok
        else icon_error
    )

    monitor_text = (
        MESSAGES["status_value_ok"]
        if monitor_ok
        else MESSAGES["status_value_error"]
    )

    labels = MESSAGES["status_labels"]

    return MESSAGES["status_body"].format(
        marker=MESSAGES["status_marker"],
        parser_icon=parser_icon,
        telegram_icon=telegram_icon,
        pszsu_icon=pszsu_icon,
        monitor_icon=monitor_icon,
        updated_at=format_time_hm(now_utc()),
        parser_label=labels["parser"],
        parser_value=parser_text,
        telegram_label=labels["telegram"],
        telegram_value=telegram_text,
        pszsu_label=labels["pszsu"],
        pszsu_value=pszsu_text,
        monitor_label=labels["monitor"],
        monitor_value=monitor_text,
        keyword=KEYWORD,
        check_interval=CHECK_INTERVAL_SECONDS,
        last_check=format_time(last_check),
        last_alert=format_time(last_alert),
    )


def edit_status_message_raw(message_id, text):
    """
    Одна попытка editMessageText для статус-сообщения.
    Возвращает:
      "ok" / "not_modified" - сообщение на месте (текст принят или
                              не изменился);
      "gone"                - сообщения больше нет / оно недоступно
                              для редактирования;
      "error"               - временная ошибка (сеть, 5xx, 429...).
    "message is not modified" - не сбой Telegram API и не должно
    переводить telegram_api_ok в False.
    """
    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/editMessageText"
    )

    try:
        response = session.post(
            url,
            data={
                "chat_id": CHAT_ID,
                "message_id": message_id,
                "text": text,
                "parse_mode": "HTML",
            },
            timeout=REQUEST_TIMEOUT,
        )

        try:
            payload = response.json()
        except Exception:
            payload = {}

        if response.status_code == 200 and payload.get("ok"):
            with state_lock:
                state["telegram_api_ok"] = True

            return "ok"

        description = str(payload.get("description", "")).lower()

        if "message is not modified" in description:
            with state_lock:
                state["telegram_api_ok"] = True

            return "not_modified"

        if (
            "message to edit not found" in description
            or "message_id_invalid" in description
            or "message can't be edited" in description
        ):
            with state_lock:
                state["telegram_api_ok"] = True

            return "gone"

        if response.status_code >= 500 or response.status_code == 429:
            with state_lock:
                state["telegram_api_ok"] = False

        print(
            "Не удалось отредактировать сообщение состояния: "
            f"HTTP {response.status_code}; {description}",
            flush=True,
        )

        return "error"

    except Exception as e:
        with state_lock:
            state["telegram_api_ok"] = False

        print(
            "Не удалось отредактировать сообщение состояния: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )

        return "error"


def _adopt_status_message(message_id):
    with state_lock:
        state["status_message_id"] = message_id

    save_status_message_id(message_id)


def _pin_status_message(message_id):
    return telegram_request(
        "pinChatMessage",
        {
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "disable_notification": True,
        },
    )


def ensure_status_message():
    """
    Гарантирует одно актуальное собственное статус-сообщение.

    1. Закреплено наше сообщение  -> используем его.
    2. getChat не удался          -> НЕ создаём новое (иначе дубли),
                                     повторим на следующем проходе.
    3. Закреплено чужое/ничего    -> пробуем сохранённый ID нашего
                                     сообщения (оно могло быть
                                     откреплено); чужое закрепление
                                     за статус не принимается.
    4. Нашего сообщения нет       -> создаём и закрепляем новое.
    """
    kind, pinned_id = inspect_pinned_message()

    if kind == "error":
        print(
            "Не удалось проверить закреплённое сообщение - "
            "новое сообщение состояния не создаётся, "
            "повтор на следующем проходе.",
            flush=True,
        )
        return False

    if kind == "own":
        _adopt_status_message(pinned_id)
        update_status_message()
        return True

    stored_id = load_status_message_id()

    if stored_id:
        result = edit_status_message_raw(stored_id, build_status_text())

        if result in ("ok", "not_modified"):
            _adopt_status_message(stored_id)

            # Своё сообщение найдено, но закреплено не оно (ничего или
            # чужое сообщение): возвращаем собственный статус в закреп.
            if kind in ("none", "foreign"):
                _pin_status_message(stored_id)

            return True

        if result == "error":
            return False

        # "gone": прежнее сообщение удалено - создаём новое ниже.
        save_status_message_id(None)

    text = build_status_text()

    message_id = send_telegram_message(
        text,
        parse_mode="HTML",
    )

    if not message_id:
        print(
            "Не удалось создать "
            "сообщение состояния.",
            flush=True,
        )
        return False

    _adopt_status_message(message_id)

    pin_result = _pin_status_message(message_id)

    if not pin_result:
        print(
            "Сообщение состояния создано, "
            "но закрепить его не удалось.",
            flush=True,
        )

    return True


def update_status_message():
    with state_lock:
        message_id = (
            state["status_message_id"]
        )

    if not message_id:
        return

    text = build_status_text()

    result = edit_status_message_raw(
        message_id,
        text,
    )

    if result in ("ok", "not_modified"):
        return

    if result == "gone":
        # Статус удалён: забываем ID, status_loop создаст новый.
        print(
            "Сообщение состояния удалено - будет создано новое.",
            flush=True,
        )

        with state_lock:
            state["status_message_id"] = None

        save_status_message_id(None)
        return

    print(
        "Не удалось обновить "
        "сообщение состояния.",
        flush=True,
    )


def verify_status_pinned():
    """
    Если наше статус-сообщение не является актуальным закреплённым
    (откреплено или поверх закреплено чужое), закрепляет его заново.
    """
    with state_lock:
        message_id = state["status_message_id"]

    if not message_id:
        return

    kind, _ = inspect_pinned_message()

    # «none» - открепили совсем, «foreign» - поверх закреплено чужое.
    # В обоих случаях актуальным закреплённым должен быть наш статус.
    if kind in ("none", "foreign"):
        print(
            "Сообщение состояния не закреплено "
            f"({kind}) - закрепляю снова.",
            flush=True,
        )
        _pin_status_message(message_id)


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
        status = (
            result["result"]["status"]
        )

        return status in (
            "creator",
            "administrator",
        )

    except Exception:
        return False


def handle_test_command(message):
    chat = message.get(
        "chat",
        {},
    )

    sender = message.get(
        "from",
        {},
    )

    chat_id = chat.get(
        "id"
    )

    user_id = sender.get(
        "id"
    )

    if chat_id != CHAT_ID:
        return

    if not user_id:
        return

    if not is_group_admin(
        user_id
    ):
        print(
            "Команда /test отклонена: "
            f"пользователь {user_id} "
            "не администратор.",
            flush=True,
        )
        return

    print(
        "Получена команда /test "
        f"от администратора {user_id}.",
        flush=True,
    )

    send_telegram_message(
        MESSAGES["test_command_response"]
    )


def build_classify_reply(text, which=None):
    """
    Диагностический разбор текста: решение, правило, признак архива и
    основные сработавшие признаки. Ничего не отправляет, состояние
    (реестр, dedup) не меняет. which: None (оба) / "pszsu" / "monitor".
    """
    lines = [
        "🔎 /classify - диагностика (тревога и архив НЕ отправляются)",
    ]

    if which in (None, "pszsu"):
        decision, rule = classify_pszsu_with_rule(text)
        final = "ARCHIVE" if decision.startswith("ARCHIVE") else decision
        lines.append(
            f"PSZSU: decision={final} rule={rule} "
            f"archive={'yes' if final == 'ARCHIVE' else 'no'}"
        )

    if which in (None, "monitor"):
        decision, rule = classify_monitor_with_rule(text)
        lines.append(
            f"MONITOR: decision={decision} rule={rule} "
            f"archive={'yes' if decision == 'ARCHIVE' else 'no'}"
        )

    features = []

    if has_kremenchuk(text):
        features.append("город Кременчук")

    if pszsu_city_with_other_place(text):
        features.append("Кременчук + другой город")

    if is_post_event_report(text):
        features.append("сводка/post-event")

    if is_continuing_threat(text):
        features.append("«триває загроза»")

    if has_high_speed_threat(text):
        features.append("high-speed цель")

    if has_cruise_missile(text):
        features.append("крылатая ракета")

    if has_banderol(text):
        features.append("Бандероль")

    if has_monitor_operational_marker(text):
        features.append("маркер ціль/вихід")

    if has_monitor_uav_archive_marker(text):
        features.append("БпЛА-признак")

    lines.append(
        "Признаки: " + (", ".join(features) if features else "нет")
    )

    return "\n".join(lines)


def handle_classify_command(message):
    """/classify [pszsu|monitor] <текст> - только для администратора."""
    chat = message.get("chat", {})
    sender = message.get("from", {})

    if chat.get("id") != CHAT_ID:
        return

    user_id = sender.get("id")

    if not user_id:
        return

    if not is_group_admin(user_id):
        print(
            "Команда /classify отклонена: "
            f"пользователь {user_id} не администратор.",
            flush=True,
        )
        return

    parts = str(message.get("text", "")).split(None, 1)
    body = parts[1] if len(parts) > 1 else ""

    which = None
    first = body.split(None, 1)

    if first and first[0].lower() in ("pszsu", "monitor"):
        which = first[0].lower()
        body = first[1] if len(first) > 1 else ""

    if not body.strip():
        send_telegram_message(
            "Использование: /classify [pszsu|monitor] <текст сообщения>"
        )
        return

    print(
        f"Команда /classify от администратора {user_id}.",
        flush=True,
    )

    send_telegram_message(build_classify_reply(body, which))


def delete_service_message(message):
    """
    Удаляет служебное сообщение Telegram о том, что
    участник присоединился к группе или покинул её.
    Другие сообщения этой функцией не затрагиваются.
    """

    chat = message.get(
        "chat",
        {},
    )

    chat_id = chat.get(
        "id"
    )

    message_id = message.get(
        "message_id"
    )

    if chat_id != CHAT_ID or not message_id:
        return

    result = telegram_request(
        "deleteMessage",
        {
            "chat_id": CHAT_ID,
            "message_id": message_id,
        },
    )

    if result:
        print(
            "Удалено служебное сообщение "
            f"(message_id={message_id}).",
            flush=True,
        )


def telegram_command_listener():
    print(
        "Запускаю обработчик "
        "Telegram-команд...",
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

        if result and result.get(
            "result"
        ):
            last_update = (
                result["result"][-1]
            )

            offset = (
                last_update["update_id"]
                + 1
            )

    except Exception as e:
        print(
            "Ошибка очистки старых "
            f"Telegram-команд: {e}",
            flush=True,
        )

    while True:
        try:
            data = {
                "timeout": 20,
                "allowed_updates": (
                    '["message"]'
                ),
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

            updates = result.get(
                "result",
                [],
            )

            for update in updates:
                offset = (
                    update["update_id"]
                    + 1
                )

                message = update.get(
                    "message"
                )

                if not message:
                    continue

                if message.get(
                    "new_chat_members"
                ) or message.get(
                    "left_chat_member"
                ):
                    delete_service_message(
                        message
                    )
                    continue

                text = message.get(
                    "text",
                    "",
                ).strip()

                if not text:
                    continue

                command = (
                    text.split()[0]
                    .lower()
                )

                if (
                    command == "/test"
                    or command.startswith(
                        "/test@"
                    )
                ):
                    handle_test_command(
                        message
                    )

                if (
                    command == "/classify"
                    or command.startswith(
                        "/classify@"
                    )
                ):
                    handle_classify_command(
                        message
                    )

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

    # ВАЖНО:
    # Теперь именно status_loop отвечает за создание
    # и обновление закреплённого сообщения.
    # Основной parser от этой функции не зависит.
    iteration = 0

    while True:
        try:
            iteration += 1

            with state_lock:
                status_message_id = (
                    state["status_message_id"]
                )

            if not status_message_id:
                ensure_status_message()

            else:
                update_status_message()

                # Открепление статуса не должно оставаться незамеченным.
                if iteration % STATUS_PIN_CHECK_EVERY == 0:
                    verify_status_pinned()

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
        time_element = element.select_one(
            "time"
        )

        if not time_element:
            return None

        value = time_element.get(
            "datetime"
        )

        if not value:
            return None

        dt = datetime.fromisoformat(
            value.replace(
                "Z",
                "+00:00",
            )
        )

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt.astimezone(
            timezone.utc
        )

    except Exception:
        return None


# ============================================================
# ФИЛЬТРЫ
# ============================================================

def normalize_text(text):
    normalized = " ".join(
        text.lower()
        .replace(
            "ё",
            "е",
        )
        .split()
    )

    # Общее сокращение «м.» / «м .» перед Кременчуком.
    # Нормализуем его один раз до применения отдельных фильтров
    # ПСЗСУ и MONITOR, не смешивая их семантику.
    normalized = re.sub(
        NORMALIZE_MP_REGEX,
        "",
        normalized,
    )

    # Русское прилагательное «кременчугский/кременчугского/...»
    # (район, водохранилище) — не город. Приводим его к украинской
    # основе «кременчуцьк…», которая городом уже не считается
    # (как «Кременчуцький район»), чтобы подстрочное «кременчуг»
    # не давало городскую привязку. Сам «Кременчуг» не затрагивается.
    normalized = re.sub(
        RU_ADJECTIVE_REGEX,
        RU_ADJECTIVE_REPLACEMENT,
        normalized,
    )

    return normalized


def has_kremenchuk(text):
    normalized = normalize_text(text)

    return any(
        pattern in normalized
        for pattern in KREMENCHUK_VARIANTS
    )


def has_kremenchuk_city(text):
    normalized = normalize_text(text)

    return any(
        pattern in normalized
        for pattern in KREMENCHUK_VARIANTS
    )


def has_banderol(text):
    normalized = normalize_text(
        text
    )

    return any(
        pattern in normalized
        for pattern in BANDEROL_PATTERNS
    )


def is_banderol_reconnaissance(text):
    normalized = normalize_text(
        text
    )

    return (
        "дорозвідка по бандеролі" in normalized
        or "дорозвідка по бандеролях" in normalized
    )


def has_impact(text):
    normalized = normalize_text(
        text
    )

    return any(
        pattern in normalized
        for pattern in IMPACT_PATTERNS
    )


def has_cruise_missile(text):
    normalized = normalize_text(text)

    if any(pattern in normalized for pattern in CRUISE_MISSILE_PATTERNS):
        return True

    # "КР" — только как отдельное сокращение, чтобы не ловить
    # случайные сочетания букв внутри других слов.
    return re.search(MONITOR_CRUISE_ABBREVIATION_REGEX, normalized) is not None


def has_high_speed_threat(text):
    normalized = normalize_text(
        text
    )

    if any(
        pattern in normalized
        for pattern in HIGH_SPEED_PATTERNS
    ):
        return True

    return (
        re.search(
            MONITOR_BR_REGEX,
            normalized,
        )
        is not None
    )


def is_continuing_threat(text):
    normalized = normalize_text(
        text
    )

    return any(
        pattern in normalized
        for pattern in CONTINUING_PATTERNS
    )


def is_post_event_report(text):
    normalized = normalize_text(
        text
    )

    return any(
        pattern in normalized
        for pattern in POST_EVENT_PATTERNS
    )


# ============================================================
# КЛАССИФИКАЦИЯ КРЕМЕНЧУГА
# ============================================================
#
# Результаты классификации:
#   ALERT          -> только основной рабочий чат
#   ARCHIVE        -> только картотека
#   IGNORE         -> никуда
#
# ВАЖНО:
# 1. Текст для анализа берётся только из текущего сообщения.
#    Reply/quote/forward-контекст Telegram не анализируется.
#    Это защищает от повторных пересылок и ответов на старые посты.
# 2. Приоритет строгий: ALERT > ARCHIVE > IGNORE.
# 3. Подтверждённое событие/удар по Кременчугу или Кременчугскому
#    району сохраняется как ALERT — это существующая рабочая логика.
# 4. Для обычной новой угрозы ALERT требует явного указания
#    направления/цели на Кременчуг.


def text_has_kremenchuk_variant(normalized):
    return any(
        pattern in normalized
        for pattern in KREMENCHUK_VARIANTS
    )


def has_kremenchuk_impact_location(text):
    """
    Сохраняет старое правило подтверждённого события:
    город Кременчук или Кременчугский район допускаются.
    Одно только Кременчугское водохранилище не считается
    местом события для ALERT без отдельной threat-relevant
    конструкции.
    """
    normalized = normalize_text(text)

    if not has_kremenchuk(text):
        return False

    return not any(
        pattern in normalized
        for pattern in KREMENCHUK_RESERVOIR_PATTERNS
    )


def has_direct_kremenchuk_target(text, include_pszsu_city_first=True):
    normalized = normalize_text(text)

    # «повз/через/довкола Кременчук» сами по себе НЕ являются
    # прямой целью. При этом отдельная последующая конструкция
    # «на Кременчук» в том же сообщении всё равно может дать ALERT.
    pass_by_city = MONITOR_LEGACY_PASS_BY_REGEX

    if any(
        pattern in normalized
        for pattern in KREMENCHUK_DIRECT_ALERT_PATTERNS
    ):
        # Не блокируем сообщение, если в нём есть отдельная явная
        # целевая конструкция после упоминания прохода.
        if not pass_by_city.search(normalized):
            return True

    if re.search(
        KREMENCHUK_DIRECT_ALERT_REGEX,
        normalized,
    ) is not None:
        # «повз Кременчук» не должен превращаться в цель.
        if not pass_by_city.search(normalized):
            return True

    # Если город упомянут как точка прохода, проверяем остальной текст
    # отдельно, чтобы «повз Кременчук, курс на ...» не стал целью города.
    cleaned = pass_by_city.sub(" ", normalized)

    if re.search(KREMENCHUK_DIRECT_ALERT_REGEX, cleaned) is not None:
        return True
    if any(pattern in cleaned for pattern in KREMENCHUK_DIRECT_ALERT_PATTERNS):
        return True

    # ПСЗСУ: «Кременчук — ударний БпЛА на місто зі сходу».
    if re.search(KREMENCHUK_CITY_FIRST_TARGET_REGEX, normalized) is not None:
        return True

    # ПСЗСУ: курс/курсом Кременчук — отдельная форма направления.
    # Для MONITOR этот PSZSU-only блок не используется.
    if include_pszsu_city_first:
        if re.search(
            KREMENCHUK_PSZSU_COURSE_TARGET_REGEX,
            normalized,
        ) is not None:
            return True

        if re.search(
            KREMENCHUK_PSZSU_CITY_FIRST_ALERT_REGEX,
            normalized,
        ) is not None:
            return True

    return False


def pszsu_city_with_other_place(text):
    return re.search(PSZSU_CITY_WITH_OTHER_PLACE_REGEX, text) is not None


def pszsu_has_independent_alarm_basis(text):
    """
    Есть ли у сообщения самостоятельное основание для ALERT, помимо
    перечисления «Кременчук та <другой город>»: направление/цель на
    город, городская тревожная форма или отдельное упоминание
    Кременчука вне перечисления.
    """
    normalized = normalize_pszsu_text(text)

    if re.search(PSZSU_DIRECT_ALARM_REGEX, normalized):
        return True

    if re.search(KREMENCHUK_PSZSU_CITY_FIRST_ALERT_REGEX, normalized):
        return True

    remainder = re.sub(PSZSU_CITY_WITH_OTHER_PLACE_REGEX, " ", text)

    return pszsu_has_kremenchuk_outside_minus(remainder)


def normalize_pszsu_text(text):
    normalized = normalize_text(text)
    return re.sub(
        PSZSU_NORMALIZE_MP_REGEX,
        "",
        normalized,
    )


def pszsu_has_kremenchuk_outside_minus(text):
    normalized = normalize_pszsu_text(text)
    cleaned = re.sub(PSZSU_KREMENCHUK_MINUS_REGEX, " ", normalized)
    return re.search(
        PSZSU_STANDALONE_CITY_REGEX,
        cleaned,
    ) is not None


def classify_pszsu_with_rule(text):
    """
    PSZSU: широкий городской триггер + согласованные исключения.
    Возвращает (решение, правило). Решение: ALERT / ARCHIVE_HIGH /
    ARCHIVE_NORMAL / IGNORE (ARCHIVE_* - внутренние значения,
    check_source() приводит их к единому ARCHIVE). Правило - короткое
    имя сработавшей ветки для диагностики.
    """
    if is_continuing_threat(text):
        return "IGNORE", "ignore:continuing_threat"

    if is_post_event_report(text):
        return "IGNORE", "ignore:post_event_report"

    normalized = normalize_pszsu_text(text)

    archive_high = re.search(PSZSU_ARCHIVE_HIGH_REGEX, normalized)
    archive_direction = re.search(PSZSU_ARCHIVE_DIRECTION_REGEX, normalized)

    if archive_high or archive_direction:
        # Архивная география не должна «съедать» отдельное основание
        # для ALERT: убираем её из текста и смотрим, остался ли
        # самостоятельный Кременчук («на північ від Кременчука,
        # курс на Кременчук» -> ALERT). Если нет — это архив.
        remainder = re.sub(PSZSU_ARCHIVE_HIGH_STRIP_REGEX, " ", normalized)
        remainder = re.sub(PSZSU_ARCHIVE_DIRECTION_REGEX, " ", remainder)

        if pszsu_has_kremenchuk_outside_minus(remainder):
            return "ALERT", "alert:independent_city_beside_archive_geography"

        if archive_high:
            return "ARCHIVE_HIGH", "archive:between_cities"

        return "ARCHIVE_NORMAL", "archive:direction_from_city"

    # После удаления минус-конструкций остаётся самостоятельный город — ALERT.
    if pszsu_has_kremenchuk_outside_minus(text):
        # «Кременчук та <другой город>» без самостоятельного основания
        # для тревоги — не ALERT, а ARCHIVE.
        if pszsu_city_with_other_place(text):
            if pszsu_has_independent_alarm_basis(text):
                return "ALERT", "alert:city_with_other_place_independent_basis"

            return "ARCHIVE_NORMAL", "archive:city_with_other_place"

        return "ALERT", "alert:city_outside_minus"

    return "IGNORE", "ignore:no_independent_city"


def classify_pszsu_kremenchuk_message(text):
    """Совместимость: только решение из classify_pszsu_with_rule()."""
    return classify_pszsu_with_rule(text)[0]


def has_monitor_kremenchuk_binding(text):
    """
    Строгая привязка именно к городу Кременчук для MONITOR.
    Кременчуцький район не считается городской привязкой.
    Водохранилище учитывается отдельно: оно не отменяет прямую
    привязку к городу, если такая привязка присутствует.
    """
    normalized = normalize_text(text)

    return text_has_kremenchuk_variant(normalized)


def has_monitor_uav_archive_marker(text):
    """Проверяет наличие общего признака БпЛА/дрона для архивного сбора MONITOR."""
    normalized = normalize_text(text)
    return any(pattern in normalized for pattern in MONITOR_UAV_ARCHIVE_PATTERNS)


def has_monitor_archive_target_binding(text):
    """
    Контрольный архив MONITOR.

    Архив MONITOR не является копией всех сообщений с Кременчуком.
    Он собирает потенциально значимые цели, которые прямо связаны
    с Кременчуком, но пока не распознаны строгим ALERT-фильтром.

    БпЛА не являются отдельным условием архива: обычный БпЛА
    попадёт сюда только при прямой целевой/направленной связи
    с Кременчуком — так же, как неизвестная новая цель.
    """
    normalized = normalize_text(text)

    if not has_monitor_kremenchuk_binding(text):
        return False
    if is_post_event_report(text):
        return False
    if MONITOR_RECONNAISSANCE_MARKER in normalized:
        return False

    pass_by = MONITOR_PASS_BY_REGEX
    direct_relation = MONITOR_ARCHIVE_DIRECT_RELATION_REGEX

    if pass_by.search(normalized):
        cleaned = pass_by.sub(" ", normalized)
        if not direct_relation.search(cleaned):
            return False

    if not direct_relation.search(normalized):
        return False

    return True


def has_monitor_city_with_other_place(text):
    """
    MONITOR: Кременчук перечислен вместе с другим населённым пунктом
    («Кременчук та Полтава») в одной строке/сегменте. Само по себе это
    не ALERT (ALERT проверяется раньше и по строгим правилам MONITOR),
    а материал для ручного анализа - ARCHIVE.
    Строки берутся из исходного текста: для распознавания «другого
    города» нужен регистр букв.
    """
    for line in re.split(MONITOR_SEGMENT_SEPARATOR_REGEX, text):
        if re.search(MONITOR_CITY_WITH_OTHER_PLACE_REGEX, line):
            return True

    return False


def has_monitor_operational_marker(text):
    """
    Оперативные маркеры, достаточные для тревоги при привязке к Кременчугу:
    «ціль», «цілі», «вихід», «виходи» — как отдельные слова.
    «виходить» / «цільова» маркерами не являются.
    """
    normalized = normalize_text(text)
    return MONITOR_OPERATIONAL_MARKER_REGEX.search(normalized) is not None


def split_monitor_segments(text):
    """
    Логические сегменты сообщения MONITOR (уже нормализованные):
    разделители — перевод строки, «/», «;» и точка с пробелом
    (многоточие «...» разделителем не считается).
    Цель из одного сегмента не привязывается к городу из другого.
    Для настоящего деления по строкам сюда нужно передавать текст
    с сохранёнными переводами строк (см. extract_message_lines_text).
    """
    segments = []

    for line in re.split(MONITOR_SEGMENT_SEPARATOR_REGEX, text):
        normalized = normalize_text(line)

        if not normalized:
            continue

        for part in re.split(MONITOR_SENTENCE_SPLIT_REGEX, normalized):
            if part.strip():
                segments.append(part)

    return segments


def has_monitor_uav_archive_binding(text):
    """
    Архивный отбор MONITOR для БпЛА/дронов: БпЛА-признак и Кременчук
    в одном сегменте, вне конструкций «повз/через/довкола».
    Это только решение «сохранить для ручного анализа» (ARCHIVE);
    ALERT-фильтр MONITOR остаётся строгим и от этой функции не зависит.
    """
    if not has_monitor_kremenchuk_binding(text):
        return False

    if is_post_event_report(text):
        return False

    if MONITOR_RECONNAISSANCE_MARKER in normalize_text(text):
        return False

    pass_by_city = MONITOR_PASS_BY_REGEX

    for segment in split_monitor_segments(text):
        remainder = pass_by_city.sub(" ", segment)

        if not any(
            term in remainder for term in KREMENCHUK_CITY_PATTERNS
        ):
            continue

        if any(
            pattern in remainder
            for pattern in MONITOR_UAV_ARCHIVE_PATTERNS
        ):
            return True

    return False


def has_monitor_direct_kremenchuk_binding(text):
    """
    Проверяет, что разрешённая цель действительно привязана к Кременчугу,
    а не просто проходит рядом с городом.
    """
    normalized = normalize_text(text)

    if not has_monitor_kremenchuk_binding(text):
        return False

    # Явные конструкции направления/цели.
    # Явные конструкции направления/цели.
    # PSZSU-only городские формы здесь намеренно отключены.
    if has_direct_kremenchuk_target(text, include_pszsu_city_first=False):
        return True

    # Операционные сообщения вида «Кременчук 3 Циркони»,
    # «Кременчук — спуск балістики», «Кременчук — вихід» и т.п.
    # Разделяем короткие локальные сегменты по /, чтобы запись
    # другого города в той же строке не привязывала его цель к Кременчугу.
    # Сегменты делятся по строкам, «/», «;» и точкам (normalized уже
    # схлопнул переводы строк, поэтому делим исходный text), чтобы
    # запись другого города/другой фразы не привязывала свою цель
    # к Кременчугу.
    segments = split_monitor_segments(text)
    kremenchuk_terms = KREMENCHUK_CITY_PATTERNS

    # Локальная запись вида «Кременчук 3 Циркони»,
    # «Кременчук — спуск балістики» или «Кременчук 1х Бандероль».
    # Самого упоминания города недостаточно: конструкции
    # «Циркон довкола Кременчука» / «Бандероль через Кременчук»
    # не должны становиться тревогой.
    # Упоминание города как точки пролёта/обхода не считается
    # прямой угрозой: «повз Кременчук», «через Кременчук»,
    # «довкола Кременчука».
    pass_by_city = MONITOR_PASS_BY_REGEX

    def local_binding(piece):
        if not any(term in piece for term in kremenchuk_terms):
            return False

        local_target = MONITOR_LOCAL_TARGET_REGEX.search(piece)
        return (
            local_target is not None
            or has_monitor_operational_marker(piece)
        )

    for segment in segments:
        if not any(term in segment for term in kremenchuk_terms):
            continue

        if pass_by_city.search(segment):
            # Сегмент целиком не отбрасываем, если в нём есть
            # отдельная (через запятую) часть с самостоятельной
            # привязкой к Кременчугу: «Кременчук 3 Циркони,
            # 1 Циркон повз Кременчук». Часть с «повз/через/
            # довкола» сама по себе тревогой не является.
            parts = [p for p in segment.split(",") if p.strip()]

            if len(parts) > 1 and any(
                local_binding(p)
                for p in parts
                if not pass_by_city.search(p)
            ):
                return True

            continue

        if local_binding(segment):
            return True

    return False


def classify_monitor_strict_message(text):
    """
    Отдельный строгий классификатор только для MONITOR.

    ALERT допускается только для:
      - баллистики / БР и других HIGH_SPEED_PATTERNS;
      - Бандероли;
      - оперативных маркеров «ціль» / «вихід» при привязке к Кременчугу.

    Обычный БпЛА, «Кременчук увага», одни только взрывы, водохранилище
    и прочие сообщения тревогу не создают. Сводки не создают ни тревогу,
    ни архивную карточку.
    """
    if not has_monitor_kremenchuk_binding(text):
        return "IGNORE"

    if is_post_event_report(text):
        return "IGNORE"

    normalized = normalize_text(text)

    # Дорозвідка не является новой угрозой для отправки тревоги.
    if MONITOR_RECONNAISSANCE_MARKER in normalized:
        return "IGNORE"

    has_allowed_target = (
        has_high_speed_threat(text)
        or has_cruise_missile(text)
        or has_banderol(text)
    )
    has_operational_marker = has_monitor_operational_marker(text)

    if not (has_allowed_target or has_operational_marker):
        return "IGNORE"

    if not has_monitor_direct_kremenchuk_binding(text):
        return "IGNORE"

    # Если строгая проверка уже установила прямую привязку разрешённой
    # цели к городу, отправляем тревогу. Это одинаково относится к
    # баллистике, скоростным/крылатым целям и «Бандероли».
    return "ALERT"


def classify_monitor_with_rule(text):
    """
    MONITOR: итоговое решение (ALERT / ARCHIVE / IGNORE) и правило.
    Порядок тот же, что был в check_source(): строгий ALERT-фильтр,
    затем архивные условия, иначе IGNORE. Классификаторы не менялись.
    """
    if classify_monitor_strict_message(text) == "ALERT":
        kinds = []

        if has_high_speed_threat(text):
            kinds.append("high_speed")

        if has_cruise_missile(text):
            kinds.append("cruise")

        if has_banderol(text):
            kinds.append("banderol")

        if has_monitor_operational_marker(text):
            kinds.append("operational_marker")

        return "ALERT", "alert:strict:" + "+".join(kinds or ["target"])

    if has_monitor_archive_target_binding(text):
        return "ARCHIVE", "archive:target_binding"

    if has_monitor_uav_archive_binding(text):
        return "ARCHIVE", "archive:uav_segment"

    if not has_monitor_kremenchuk_binding(text):
        return "IGNORE", "ignore:no_city"

    if is_post_event_report(text):
        return "IGNORE", "ignore:post_event_report"

    if MONITOR_RECONNAISSANCE_MARKER in normalize_text(text):
        return "IGNORE", "ignore:reconnaissance"

    # Кременчук + другой город без самостоятельного основания для
    # ALERT (строгий фильтр уже отработал выше) -> ARCHIVE.
    if has_monitor_city_with_other_place(text):
        return "ARCHIVE", "archive:city_with_other_place"

    return "IGNORE", "ignore:no_allowed_target_or_binding"


# ============================================================
# ОТПРАВКА ОПЕРАТИВНОГО СООБЩЕНИЯ
# ============================================================

def build_alert_text(
    source_name,
    source_link,
    post_link,
    original_text,
    classification,
):
    if classification == "IMPACT_CONFIRMED":
        title = MESSAGES["alert_title_impact_confirmed"]

    else:
        title = MESSAGES["alert_title_threat"]

    if source_link.rstrip("/").endswith("/kpszsu"):
        source_label = "🇺🇦 ПОВІТРЯНІ СИЛИ ЗСУ"
    elif source_link.rstrip("/").endswith("/war_monitor"):
        source_label = "🛰️ MONITOR"
    else:
        # Запасной вариант для уже существующих/тестовых источников.
        source_label = html.escape(source_name, quote=False)

    safe_post_link = html.escape(post_link, quote=True)

    # Лимит считается по итоговой строке ПОСЛЕ экранирования:
    # оригинальный текст -> escape -> бюджет длины -> безопасное
    # ограничение. Шаблон без текста даёт размер служебной части.
    overhead = len(
        MESSAGES["alert_body"].format(
            source_label=source_label,
            title=title,
            post_link=safe_post_link,
            escaped_text="",
        )
    )

    budget = max(0, TELEGRAM_MAX_MESSAGE_LENGTH - overhead - 1)

    escaped_parts = []
    used = 0
    truncated = False

    for char in original_text:
        escaped_char = html.escape(char, quote=False)

        if used + len(escaped_char) > budget:
            truncated = True
            break

        escaped_parts.append(escaped_char)
        used += len(escaped_char)

    if truncated and escaped_parts:
        # Убираем целый последний символ (не часть сущности &amp;),
        # чтобы освободить место под многоточие.
        escaped_parts.pop()
        escaped_parts.append("…")

    escaped_text = "".join(escaped_parts)

    return MESSAGES["alert_body"].format(
        source_label=source_label,
        title=title,
        post_link=safe_post_link,
        escaped_text=escaped_text,
    )


def send_alert(
    source_name,
    source_link,
    post_id,
    original_text,
    classification,
):
    dedup_key = (
        f"{source_name}:{post_id}"
    )

    with sent_messages_lock:
        if dedup_key in sent_messages:
            return False

    post_link = build_source_post_link(
        source_link=source_link,
        post_id=post_id,
    )

    alert_text = build_alert_text(
        source_name=source_name,
        source_link=source_link,
        post_link=post_link,
        original_text=original_text,
        classification=classification,
    )

    message_id = send_telegram_message(
        alert_text,
        parse_mode="HTML",
        disable_link_preview=True,
    )

    # Только после успешной отправки считаем
    # сообщение доставленным.
    if not message_id:
        return False

    # ID фиксируем только после успешной доставки.
    # Изменение текста того же поста уже не создаст вторую тревогу.
    register_sent(dedup_key)

    with state_lock:
        state["last_alert"] = now_utc()

    print(
        f"ОТПРАВЛЕНО: {source_name}; "
        f"{classification}; {post_id}",
        flush=True,
    )

    return True


def build_archive_text(
    source_name,
    post_id,
    post_link,
    original_text,
    classification,
    received_at,
    published_at,
):
    # Для обычных постов карточка помещается в одно сообщение.
    # ВАЖНО: исходный текст здесь НЕ обрезается. Если пост слишком
    # длинный для Telegram, send_archive() отправит его продолжение
    # отдельным сообщением, чтобы архив не терял данные.
    escaped_text = html.escape(
        original_text,
        quote=False,
    )

    template = MESSAGES["archive_body"]

    return template.format(
        received_at=format_time(received_at),
        published_at=format_time(published_at),
        source_name=html.escape(source_name, quote=False),
        post_id=html.escape(str(post_id), quote=False),
        post_link=html.escape(post_link, quote=True),
        escaped_text=escaped_text,
    )


def build_source_post_link(source_link, post_id):
    """Строит прямую ссылку на конкретный публичный Telegram-пост."""
    post_id = str(post_id or "").strip()

    if "/" in post_id:
        channel_part, message_part = post_id.rsplit("/", 1)
        if channel_part and message_part.isdigit():
            return f"https://t.me/{channel_part}/{message_part}"

    # Если формат Telegram неожиданно другой, не придумываем URL.
    return source_link


def send_archive(
    source_name,
    source_link,
    post_id,
    original_text,
    classification,
    published_at,
    received_at=None,
):
    """
    Отправляет архивные сообщения в единую приватную картотеку.
    Архив один и без типов: принимается только ARCHIVE, формат
    карточки не зависит ни от источника, ни от причины попадания.
    """
    if classification != "ARCHIVE":
        return False

    if ARCHIVE_CHAT_ID is None:
        _log_archive_config_error()
        return False

    dedup_key = f"archive:{source_name}:{post_id}"

    with sent_messages_lock:
        if dedup_key in sent_messages:
            return False

    if received_at is None:
        received_at = now_utc()

    post_link = build_source_post_link(
        source_link=source_link,
        post_id=post_id,
    )

    archive_text = build_archive_text(
        source_name=source_name,
        post_id=post_id,
        post_link=post_link,
        original_text=original_text,
        classification=classification,
        received_at=received_at,
        published_at=published_at,
    )

    # Telegram принимает максимум 4096 символов в одном сообщении.
    # Сначала пробуем отправить карточку целиком. Для исключительно
    # длинных исходных постов отправляем её частями, не теряя текст.
    archive_parts = []
    if len(archive_text) <= 4096:
        archive_parts.append(archive_text)
    else:
        escaped_full_text = html.escape(
            original_text,
            quote=False,
        )

        # Первая часть сохраняет всю служебную шапку карточки.
        prefix = build_archive_text(
            source_name=source_name,
            post_id=post_id,
            post_link=post_link,
            original_text="",
            classification=classification,
            received_at=received_at,
            published_at=published_at,
        )

        prefix_marker = "<blockquote>"
        suffix_marker = "</blockquote>"
        prefix_before_text, _, prefix_after_text = prefix.partition(
            prefix_marker
        )
        prefix_after_text = prefix_after_text.rsplit(
            suffix_marker,
            1,
        )[0]

        first_capacity = max(1, 4096 - len(prefix_before_text) - len(prefix_marker) - len(suffix_marker) - len(prefix_after_text))
        first_chunk = escaped_full_text[:first_capacity]
        archive_parts.append(
            prefix_before_text
            + prefix_marker
            + first_chunk
            + suffix_marker
            + prefix_after_text
        )

        remaining = escaped_full_text[len(first_chunk):]
        continuation_header = "📝 Продолжение сообщения:\n<blockquote>"
        continuation_suffix = "</blockquote>"
        continuation_capacity = max(1, 4096 - len(continuation_header) - len(continuation_suffix))

        while remaining:
            chunk = remaining[:continuation_capacity]
            remaining = remaining[len(chunk):]
            archive_parts.append(
                continuation_header
                + chunk
                + continuation_suffix
            )

    message_ids = []

    # Многочастная карточка: каждая успешно отправленная часть
    # фиксируется отдельно, чтобы повтор после частичного сбоя
    # досылал только недостающие части, а не дублировал отправленные.
    multipart = len(archive_parts) > 1

    for index, part in enumerate(archive_parts):
        part_key = f"{dedup_key}:part{index}"

        if multipart:
            with sent_messages_lock:
                if part_key in sent_messages:
                    continue

        data = {
            "chat_id": ARCHIVE_CHAT_ID,
            "text": part,
            "parse_mode": "HTML",
            "link_preview_options": json.dumps({
                "is_disabled": True,
            }),
        }

        result = telegram_request("sendMessage", data)

        if not result:
            return False

        try:
            message_id = result["result"]["message_id"]
        except Exception:
            return False

        message_ids.append(message_id)

        if multipart:
            register_sent(part_key)

    register_sent(dedup_key)

    print(
        f"ОТПРАВЛЕНО В КАРТОТЕКУ: {source_name}; "
        f"{classification}; {post_id}; message_ids={message_ids}",
        flush=True,
    )

    return True


_archive_config_error_logged = False


def _log_archive_config_error():
    global _archive_config_error_logged

    if _archive_config_error_logged:
        return

    _archive_config_error_logged = True

    print(
        "ОШИБКА КОНФИГУРАЦИИ: архивное сообщение не отправлено - "
        f"{ARCHIVE_CONFIG_ERROR or 'ARCHIVE_CHAT_ID не настроен'}.",
        flush=True,
    )


# Архив отправляется отдельным фоновым потоком, чтобы медленная или
# недоступная картотека не задерживала обработку следующих постов и
# источников (ALERT всегда отправляется парсером синхронно и первым).
archive_queue = queue.Queue()
archive_pending = set()
archive_pending_lock = threading.Lock()
_archive_worker_thread = None
_archive_worker_lock = threading.Lock()


def _archive_worker():
    while True:
        key, kwargs = archive_queue.get()

        try:
            send_archive(**kwargs)

        except Exception as e:
            print(
                "Ошибка отправки в картотеку: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

        finally:
            with archive_pending_lock:
                archive_pending.discard(key)

            archive_queue.task_done()


def _ensure_archive_worker():
    global _archive_worker_thread

    with _archive_worker_lock:
        if (
            _archive_worker_thread is None
            or not _archive_worker_thread.is_alive()
        ):
            _archive_worker_thread = threading.Thread(
                target=_archive_worker,
                name="archive-sender",
                daemon=True,
            )
            _archive_worker_thread.start()


def enqueue_archive(**kwargs):
    """
    Ставит архивную карточку в очередь отправки. Пост, который уже
    в очереди или уже отправлен, повторно не ставится. Если отправка
    не удалась, на следующем цикле (пока пост в окне актуальности)
    карточка будет поставлена снова.
    """
    if ARCHIVE_CHAT_ID is None:
        _log_archive_config_error()
        return False

    key = f"archive:{kwargs['source_name']}:{kwargs['post_id']}"

    with sent_messages_lock:
        if key in sent_messages:
            return False

    with archive_pending_lock:
        if key in archive_pending:
            return False

        archive_pending.add(key)

    _ensure_archive_worker()
    archive_queue.put((key, kwargs))

    return True


def _current_message_text_element(post):
    """Элемент с текстом текущего сообщения (без reply/forward-контекста)."""
    text_element = post.select_one(
        ".tgme_widget_message_bubble > .tgme_widget_message_text"
    )

    if text_element is None:
        for candidate in post.select(
            ".tgme_widget_message_text"
        ):
            if candidate.find_parent(
                class_="tgme_widget_message_reply"
            ) is not None:
                continue

            if candidate.find_parent(
                class_="tgme_widget_message_forwarded_from"
            ) is not None:
                continue

            text_element = candidate
            break

    return text_element


def _clean_text_soup(text_element):
    clean_soup = BeautifulSoup(
        str(text_element),
        "html.parser",
    )

    for context in clean_soup.select(
        ".tgme_widget_message_reply, "
        ".tgme_widget_message_forwarded_from"
    ):
        context.decompose()

    return clean_soup


def extract_current_message_text(post):
    """Возвращает только текст текущего Telegram-сообщения."""
    text_element = _current_message_text_element(post)

    if text_element is None:
        return ""

    return _clean_text_soup(text_element).get_text(
        "\n",
        strip=True,
    )


def extract_message_lines_text(post):
    """
    Тот же текст, но с настоящими переводами строк (<br>) и без
    «переводов строк» между соседними inline-тегами (<b>, <a>, <i>).
    Используется ТОЛЬКО для классификации MONITOR (деление на
    сегменты); в тревогу и архив по-прежнему идёт extract_current_message_text().
    """
    text_element = _current_message_text_element(post)

    if text_element is None:
        return ""

    clean_soup = _clean_text_soup(text_element)

    for br in clean_soup.find_all("br"):
        br.replace_with("\n")

    raw = clean_soup.get_text("")

    return "\n".join(
        line.strip()
        for line in raw.split("\n")
        if line.strip()
    )


# ============================================================
# ПРОВЕРКА ОДНОГО ИСТОЧНИКА
# ============================================================

_source_state_log = {}


def log_source_state(source_name, status):
    """
    Диагностика состояния источника. Пишется только при СМЕНЕ
    состояния (без спама каждые 15 секунд). Тишина канала
    (валидная структура, нет свежих постов) - это рабочий источник.
    """
    if _source_state_log.get(source_name) == status:
        return

    _source_state_log[source_name] = status

    print(
        f"source={source_name} state={status}",
        flush=True,
    )


_logged_decisions = set()


def log_decision(source, post_id, decision, rule, text):
    """
    Почему пост получил решение: source / decision / rule / post.
    Текст поста не логируется. IGNORE пишется только если в тексте
    упомянут Кременчук (иначе это шум), и по одному разу на пост.
    """
    if decision == "IGNORE" and KEYWORD not in normalize_text(text):
        return

    key = (source, post_id, decision, rule)

    if key in _logged_decisions:
        return

    if len(_logged_decisions) > 5000:
        _logged_decisions.clear()

    _logged_decisions.add(key)

    print(
        f"source={source} decision={decision} rule={rule} post={post_id}",
        flush=True,
    )


# Backoff при технической недоступности источника: после
# SOURCE_BACKOFF_AFTER_FAILURES подряд неудачных проверок источник
# опрашивается реже (SOURCE_BACKOFF_STEPS_SECONDS, максимум 60 с);
# первая же успешная проверка возвращает обычный интервал.
# Для каждого источника счётчик свой.
SOURCE_BACKOFF_AFTER_FAILURES = 3
SOURCE_BACKOFF_STEPS_SECONDS = (30, 60)

source_failure_count = {}
source_retry_at = {}


def source_backoff_active(source_name):
    return time.monotonic() < source_retry_at.get(source_name, 0.0)


def source_record_result(source_name, ok):
    if ok:
        if source_failure_count.get(source_name):
            print(
                f"source={source_name} backoff=reset (источник снова доступен)",
                flush=True,
            )

        source_failure_count[source_name] = 0
        source_retry_at.pop(source_name, None)
        return

    failures = source_failure_count.get(source_name, 0) + 1
    source_failure_count[source_name] = failures

    if failures >= SOURCE_BACKOFF_AFTER_FAILURES:
        step = min(
            failures - SOURCE_BACKOFF_AFTER_FAILURES,
            len(SOURCE_BACKOFF_STEPS_SECONDS) - 1,
        )
        delay = SOURCE_BACKOFF_STEPS_SECONDS[step]

        source_retry_at[source_name] = time.monotonic() + delay

        print(
            f"source={source_name} backoff={delay}s "
            f"(подряд неудач: {failures})",
            flush=True,
        )


def check_source(
    source_url,
    source_name,
    source_link,
    is_monitor=False,
):
    try:
        parser_progress_beat()

        response = session.get(
            source_url,
            timeout=SOURCE_REQUEST_TIMEOUT,
        )

        parser_progress_beat()

        if response.status_code != 200:
            log_source_state(
                source_name,
                f"unavailable:http_{response.status_code}",
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
            # Пустая лента у живого канала не бывает; HTTP 200 без
            # структуры канала (заглушка, капча, редирект на превью)
            # означает, что парсер фактически «ослеп».
            if soup.select_one(
                ".tgme_channel_history, .tgme_channel_info"
            ) is not None:
                log_source_state(
                    source_name,
                    "ok:valid_channel_structure_no_posts",
                )
                return True

            log_source_state(
                source_name,
                "unavailable:http_200_without_channel_structure",
            )

            return False

        # Посты есть, но ни у одного не читается время публикации —
        # разметка изменилась, фильтр по возрасту работать не сможет.
        if not any(
            get_post_datetime(post) for post in posts
        ):
            log_source_state(
                source_name,
                "unavailable:posts_without_readable_time",
            )

            return False

        current_time = now_utc()

        # Сначала самые свежие.
        posts = list(
            reversed(posts)
        )

        fresh_count = 0

        for post in posts:
            post_datetime = (
                get_post_datetime(post)
            )

            if not post_datetime:
                continue

            age_seconds = (
                current_time
                - post_datetime
            ).total_seconds()

            if age_seconds < 0:
                age_seconds = 0

            if (
                age_seconds
                > MAX_MESSAGE_AGE_MINUTES * 60
            ):
                break

            fresh_count += 1

            text = extract_current_message_text(
                post
            )

            if not text:
                continue

            post_id = post.get(
                "data-post"
            )

            if not post_id:
                continue

            # ------------------------------------------------
            # РАЗДЕЛЬНАЯ ЛОГИКА PSZSU / MONITOR
            # ------------------------------------------------
            # PSZSU оставляем на существующей классификации без изменений.
            # Для MONITOR сначала проходит отдельный строгий тревожный
            # фильтр. Если тревога НЕ сработала, но сообщение содержит
            # Кременчук, оно отправляется ТОЛЬКО в картотеку.

            if is_monitor:
                # Для классификации MONITOR используется текст с
                # сохранёнными строками; в тревогу/архив идёт `text`.
                classify_text = (
                    extract_message_lines_text(post) or text
                )

                # ALERT > ARCHIVE > IGNORE. Строгий оперативный фильтр
                # MONITOR первым; архивные условия MONITOR свои, но
                # архив единый: любой архивный результат -> ARCHIVE.
                decision, rule = classify_monitor_with_rule(classify_text)

                log_decision(
                    "MONITOR", post_id, decision, rule, classify_text,
                )

                if decision == "ALERT":
                    send_alert(
                        source_name=source_name,
                        source_link=source_link,
                        post_id=post_id,
                        original_text=text,
                        classification="HIGH_SPEED_THREAT",
                    )
                    continue

                if decision == "ARCHIVE":
                    enqueue_archive(
                        source_name=source_name,
                        source_link=source_link,
                        post_id=post_id,
                        original_text=text,
                        classification="ARCHIVE",
                        published_at=post_datetime,
                        received_at=now_utc(),
                    )

                continue

            # ------------------------------------------------
            # PSZSU — широкий городской триггер + minus-фильтр
            # ------------------------------------------------
            classification, rule = classify_pszsu_with_rule(text)

            log_decision(
                "PSZSU",
                post_id,
                "ARCHIVE" if classification.startswith("ARCHIVE") else classification,
                rule,
                text,
            )

            if classification == "IGNORE":
                continue

            if classification == "ALERT":
                send_alert(
                    source_name=source_name,
                    source_link=source_link,
                    post_id=post_id,
                    original_text=text,
                    classification=(
                        "IMPACT_CONFIRMED"
                        if has_kremenchuk(text) and has_impact(text)
                        else "HIGH_SPEED_THREAT"
                    ),
                )
                continue

            # ARCHIVE_HIGH / ARCHIVE_NORMAL — внутренние значения
            # классификатора PSZSU. Архив единый и без типов, поэтому
            # оба превращаются в одно действие ARCHIVE.
            if classification in (
                "ARCHIVE",
                "ARCHIVE_HIGH",
                "ARCHIVE_NORMAL",
            ):
                enqueue_archive(
                    source_name=source_name,
                    source_link=source_link,
                    post_id=post_id,
                    original_text=text,
                    classification="ARCHIVE",
                    published_at=post_datetime,
                    received_at=now_utc(),
                )
                continue

        log_source_state(
            source_name,
            (
                f"ok:valid_channel_structure_fresh_posts={fresh_count}"
                if fresh_count
                else "ok:valid_channel_structure_no_fresh_posts"
            ),
        )

        return True

    except Exception as e:
        print(
            f"Ошибка проверки {source_name}: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )

        log_source_state(
            source_name,
            f"unavailable:exception_{type(e).__name__}",
        )

        return False


# ============================================================
# ОСНОВНАЯ ПРОВЕРКА
# ============================================================

def check_updates():
    """
    Полный цикл проверки двух источников.

    КРИТИЧЕСКИ ВАЖНО:
    Ошибка PSZSU не должна мешать проверке monitor.
    Ошибка monitor не должна мешать проверке PSZSU.
    """

    # --------------------------------------------------------
    # PSZSU
    # --------------------------------------------------------

    if source_backoff_active(PSZSU_NAME):
        # Backoff: источник недавно подряд не отвечал. Состояние
        # pszsu_ok остаётся неисправным (watchdog его видит), запрос
        # пропускается до конца паузы.
        pszsu_checked = False

    else:
        pszsu_checked = True

        try:
            pszsu_ok = check_source(
                source_url=PSZSU_URL,
                source_name=PSZSU_NAME,
                source_link=PSZSU_LINK,
                is_monitor=False,
            )

        except Exception as e:
            pszsu_ok = False

            print(
                "ИСКЛЮЧЕНИЕ ПРИ ПРОВЕРКЕ PSZSU: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

        source_record_result(PSZSU_NAME, pszsu_ok)

    if pszsu_checked:
        with state_lock:
            state["pszsu_ok"] = pszsu_ok
            state["last_pszsu_check"] = now_utc()

    # --------------------------------------------------------
    # MONITOR
    # --------------------------------------------------------

    if source_backoff_active(MONITOR_NAME):
        monitor_checked = False

    else:
        monitor_checked = True

        try:
            monitor_ok = check_source(
                source_url=MONITOR_URL,
                source_name=MONITOR_NAME,
                source_link=MONITOR_LINK,
                is_monitor=True,
            )

        except Exception as e:
            monitor_ok = False

            print(
                "ИСКЛЮЧЕНИЕ ПРИ ПРОВЕРКЕ MONITOR: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

        source_record_result(MONITOR_NAME, monitor_ok)

    if monitor_checked:
        with state_lock:
            state["monitor_ok"] = monitor_ok
            state["last_monitor_check"] = now_utc()

    # --------------------------------------------------------
    # ЗАВЕРШЕНИЕ ПОЛНОГО ЦИКЛА
    # --------------------------------------------------------

    completed_at = now_utc()

    with state_lock:
        state["last_check"] = completed_at

    write_parser_heartbeat()


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
        "Картотека: "
        + (
            "настроена"
            if ARCHIVE_CHAT_ID is not None
            else f"ОТКЛЮЧЕНА ({ARCHIVE_CONFIG_ERROR})"
        ),
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

        # Время запуска процесса фиксируем только один раз.
        if state["started_at"] is None:
            state["started_at"] = (
                startup_time
            )

        state["last_check"] = (
            startup_time
        )

    write_parser_heartbeat()

    print(
        "Heartbeat парсера зафиксирован.",
        flush=True,
    )

    print(
        "Parser сразу начинает проверку "
        "источников PSZSU и monitor.",
        flush=True,
    )

    # ========================================================
    # КРИТИЧЕСКИ ВАЖНО:
    #
    # Здесь НЕТ:
    #   test_telegram_api()
    #   ensure_status_message()
    #
    # Эти служебные операции больше не могут
    # остановить начало работы parser.
    # ========================================================

    while True:
        cycle_started = time.monotonic()

        try:
            check_updates()

        except Exception as e:
            # Ошибка полного цикла не должна
            # остановить parser.
            with state_lock:
                state["pszsu_ok"] = False
                state["monitor_ok"] = False

            print(
                "Ошибка цикла парсера: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

        elapsed = (
            time.monotonic()
            - cycle_started
        )

        sleep_time = max(
            0,
            CHECK_INTERVAL_SECONDS
            - elapsed,
        )

        time.sleep(
            sleep_time
        )


# ============================================================
# ВНУТРЕННИЙ WATCHDOG ПРОЦЕССА
# ============================================================
#
# Отдельная функция от watchdog_loop() выше.
# watchdog_loop() — это "внешний" по смыслу контроль состояния
# источников/Telegram API, который шлёт уведомления в чат.
#
# internal_process_watchdog_loop() — это внутренний контроль
# самого процесса. Он НИЧЕГО не пишет в Telegram и не меняет
# ни архитектуру, ни Gunicorn, ни Render, ни интервалы парсера.
#
# Его задача — то, что описано в ТЗ как "Внутренний Watchdog
# (Render)": следить за heartbeat парсера и за живостью
# критических потоков, и при обнаружении РЕАЛЬНОЙ гибели/
# зависания принудительно завершить процесс (os._exit).
# После этого gunicorn (worker=1) сам поднимает новый рабочий
# процесс — без участия Render и без изменения его настроек.
# Это устраняет ситуацию "Render показывает Live, а бот мёртв".

def internal_process_watchdog_loop():
    print(
        "Запущен внутренний watchdog процесса",
        flush=True,
    )

    # Исправление №2 (ТЗ): двухэтапная проверка.
    #
    # Проблема обнаруживается на цикле N, состояние
    # запоминается, но процесс НЕ завершается сразу.
    # На следующем цикле (через INTERNAL_WATCHDOG_INTERVAL_SECONDS)
    # проблема перепроверяется, и только если она подтвердилась
    # повторно — выполняется os._exit(1).
    #
    # Это относится отдельно к потокам и отдельно к heartbeat.
    suspected_dead_threads = set()
    heartbeat_suspected = False

    while True:
        try:
            with state_lock:
                started_at = state["started_at"]

            if (
                started_at is None
                or (
                    now_utc() - started_at
                ).total_seconds()
                < STARTUP_GRACE_SECONDS
            ):
                # На старте ещё нет смысла копить подозрения.
                suspected_dead_threads = set()
                heartbeat_suspected = False

                time.sleep(
                    INTERNAL_WATCHDOG_INTERVAL_SECONDS
                )
                continue

            # ----------------------------------------------
            # Проверка живости критических потоков.
            # Этап 1: обнаружить. Этап 2 (следующий цикл):
            # подтвердить и только тогда завершить процесс.
            # ----------------------------------------------

            critical_threads = (
                ("telegram-monitor", parser_thread),
                ("status-updater", status_thread),
                ("system-watchdog", watchdog_thread),
                ("telegram-commands", commands_thread),
            )

            currently_dead_threads = set()

            for thread_name, thread_obj in critical_threads:
                if (
                    thread_obj is not None
                    and not thread_obj.is_alive()
                ):
                    currently_dead_threads.add(thread_name)

            confirmed_dead_threads = (
                currently_dead_threads
                & suspected_dead_threads
            )

            if confirmed_dead_threads:
                print(
                    "ВНУТРЕННИЙ WATCHDOG: поток(и) "
                    f"{sorted(confirmed_dead_threads)} "
                    "подтверждённо мертвы повторной проверкой. "
                    "Принудительное завершение процесса "
                    "для автоматического перезапуска.",
                    flush=True,
                )
                os._exit(1)

            if currently_dead_threads:
                print(
                    "ВНУТРЕННИЙ WATCHDOG: обнаружена возможная "
                    f"проблема с потоком(и) {sorted(currently_dead_threads)}. "
                    "Будет перепроверено на следующем цикле "
                    f"({INTERNAL_WATCHDOG_INTERVAL_SECONDS} сек.).",
                    flush=True,
                )

            suspected_dead_threads = currently_dead_threads

            # ----------------------------------------------
            # Проверка реального зависания парсера
            # по heartbeat-файлу. Та же двухэтапная логика.
            # ----------------------------------------------

            heartbeat_age = get_parser_heartbeat_age()

            heartbeat_problem_now = (
                heartbeat_age is not None
                and heartbeat_age
                > PROCESS_HANG_THRESHOLD_SECONDS
            )

            if heartbeat_problem_now and heartbeat_suspected:
                print(
                    "ВНУТРЕННИЙ WATCHDOG: heartbeat парсера "
                    f"устарел на {int(heartbeat_age)} сек. "
                    "Зависание подтверждено повторной проверкой. "
                    "Принудительное завершение процесса "
                    "для автоматического перезапуска.",
                    flush=True,
                )
                os._exit(1)

            if heartbeat_problem_now:
                print(
                    "ВНУТРЕННИЙ WATCHDOG: обнаружено возможное "
                    f"зависание heartbeat ({int(heartbeat_age)} сек.). "
                    "Будет перепроверено на следующем цикле "
                    f"({INTERNAL_WATCHDOG_INTERVAL_SECONDS} сек.).",
                    flush=True,
                )

            heartbeat_suspected = heartbeat_problem_now

        except Exception as e:
            print(
                "Ошибка внутреннего watchdog процесса: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

        time.sleep(
            INTERNAL_WATCHDOG_INTERVAL_SECONDS
        )


# ============================================================
# ЗАПУСК ФОНОВЫХ ПОТОКОВ
# ============================================================

parser_thread = threading.Thread(
    target=run_bot,
    name=PARSER_THREAD_NAME,
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


internal_watchdog_thread = threading.Thread(
    target=internal_process_watchdog_loop,
    name="internal-process-watchdog",
    daemon=True,
)

internal_watchdog_thread.start()


# ============================================================
# LOCAL START
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "10000",
            )
        ),
    )
