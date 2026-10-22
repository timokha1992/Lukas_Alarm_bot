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
    PSZSU_POSITION_LOCATIVE_REGEX,
    PSZSU_COURSE_PHRASE_REGEXES,
    PSZSU_DIRECTION_WORD_REGEX,
    PSZSU_PASS_BY_CITY_REGEX,
    PSZSU_CANCEL_REGEX,
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
# Задержка перед уведомлением о сбое (отсчёт от фактического начала
# инцидента). Применяется ко всем типам инцидентов watchdog
# (источники, парсер, Telegram API): так устроена общая логика.
FAILURE_NOTIFICATION_AFTER_SECONDS = 90

# Последняя проверка источника старше этого срока - состояние не
# подтверждено. Запас над худшим циклом: 2 запроса по 5+15 с
# + интервал 15 с + максимальный backoff 60 с (~115 с).
WATCHDOG_STATE_STALE_AFTER_SECONDS = 180
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

    # {sources}, {details}, {threshold_seconds}
    # Одно сообщение, когда одновременно недоступны несколько
    # источников (pszsu и monitor).
    "watchdog_source_issue_multi_body": (
        "🔴 ПРОБЛЕМА ИСТОЧНИКОВ: {sources}\n"
        "\n"
        "ℹ️ ДИАГНОСТИКА ИСТОЧНИКОВ\n"
        "\n"
        "Источники временно недоступны "
        "или обработаны с ошибкой.\n"
        "Сам бот продолжает работать.\n"
        "\n"
        "{details}\n"
        "\n"
        "Проблема длится более {threshold_seconds} секунд."
    ),

    # {details}: строки "<причина восстановления> Длительность сбоя: ..."
    "watchdog_source_recovery_multi_body": (
        "🟢 ИСТОЧНИКИ ВОССТАНОВЛЕНЫ\n"
        "\n"
        "{details}"
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
    chat_id=None,
):
    """
    Отправляет сообщение. Без chat_id - в рабочий чат CHAT_ID, как всегда.
    chat_id нужен только там, где ответ должен уйти в конкретный чат
    (сейчас: ответ /classify в тот чат, откуда пришёл запрос).
    """
    data = {
        "chat_id": CHAT_ID if chat_id is None else chat_id,
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


# --------------------------------------------------------------
# Уведомления watchdog (правки 20.10.2026: согласованный план +
# доработка по ТЗ «доработка watchdog»):
#  1. решения принимаются по СВЕЖЕМУ состоянию каждого компонента,
#     а не по снимку из начала прохода; устаревшие данные источника
#     (давно не было проверки) не выдаются за актуальные;
#  2. инцидент, закрытый без доставленного уведомления, пишется
#     только в лог (запоздалых сообщений в Telegram нет);
#  3. сбои нескольких источников (pszsu / monitor) уходят ОДНИМ
#     сообщением, восстановления - тоже одним; состояния инцидентов
#     (<тип>_failure_since / _notified) и история по каждому
#     источнику (source_diag) остаются независимыми;
#  4. порог уведомления - FAILURE_NOTIFICATION_AFTER_SECONDS (90 с),
#     отсчёт от фактического времени первой неудачной проверки;
#  5. отправка - одна ограниченная попытка (без серии повторов
#     telegram_request); после первой неудачи в проходе остальные
#     отправки откладываются на следующий проход;
#  6. флаг notified ставится только после успешной отправки сообщения,
#     в котором перечислен этот источник.
# --------------------------------------------------------------

# Одна попытка отправки уведомления watchdog: connect 3 с, read 8 с.
WATCHDOG_SEND_TIMEOUT = (3, 8)

# Сколько раз пытались доставить уведомление о сбое по типу (только
# для диагностики в логе; в состояние/файл не пишется).
watchdog_send_attempts = {}

# Типы, по которым уже записано в лог «состояние устарело» (чтобы не
# повторять строку каждые 10 секунд).
watchdog_stale_logged = set()

SOURCE_DIAG_HISTORY_LIMIT = 50


# --------------------------------------------------------------
# Диагностика источников: что именно произошло по каждому из них
# --------------------------------------------------------------

def _new_source_diag():
    return {
        "consecutive_failures": 0,
        "consecutive_successes": 0,
        "first_fail_at": None,      # начало текущей серии неудач
        "last_ok_at": None,         # последняя успешная проверка
        "last_error": None,         # dict: kind/label/class/message/...
        "pending_error": None,      # исключение, ещё не учтённое
        "backoff_active": False,
        "backoff_since": None,
        "backoff_events": 0,
        "incident": None,           # открытый инцидент (dict)
        "history": [],              # закрытые инциденты
    }


source_diag = {
    "pszsu": _new_source_diag(),
    "monitor": _new_source_diag(),
}
source_diag_lock = threading.Lock()


def source_key(source_name):
    return "pszsu" if source_name == PSZSU_NAME else "monitor"


def _source_title(failure_type):
    return "PSZSU" if failure_type == "pszsu" else "MONITOR"


def classify_source_error(exc):
    """
    Тип сетевой ошибки по исключению requests. Возвращает
    (kind, человекочитаемая метка). Первопричину (Render / сеть /
    t.me) это не определяет, только различает виды отказа.
    """
    cls = type(exc).__name__
    low = str(exc).lower()
    rex = requests.exceptions

    if isinstance(exc, rex.ConnectTimeout):
        return "connect_timeout", f"тайм-аут соединения ({cls})"

    if isinstance(exc, rex.ReadTimeout):
        return "read_timeout", f"тайм-аут чтения ({cls})"

    if isinstance(exc, rex.Timeout):
        return "timeout", f"тайм-аут ({cls})"

    if isinstance(exc, rex.SSLError):
        return "ssl", f"ошибка SSL ({cls})"

    if isinstance(exc, rex.ConnectionError):
        if "network is unreachable" in low or "errno 101" in low:
            return (
                "network_unreachable",
                f"сеть недоступна: Network is unreachable ({cls})",
            )

        if any(
            s in low
            for s in (
                "name or service not known",
                "temporary failure in name resolution",
                "nodename nor servname",
                "getaddrinfo failed",
                "failed to resolve",
            )
        ):
            return "dns", f"ошибка DNS ({cls})"

        if "connection refused" in low:
            return "connection_refused", f"соединение отклонено ({cls})"

        if any(
            s in low
            for s in (
                "connection reset",
                "reset by peer",
                "connection aborted",
                "remotedisconnected",
                "remote end closed",
            )
        ):
            return "connection_reset", f"соединение сброшено ({cls})"

        return "connection_error", f"ошибка соединения ({cls})"

    return "other", f"ошибка {cls}"


def _label_from_source_state(status):
    """Метка причины для отказа без исключения (по log_source_state)."""
    status = str(status or "")

    if status.startswith("unavailable:http_200_without"):
        return "HTTP 200 без структуры канала"

    if status.startswith("unavailable:http_"):
        return "HTTP " + status.rsplit("_", 1)[-1]

    if status == "unavailable:posts_without_readable_time":
        return "не читается время публикаций"

    return status or "причина не определена"


def _watchdog_context_text():
    """Состояние остальных компонентов в момент ошибки (для лога:
    различить отказ одного источника и общий сбой исходящих
    соединений)."""
    with state_lock:
        return (
            f"pszsu_ok={state['pszsu_ok']} "
            f"monitor_ok={state['monitor_ok']} "
            f"telegram_api_ok={state['telegram_api_ok']}"
        )


def source_diag_record_error(source_name, exc, elapsed):
    """Вызывается из check_source при исключении. Не бросает."""
    try:
        kind, label = classify_source_error(exc)

        entry = {
            "kind": kind,
            "label": label,
            "class": type(exc).__name__,
            "message": sanitize_secrets(str(exc))[:200],
            "elapsed": elapsed,
            "at": now_utc(),
        }

        with source_diag_lock:
            source_diag[source_key(source_name)]["pending_error"] = entry

    except Exception as e:
        print(
            f"Ошибка записи диагностики источника: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )


def source_diag_record_result(source_name, ok, state_status=None):
    """Результат очередной проверки источника (из source_record_result)."""
    try:
        key = source_key(source_name)
        now = now_utc()
        log_line = None

        with source_diag_lock:
            d = source_diag[key]

            if ok:
                had = d["consecutive_failures"]
                d["consecutive_failures"] = 0
                d["consecutive_successes"] += 1
                d["first_fail_at"] = None
                d["last_ok_at"] = now
                d["pending_error"] = None

                if had:
                    log_line = (
                        f"SOURCE OK source={key} "
                        f"consecutive_ok={d['consecutive_successes']} "
                        f"after_failures={had}"
                    )
            else:
                entry = d["pending_error"]
                d["pending_error"] = None

                if entry is None:
                    entry = {
                        "kind": "state",
                        "label": _label_from_source_state(state_status),
                        "class": "",
                        "message": str(state_status or ""),
                        "elapsed": None,
                        "at": now,
                    }

                d["consecutive_failures"] += 1
                d["consecutive_successes"] = 0

                if d["first_fail_at"] is None:
                    d["first_fail_at"] = now

                d["last_error"] = entry

                if d["incident"] is not None:
                    d["incident"]["failed_checks"] = (
                        d["incident"].get("failed_checks", 0) + 1
                    )

                elapsed = entry["elapsed"]

                log_line = (
                    f"SOURCE ERROR source={key} kind={entry['kind']} "
                    f"class={entry['class'] or '-'} "
                    f"elapsed={'-' if elapsed is None else f'{elapsed:.1f}s'} "
                    f"consecutive={d['consecutive_failures']} "
                    f"backoff={d['backoff_active']} "
                    f"msg={entry['message']!r}"
                )

        if log_line:
            print(
                log_line
                + ("" if ok else " | " + _watchdog_context_text()),
                flush=True,
            )

    except Exception as e:
        print(
            f"Ошибка записи диагностики источника: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )


def source_diag_backoff(source_name, active, delay=None):
    try:
        key = source_key(source_name)

        with source_diag_lock:
            d = source_diag[key]
            d["backoff_active"] = bool(active)

            if active:
                d["backoff_events"] += 1
                d["backoff_since"] = now_utc()
            else:
                d["backoff_since"] = None

    except Exception:
        pass


def _source_diag_snapshot(failure_type):
    with source_diag_lock:
        d = source_diag[failure_type]
        snap = dict(d)
        snap["incident"] = (
            dict(d["incident"]) if d["incident"] else None
        )

    return snap


def source_incident_open(failure_type):
    """Фиксирует открытие инцидента источника. Возвращает фактическое
    время начала (первая неудачная проверка серии)."""
    with source_diag_lock:
        d = source_diag[failure_type]

        if d["incident"] is None:
            d["incident"] = {
                "started_at": d["first_fail_at"] or now_utc(),
                "first_error": d["last_error"],
                "failed_checks": d["consecutive_failures"],
                "notify_errors": 0,
                "notified_at": None,
            }

        started = d["incident"]["started_at"]
        first = d["incident"]["first_error"]

    print(
        f"INCIDENT START source={failure_type} "
        f"started={format_time(started)} "
        f"first_error="
        f"{first['kind'] if first else 'unknown'} | "
        + _watchdog_context_text(),
        flush=True,
    )

    return started


def source_incident_note_notify_error(failure_type):
    try:
        with source_diag_lock:
            inc = source_diag[failure_type]["incident"]

            if inc is not None:
                inc["notify_errors"] += 1

    except Exception:
        pass


def source_incident_note_notified(failure_type):
    try:
        with source_diag_lock:
            inc = source_diag[failure_type]["incident"]

            if inc is not None:
                inc["notified_at"] = now_utc()

    except Exception:
        pass


def source_incident_close(failure_type, close_reason, duration):
    """Закрывает инцидент источника: запись в историю и в лог."""
    with source_diag_lock:
        d = source_diag[failure_type]
        inc = d["incident"]
        d["incident"] = None

        if inc is None:
            inc = {
                "started_at": None,
                "first_error": None,
                "failed_checks": 0,
                "notify_errors": 0,
                "notified_at": None,
            }

        record = dict(inc)
        record["closed_at"] = now_utc()
        record["duration"] = duration
        record["close_reason"] = close_reason
        d["history"].append(record)
        del d["history"][:-SOURCE_DIAG_HISTORY_LIMIT]

    first = record["first_error"]

    print(
        f"INCIDENT CLOSED source={failure_type} "
        f"started={format_time(record['started_at'])} "
        f"duration={format_duration(duration)} "
        f"reason={close_reason} "
        f"failed_checks={record['failed_checks']} "
        f"first_error={first['kind'] if first else 'unknown'} "
        f"notify_errors={record['notify_errors']}",
        flush=True,
    )


# --------------------------------------------------------------
# Свежее состояние компонентов
# --------------------------------------------------------------

def _watchdog_component_state(failure_type):
    """
    'fail'  - компонент неисправен;
    'ok'    - исправен, данные свежие;
    'stale' - флаг «исправен», но последняя проверка источника
              слишком давняя: состояние НЕ подтверждено.
    """
    if failure_type == "parser":
        with state_lock:
            running = state["parser_running"]

        age = get_parser_heartbeat_age()

        alive = bool(
            running
            and age is not None
            and age <= PARSER_STALE_AFTER_SECONDS
        )

        return "ok" if alive else "fail"

    if failure_type == "telegram":
        with state_lock:
            return "ok" if state["telegram_api_ok"] else "fail"

    with state_lock:
        ok = bool(state[f"{failure_type}_ok"])
        last_check = state[f"last_{failure_type}_check"]

    if not ok:
        return "fail"

    if last_check is None:
        return "stale"

    age = (now_utc() - last_check).total_seconds()

    if age > WATCHDOG_STATE_STALE_AFTER_SECONDS:
        return "stale"

    return "ok"


def _watchdog_component_ok(failure_type):
    return _watchdog_component_state(failure_type) == "ok"


def _watchdog_note_stale(failure_type, is_stale):
    """Одна строка в лог на эпизод устаревших данных."""
    if not is_stale:
        watchdog_stale_logged.discard(failure_type)
        return

    if failure_type in watchdog_stale_logged:
        return

    watchdog_stale_logged.add(failure_type)

    with state_lock:
        last_check = state.get(f"last_{failure_type}_check")

    age = (
        "неизвестно"
        if last_check is None
        else f"{int((now_utc() - last_check).total_seconds())} с назад"
    )

    print(
        f"СОСТОЯНИЕ УСТАРЕЛО: {failure_type}; последняя проверка: "
        f"{age} (порог {WATCHDOG_STATE_STALE_AFTER_SECONDS} с); "
        "восстановление не подтверждено, статус «работает» не выдаётся",
        flush=True,
    )


def _watchdog_incident_snapshot(failure_type):
    with state_lock:
        return (
            state[f"{failure_type}_failure_since"],
            state[f"{failure_type}_failure_notified"],
        )


# --------------------------------------------------------------
# Тексты сообщений
# --------------------------------------------------------------

def _source_reason_text(failure_type):
    snap = _source_diag_snapshot(failure_type)
    err = snap["last_error"]

    if not err:
        return "причина не определена"

    text = err["label"]

    if err.get("elapsed") is not None and err["kind"] != "state":
        text += f", ожидание {err['elapsed']:.0f} с"

    return text


def _source_failure_line(failure_type, now):
    since, _ = _watchdog_incident_snapshot(failure_type)
    snap = _source_diag_snapshot(failure_type)

    duration = (now - since).total_seconds() if since else 0

    return (
        f"• {_source_title(failure_type)}: недоступен с "
        f"{format_time(since)} ({format_duration(duration)})\n"
        f"  Причина: {_source_reason_text(failure_type)}\n"
        f"  Неудачных проверок подряд: "
        f"{snap['consecutive_failures']}; последняя успешная: "
        f"{format_time(snap['last_ok_at'])}"
    )


def _watchdog_failure_text(failure_types, last_check):
    """Текст уведомления о сбое: один компонент или группа источников."""
    now = now_utc()

    if len(failure_types) == 1 and (
        failure_types[0] not in SOURCE_FAILURE_TYPES
    ):
        failure_type = failure_types[0]
        reason = MESSAGES["watchdog_failure_reasons"].get(
            failure_type, ""
        )
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

        return MESSAGES["watchdog_failure_body"].format(
            failure_title=failure_title,
            details=details,
            threshold_seconds=FAILURE_NOTIFICATION_AFTER_SECONDS,
        )

    lines = "\n".join(
        _source_failure_line(t, now) for t in failure_types
    )
    details = "Состояние: ОТКАЗ\n" + lines

    if len(failure_types) == 1:
        failure_type = failure_types[0]

        return MESSAGES["watchdog_source_issue_body"].format(
            failure_title=MESSAGES["watchdog_failure_titles"][
                failure_type
            ],
            details=details,
            threshold_seconds=FAILURE_NOTIFICATION_AFTER_SECONDS,
        )

    names = ", ".join(_source_title(t) for t in failure_types)

    return MESSAGES["watchdog_source_issue_multi_body"].format(
        sources=names,
        details=details,
        threshold_seconds=FAILURE_NOTIFICATION_AFTER_SECONDS,
    )


def _sources_state_line(recovered_types):
    """Итоговое состояние: полное или частичное восстановление."""
    pending = []

    for t in SOURCE_FAILURE_TYPES:
        if t in recovered_types:
            continue

        comp = _watchdog_component_state(t)

        if comp == "fail":
            since, _ = _watchdog_incident_snapshot(t)
            pending.append(
                f"{_source_title(t)} всё ещё недоступен"
                + (f" с {format_time(since)}" if since else "")
                + f" ({_source_reason_text(t)})"
            )
        elif comp == "stale":
            pending.append(
                f"состояние {_source_title(t)} не подтверждено "
                "(данные устарели)"
            )

    if pending:
        return "Состояние: ЧАСТИЧНОЕ ВОССТАНОВЛЕНИЕ. " + "; ".join(
            pending
        ) + "."

    if len(recovered_types) > 1:
        return "Состояние: ПОЛНОЕ ВОССТАНОВЛЕНИЕ. Оба источника доступны."

    return "Состояние: ВОССТАНОВЛЕНО. Оба источника доступны."


def _watchdog_recovery_text(items):
    """items: [(тип, длительность_сек)]"""
    if len(items) == 1 and items[0][0] not in SOURCE_FAILURE_TYPES:
        failure_type, duration = items[0]

        return MESSAGES["watchdog_recovery_body"].format(
            recovery_reason=MESSAGES["watchdog_recovery_reasons"][
                failure_type
            ],
            duration=format_duration(duration),
        )

    recovered_types = [t for t, _ in items]
    state_line = _sources_state_line(recovered_types)

    if len(items) == 1:
        failure_type, duration = items[0]
        since, _ = _watchdog_incident_snapshot(failure_type)

        return MESSAGES["watchdog_source_recovery_body"].format(
            recovery_reason=(
                MESSAGES["watchdog_recovery_reasons"][failure_type].rstrip(".")
                + (
                    f" (сбой с {format_time(since)})"
                    if since
                    else ""
                )
                + ".\n"
                + state_line
            ),
            duration=format_duration(duration),
        )

    lines = "\n".join(
        MESSAGES["watchdog_recovery_reasons"][t]
        + f" Длительность сбоя: {format_duration(d)}"
        for t, d in items
    )

    return MESSAGES["watchdog_source_recovery_multi_body"].format(
        details=lines + "\n" + state_line,
    )


# --------------------------------------------------------------
# Уведомления
# --------------------------------------------------------------

def watchdog_send_once(text):
    """
    Одна ограниченная попытка отправки в рабочий чат. Без повторов и
    без telegram_request(), состояние telegram_api_ok не трогает
    (его ведут штатные проверки). Возвращает message_id или None.
    Токен в лог не попадает. Ошибка отправки - отдельная строка
    WATCHDOG TELEGRAM FAILED, не путается с ошибками источников.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    try:
        response = session.post(
            url,
            data={"chat_id": CHAT_ID, "text": text},
            timeout=WATCHDOG_SEND_TIMEOUT,
        )

        if response.status_code != 200:
            print(
                "WATCHDOG TELEGRAM FAILED: "
                f"HTTP {response.status_code}",
                flush=True,
            )
            return None

        result = response.json()

        if not result.get("ok"):
            print(
                "WATCHDOG TELEGRAM FAILED: Bot API вернул ошибку "
                f"(error_code={result.get('error_code')})",
                flush=True,
            )
            return None

        message_id = result.get("result", {}).get("message_id")

        if not message_id:
            print(
                "WATCHDOG TELEGRAM FAILED: нет message_id в ответе",
                flush=True,
            )
            return None

        return message_id

    except Exception as e:
        kind, _label = classify_source_error(e)

        print(
            "WATCHDOG TELEGRAM FAILED: "
            f"kind={kind} {type(e).__name__}: {e}",
            flush=True,
        )
        return None


def _watchdog_close_silently(failure_type):
    """Инцидент закончился, а уведомление о сбое не доставлено."""
    info = clear_failure_state(failure_type)

    if info is None:
        return

    attempts = watchdog_send_attempts.pop(failure_type, 0)

    if attempts:
        why = (
            f"уведомление о сбое пытались отправить {attempts} раз, "
            "доставить не удалось"
        )
        close_reason = "recovered_notify_failed"
    else:
        why = (
            "порог уведомления "
            f"({FAILURE_NOTIFICATION_AFTER_SECONDS} с) не достигнут"
        )
        close_reason = "recovered_below_threshold"

    print(
        "ИНЦИДЕНТ ЗАКРЫТ БЕЗ УВЕДОМЛЕНИЯ: "
        f"{failure_type}; длительность "
        f"{format_duration(info['duration'])}; {why}",
        flush=True,
    )

    if failure_type in SOURCE_FAILURE_TYPES:
        source_incident_close(
            failure_type, close_reason, info["duration"]
        )


def watchdog_notify_failures(failure_types):
    """
    Отправляет уведомления о сбоях для тех компонентов из
    failure_types, у которых инцидент идёт не меньше порога и о нём
    ещё не сообщено. Источники (pszsu / monitor) объединяются в одно
    сообщение, parser и telegram - отдельные сообщения.
    Возвращает False, если отправка не удалась (остальные отправки
    в этом проходе нужно отложить).
    """
    now = now_utc()
    due = []

    for failure_type in failure_types:
        since, notified = _watchdog_incident_snapshot(failure_type)

        if since is None or notified:
            continue

        if (
            now - since
        ).total_seconds() < FAILURE_NOTIFICATION_AFTER_SECONDS:
            continue

        comp = _watchdog_component_state(failure_type)

        if comp != "fail":
            # Уже восстановился (или состояние не подтверждено):
            # запоздалое сообщение о сбое не отправляется, инцидент
            # закроет watchdog_notify_recoveries (молча, в лог).
            _watchdog_note_stale(failure_type, comp == "stale")
            continue

        due.append(failure_type)

    if not due:
        return True

    with state_lock:
        last_check = state["last_check"]

    sources = [t for t in due if t in SOURCE_FAILURE_TYPES]
    others = [t for t in due if t not in SOURCE_FAILURE_TYPES]

    groups = []

    if sources:
        groups.append(sources)

    groups.extend([t] for t in others)

    for group in groups:
        text = sanitize_secrets(_watchdog_failure_text(group, last_check))

        message_id = watchdog_send_once(text)

        if not message_id:
            for failure_type in group:
                watchdog_send_attempts[failure_type] = (
                    watchdog_send_attempts.get(failure_type, 0) + 1
                )

                if failure_type in SOURCE_FAILURE_TYPES:
                    source_incident_note_notify_error(failure_type)

            print(
                "УВЕДОМЛЕНИЕ О СБОЕ НЕ ОТПРАВЛЕНО (повтор в следующем "
                f"проходе): {', '.join(group)}",
                flush=True,
            )

            return False

        with state_lock:
            for failure_type in group:
                state[f"{failure_type}_failure_notified"] = True

        for failure_type in group:
            watchdog_send_attempts.pop(failure_type, None)

            if failure_type in SOURCE_FAILURE_TYPES:
                source_incident_note_notified(failure_type)

        print(
            "ОТПРАВЛЕНО УВЕДОМЛЕНИЕ О СБОЕ: "
            f"{', '.join(group)}",
            flush=True,
        )

    return True


def watchdog_notify_recoveries(failure_types):
    """
    Для компонентов из failure_types, которые снова исправны (данные
    свежие): закрывает инцидент. Если о сбое было сообщено -
    отправляет уведомление о восстановлении (источники - одним
    сообщением), инцидент закрывается только после успешной
    отправки. Если не сообщено - закрывается молча (с записью в
    лог). Компонент с устаревшим состоянием не считается
    восстановившимся. Возвращает False, если отправка не удалась.
    """
    now = now_utc()
    recovered = []

    for failure_type in failure_types:
        comp = _watchdog_component_state(failure_type)

        _watchdog_note_stale(failure_type, comp == "stale")

        if comp != "ok":
            continue

        since, notified = _watchdog_incident_snapshot(failure_type)

        if since is None:
            continue

        if not notified:
            _watchdog_close_silently(failure_type)
            continue

        recovered.append(
            (failure_type, (now - since).total_seconds())
        )

    if not recovered:
        return True

    sources = [
        item for item in recovered if item[0] in SOURCE_FAILURE_TYPES
    ]
    others = [
        item for item in recovered
        if item[0] not in SOURCE_FAILURE_TYPES
    ]

    groups = []

    if sources:
        groups.append(sources)

    groups.extend([item] for item in others)

    for group in groups:
        text = sanitize_secrets(_watchdog_recovery_text(group))

        message_id = watchdog_send_once(text)

        if not message_id:
            for failure_type, _ in group:
                if failure_type in SOURCE_FAILURE_TYPES:
                    source_incident_note_notify_error(failure_type)

            print(
                "УВЕДОМЛЕНИЕ О ВОССТАНОВЛЕНИИ НЕ ОТПРАВЛЕНО (повтор в "
                "следующем проходе): "
                f"{', '.join(t for t, _ in group)}",
                flush=True,
            )

            return False

        for failure_type, duration in group:
            clear_failure_state(failure_type)
            watchdog_send_attempts.pop(failure_type, None)

            if failure_type in SOURCE_FAILURE_TYPES:
                source_incident_close(
                    failure_type, "recovered_notified", duration
                )

        print(
            "ОТПРАВЛЕНО УВЕДОМЛЕНИЕ О ВОССТАНОВЛЕНИИ: "
            f"{', '.join(t for t, _ in group)}",
            flush=True,
        )

    return True


def watchdog_send_failure(failure_type, reason=None):
    """Совместимость: проход уведомления о сбое для одного типа."""
    watchdog_notify_failures([failure_type])


def watchdog_check_recovery(failure_type, recovery_reason=None):
    """Совместимость: проход восстановления для одного типа."""
    watchdog_notify_recoveries([failure_type])


WATCHDOG_COMPONENTS = ("parser", "pszsu", "monitor", "telegram")


def watchdog_pass():
    """Один проход контроля: свежие флаги, фиксация инцидентов,
    уведомления о сбоях и восстановлениях."""
    started_now = []

    # 1. Фиксация начала инцидентов по свежим флагам.
    for failure_type in WATCHDOG_COMPONENTS:
        comp = _watchdog_component_state(failure_type)

        _watchdog_note_stale(failure_type, comp == "stale")

        if comp != "fail":
            continue

        if failure_type in SOURCE_FAILURE_TYPES:
            reason = _source_reason_text(failure_type)
        else:
            reason = MESSAGES["watchdog_failure_reasons"][failure_type]

        if set_failure_state(failure_type, reason):
            started_now.append(failure_type)

            if failure_type in SOURCE_FAILURE_TYPES:
                # Отсчёт порога - от фактической первой неудачной
                # проверки серии, а не от момента, когда её заметил
                # watchdog (шаг прохода 10 с).
                started_at = source_incident_open(failure_type)

                with state_lock:
                    current = state[f"{failure_type}_failure_since"]

                    if (
                        current is not None
                        and started_at is not None
                        and started_at < current
                    ):
                        state[f"{failure_type}_failure_since"] = started_at

    if len(started_now) >= 2:
        print(
            "ОБЩИЙ СБОЙ ИСХОДЯЩИХ СОЕДИНЕНИЙ (подозрение, причина не "
            "установлена): одновременно начались инциденты: "
            + ", ".join(started_now)
            + " | "
            + _watchdog_context_text(),
            flush=True,
        )

    # 2. Уведомления о сбоях (после первой неудачи отправки в этом
    #    проходе остальное откладывается).
    if not watchdog_notify_failures(WATCHDOG_COMPONENTS):
        return

    # 3. Восстановления - по флагам, прочитанным заново.
    watchdog_notify_recoveries(WATCHDOG_COMPONENTS)


def watchdog_loop():
    print(
        "Запущен контроль состояния системы",
        flush=True,
    )

    while True:
        try:
            now = now_utc()

            with state_lock:
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

            watchdog_pass()

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

def is_group_admin(user_id, chat_id=None):
    """
    Администратор ли пользователь в чате chat_id.
    Без chat_id - в рабочем чате CHAT_ID (прежнее поведение).
    """
    result = telegram_request(
        "getChatMember",
        {
            "chat_id": CHAT_ID if chat_id is None else chat_id,
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
    """
    /classify [pszsu|monitor] <текст> - только для администратора.

    Разрешён в двух чатах: рабочем CHAT_ID и приватной картотеке
    ARCHIVE_CHAT_ID (если она настроена; иначе из картотеки команда
    игнорируется, без подмены на рабочий чат). Права администратора
    проверяются именно в том чате, откуда пришла команда, и ответ
    уходит в тот же чат. Команда только классифицирует текст: тревогу,
    архивную карточку, dedup и состояние она не трогает.
    """
    chat = message.get("chat", {})
    sender = message.get("from", {})
    chat_id = chat.get("id")

    allowed_chats = {CHAT_ID}

    if ARCHIVE_CHAT_ID is not None:
        allowed_chats.add(ARCHIVE_CHAT_ID)

    if chat_id not in allowed_chats:
        return

    user_id = sender.get("id")

    if not user_id:
        return

    if not is_group_admin(user_id, chat_id):
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
            "Использование: /classify [pszsu|monitor] <текст сообщения>",
            chat_id=chat_id,
        )
        return

    print(
        f"Команда /classify от администратора {user_id}.",
        flush=True,
    )

    send_telegram_message(
        build_classify_reply(body, which),
        chat_id=chat_id,
    )


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
                    map_beta_test_command(message)

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


def _pszsu_sides(phrase):
    """Компоненты сторон света во фразе: {N,S,E,W}. «северо-западный» -> {N,W}."""
    sides = set()

    for word in re.findall(r"[а-яіїєґ]+", phrase):
        if word.startswith("півн"):
            sides.add("N")
        elif word.startswith("півд"):
            sides.add("S")
        elif word.startswith(("схід", "сход", "східн")):
            sides.add("E")
        elif word.startswith("зах"):
            sides.add("W")

    return sides


_PSZSU_OPPOSITE_SIDE = {"N": "S", "S": "N", "E": "W", "W": "E"}


def pszsu_position_course_exclusion(text):
    """
    Исключение «положение + курс»: объект севернее/южнее/восточнее/западнее
    города (предложный падеж: «на півночі від Кременчука») с ЯВНЫМ курсом,
    который не направлен к городу. Курс «к городу» - любая сторона,
    противоположная положению (север -> юг, восток -> запад; для
    составных сторон проверяется каждая компонента).

    Проверяется построчно (каждая строка сообщения ПС ЗСУ - отдельный
    объект): курс из соседней строки к положению относительно Кременчука
    не относится. Каждая строка с положением должна иметь явный курс не к
    городу; если у такой строки курса нет или он направлен к городу -
    исключение не применяется и действует прежняя логика.
    """
    checked = False

    for line in text.split("\n"):
        normalized = normalize_pszsu_text(line)

        positions = set()

        for m in re.finditer(PSZSU_POSITION_LOCATIVE_REGEX, normalized):
            positions |= _pszsu_sides(m.group(1))

        if not positions:
            continue

        courses = set()

        for pattern in PSZSU_COURSE_PHRASE_REGEXES:
            for m in re.finditer(pattern, normalized):
                courses |= _pszsu_sides(m.group(1))

        if not courses:
            return False

        if any(_PSZSU_OPPOSITE_SIDE[p] in courses for p in positions):
            return False

        checked = True

    return checked


def pszsu_has_direct_alarm_basis(normalized):
    """Явное основание для тревоги: цель/курс на город или городская тревожная форма."""
    return (
        re.search(PSZSU_DIRECT_ALARM_REGEX, normalized) is not None
        or re.search(KREMENCHUK_PSZSU_CITY_FIRST_ALERT_REGEX, normalized) is not None
    )


def pszsu_cancellation_decision(normalized):
    """
    Явная отмена угрозы (PSZSU_CANCEL_REGEX) в части предложений. Предложение с
    отменой не даёт тревогу. Если в остальных предложениях нет прямого
    основания (pszsu_has_direct_alarm_basis), а город упомянут вне
    минус-конструкций - сообщение уходит в картотеку, не в ALERT.
    Возвращает (решение, правило) или None (отмены нет / прямое основание
    в другом предложении / города нет - обычная логика).
    """
    sentences = [
        part for part in re.split(r"[.!?;\n]+", normalized) if part.strip()
    ]

    cancel = [
        bool(re.search(PSZSU_CANCEL_REGEX, part)) for part in sentences
    ]

    if not any(cancel):
        return None

    for part, is_cancel in zip(sentences, cancel):
        if not is_cancel and pszsu_has_direct_alarm_basis(part):
            return None

    if not pszsu_has_kremenchuk_outside_minus(normalized):
        return None

    return "ARCHIVE_NORMAL", "archive:threat_cancelled_city"


def classify_pszsu_with_rule(text, _skip_continuing=False):
    """
    PSZSU: широкий городской триггер + согласованные исключения.
    Возвращает (решение, правило). Решение: ALERT / ARCHIVE_HIGH /
    ARCHIVE_NORMAL / IGNORE (ARCHIVE_* - внутренние значения,
    check_source() приводит их к единому ARCHIVE). Правило - короткое
    имя сработавшей ветки для диагностики.
    """
    if not _skip_continuing and is_continuing_threat(text):
        # «Триває загроза…»: повтор уже поданной тревоги. Сводка исключается
        # полностью; остальное разбираем обычной логикой (без этой ветки):
        # город без самостоятельного основания - картотека, а не потеря;
        # прямое основание (курс/цель на город) - ALERT; без города - IGNORE.
        if is_post_event_report(text):
            return "IGNORE", "ignore:continuing_threat"

        decision, rule = classify_pszsu_with_rule(text, _skip_continuing=True)

        if decision.startswith("ARCHIVE"):
            return decision, rule

        if decision == "ALERT":
            if pszsu_has_direct_alarm_basis(normalize_pszsu_text(text)):
                return "ALERT", "alert:continuing_threat_direct_basis"

            return "ARCHIVE_NORMAL", "archive:continuing_threat_city"

        return "IGNORE", "ignore:continuing_threat"

    if is_post_event_report(text):
        return "IGNORE", "ignore:post_event_report"

    normalized = normalize_pszsu_text(text)

    cancelled = pszsu_cancellation_decision(normalized)

    if cancelled is not None:
        return cancelled

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

    # Исключение «положение + курс» (локатив «на півночі від Кременчука»
    # с явным курсом не к городу). Не подавляет тревогу, если есть
    # самостоятельное основание; сообщение уходит в картотеку, не теряется.
    if pszsu_position_course_exclusion(text):
        remainder = re.sub(PSZSU_POSITION_LOCATIVE_REGEX, " ", normalized)

        if (
            pszsu_has_direct_alarm_basis(normalized)
            or pszsu_has_kremenchuk_outside_minus(remainder)
        ):
            return "ALERT", "alert:independent_basis_beside_position_course"

        return "ARCHIVE_NORMAL", "archive:position_course_not_toward_city"

    # После удаления минус-конструкций остаётся самостоятельный город — ALERT.
    if pszsu_has_kremenchuk_outside_minus(text):
        # «Кременчук та <другой город>» без самостоятельного основания
        # для тревоги — не ALERT, а ARCHIVE.
        if pszsu_city_with_other_place(text):
            if pszsu_has_independent_alarm_basis(text):
                return "ALERT", "alert:city_with_other_place_independent_basis"

            return "ARCHIVE_NORMAL", "archive:city_with_other_place"

        return "ALERT", "alert:city_outside_minus"

    # «Повз/довкола Кременчука» без самостоятельного основания: не ALERT,
    # но город упомянут - в картотеку, а не в IGNORE.
    if re.search(PSZSU_PASS_BY_CITY_REGEX, normalized):
        return "ARCHIVE_NORMAL", "archive:pass_by_city"

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
    source_diag_record_result(
        source_name,
        ok,
        _source_state_log.get(source_name),
    )

    if ok:
        if source_failure_count.get(source_name):
            source_diag_backoff(source_name, False)

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
        source_diag_backoff(source_name, True, delay)

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
    _request_started = time.monotonic()

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
                    _alert_args = dict(
                        source_name=source_name,
                        source_link=source_link,
                        post_id=post_id,
                        original_text=text,
                        classification="HIGH_SPEED_THREAT",
                    )
                    if send_alert(**_alert_args):
                        map_beta_submit(**_alert_args)
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
                _alert_args = dict(
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
                if send_alert(**_alert_args):
                    map_beta_submit(**_alert_args)
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

        source_diag_record_error(
            source_name,
            e,
            time.monotonic() - _request_started,
        )

        log_source_state(
            source_name,
            f"unavailable:exception_{type(e).__name__}",
        )

        return False


# ============================================================
# BEGIN MAP SCREENSHOT BETA
# ============================================================

import collections
#
# Бета-тест (20.10.2026): после УСПЕШНОЙ отправки тревоги в рабочую
# группу бот публикует в картотеке копию тревоги с картинкой-заглушкой
# и, получив скриншот карты от Cloudflare Worker, заменяет заглушку в
# ТОМ ЖЕ сообщении (editMessageMedia). Под фото: время получения снимка по Киеву
# и ссылка на карту.
#
# ИЗОЛЯЦИЯ:
#  - единственная точка входа для основного кода - map_beta_submit();
#    она вызывается в двух местах check_source() (PSZSU и MONITOR) только
#    после того, как send_alert() вернул True. Вся её работа в парсер-
#    потоке: проверка флага, постановка в очередь (put_nowait, без
#    ожидания), ленивый запуск двух фоновых потоков; любое исключение
#    гасится внутри;
#  - свои очередь и два потока (отправка заглушки / получение снимка и
#    замена), свои HTTP-вызовы (не telegram_request, не
#    send_telegram_message, без heartbeat и без telegram_api_ok);
#  - дедупликация: отдельное пространство ключей mapbeta:<источник>:<post_id>
#    в существующем реестре sent_messages (формат STATE_FILE не меняется);
#  - выключено по умолчанию: MAP_BETA_ENABLED=1 включает.
#
# ИЗВЕСТНЫЕ ОГРАНИЧЕНИЯ (первая версия):
#  - очередь в памяти: при перезапуске Render уже отправленная заглушка
#    остаётся без замены, события в очереди теряются; повторной
#    публикации нет (ключ дедупликации сохранён в STATE_FILE);
#  - в подписи «Снимок получен в ЧЧ:ММ:СС» (Киев): время из заголовка X-Shot-At
#    Worker'а (он фиксируется ПОСЛЕ возврата снимка из Browser Run), а если
#    заголовка нет или он недостоверен - время получения снимка ботом. Это не
#    время съёмки;
#  - Cloudflare Free: ~10 минут браузера в сутки, снимок может не
#    успеть/не получиться - тогда остаётся заглушка.
#
# УДАЛЕНИЕ: убрать этот блок, две вставки "map_beta_submit" в
# check_source() (и переменные _alert_args там же), файл
# assets/map_placeholder.jpg.

MAP_BETA_ENABLED = os.getenv("MAP_BETA_ENABLED", "").strip() == "1"

# Контрольный запуск ТОЛЬКО в картотеке без ожидания реальной тревоги:
# при MAP_BETA_SELFTEST=<метка> один раз через ~30 с после старта публикуется
# тестовое сообщение (помечено как тест, не тревога) и проходит весь настоящий
# путь: заглушка -> Worker -> замена. MAP_BETA_ENABLED для этого НЕ нужен: при
# выключенном MAP_BETA_ENABLED реальные тревоги бету по-прежнему не запускают.
# Метка запоминается в реестре (повторный запуск на той же метке не выполняется);
# метка, начинающаяся с «long», проверяет вариант длинного текста. После проверки
# переменную уберите.
MAP_BETA_SELFTEST = os.getenv("MAP_BETA_SELFTEST", "").strip()
MAP_BETA_SELFTEST_DELAY = 30

# Необязательный секрет Worker (если на Worker задан SHOT_TOKEN).
# Передаётся заголовком X-Shot-Token, в логи не попадает.
MAP_BETA_SHOT_TOKEN = os.getenv("MAP_BETA_SHOT_TOKEN", "").strip()

MAP_BETA_WORKER_URL = "https://map-shot-test.timokha1992.workers.dev/screenshot"
MAP_BETA_MAP_URL = "https://map.ukrainealarm.com/onlyMap"
MAP_BETA_PLACEHOLDER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "assets",
    "map_placeholder.jpg",
)

MAP_BETA_WAIT_SECONDS = 180            # ожидание снимка после публикации заглушки
MAP_BETA_EDIT_GRACE_SECONDS = 30       # запас на замену уже полученного снимка
MAP_BETA_CAPTION_LIMIT = 1024          # лимит подписи к фото в Telegram
# Длинный текст (подпись > 1024): по ТЗ 22.10 отдельное сообщение с полным текстом НЕ создаётся без отдельного
# согласования. False: для такой тревоги бета не выполняется (запись в лог), текст не обрезается и не делится.
# True: прежнее поведение (короткая подпись + полный текст отдельным сообщением) - включать только по решению владельца.
MAP_BETA_SPLIT_LONG_TEXT = False
MAP_BETA_QUEUE_LIMIT = 10              # ожидающие снимка; переполнение: вытесняется САМАЯ РАННЯЯ задача
MAP_BETA_EVENT_QUEUE_LIMIT = 50        # события, ждущие публикации заглушки (заглушка уходит за доли секунды)
MAP_BETA_WORKER_MIN_INTERVAL = 16      # пауза между запросами к Worker (его защита 15 с)
# Попытка получения снимка ограничена 38 с: соединение 2 с + ожидание ответа Worker 36 с. Worker сам укладывается
# в 33 с (worker-timeout), поэтому причина доходит до бота раньше, чем истекает его таймаут.
MAP_BETA_WORKER_CONNECT_TIMEOUT = 2
MAP_BETA_WORKER_READ_TIMEOUT = 36
MAP_BETA_TG_TIMEOUT = (5, 30)
# Не более 2 попыток на одно оповещение (ТЗ 22.10): серия 38 + пауза/повтор 5 + 38 = 81 с <= 92 с. Счётчик локален для
# одного задания (job): задание берётся из очереди один раз и не возвращается в неё, ключ дедупликации в sent_messages
# фиксируется до публикации заглушки, поэтому серия не сбрасывается повторным вызовом и параллельных попыток нет
# (_mb_inflight).
MAP_BETA_MAX_SHOT_ATTEMPTS = 2
# Причины сбоя, которые Worker передаёт заголовком X-Shot-Fail-Reason (только коды, без чувствительных данных).
# Всё, чего здесь нет, считается неизвестной ошибкой: причину не выдумываем.
MAP_BETA_FAIL_REASONS = {
    "prep-timeout": "таймаут подготовки карты",
    "targets-no-data": "не удалось получить данные о целях",
    "targets-subscribe-error": "ошибка подписки на данные целей",
    "render-error": "ошибка сервера или получения изображения",
    "worker-timeout": "таймаут подготовки карты (лимит Worker)",
    "bot-timeout": "нет ответа от Worker за отведённое время",
    "unknown": "неизвестная ошибка",
}
MAP_BETA_MAX_EDIT_ATTEMPTS = 3
MAP_BETA_EXPECTED_SIZE = 800
MAP_BETA_DAILY_BROWSER_MS = 540000     # 9 из 10 бесплатных минут браузера в сутки
MAP_BETA_ASSUMED_BROWSER_MS = 20000    # если Worker не сообщил время браузера

class _MbQueue:
    """
    Ограниченная очередь с приоритетом новых задач: при переполнении
    вытесняется САМАЯ РАННЯЯ ещё не взятая в работу задача (взятая в
    работу в очереди уже не числится и не прерывается).
    """

    def __init__(self, limit):
        self._limit = limit
        self._items = collections.deque()
        self._cond = threading.Condition()

    def put_latest(self, item):
        """Кладёт задачу; возвращает вытесненную (или None)."""
        evicted = None

        with self._cond:
            if len(self._items) >= self._limit:
                evicted = self._items.popleft()

            self._items.append(item)
            self._cond.notify()

        return evicted

    def get(self):
        with self._cond:
            while not self._items:
                self._cond.wait()

            return self._items.popleft()

    def get_nowait(self):
        with self._cond:
            if not self._items:
                raise queue.Empty

            return self._items.popleft()

    def qsize(self):
        with self._cond:
            return len(self._items)

    def empty(self):
        return self.qsize() == 0


_mb_lock = threading.Lock()
_mb_sender_queue = _MbQueue(MAP_BETA_EVENT_QUEUE_LIMIT)
_mb_shot_queue = _MbQueue(MAP_BETA_QUEUE_LIMIT)
_mb_threads_started = False
_mb_inflight = set()
_mb_logged_once = set()
_mb_last_worker_call = {"t": None}
_mb_budget = {"day": None, "ms": 0}

# Ручные тесты (/test в картотеке): активные запуски и кэш имени бота.
MAP_BETA_MANUAL_STALE_SECONDS = MAP_BETA_WAIT_SECONDS + 120
_mb_manual_active = {}
_mb_manual_seq = {"n": 0}
_mb_bot_username_cache = {"v": None}


def _mb_log(key, stage, message):
    print(f"MAP BETA [{stage}] key={key} {message}", flush=True)


def _mb_log_once(token, message):
    with _mb_lock:
        if token in _mb_logged_once:
            return

        _mb_logged_once.add(token)

    print(f"MAP BETA {message}", flush=True)


# Тонкие обёртки над вводом-выводом (в тестах подменяются).
def _mb_http_post(url, data, files, timeout):
    return requests.post(url, data=data, files=files, timeout=timeout)


def _mb_http_get(url, headers, timeout):
    return requests.get(url, headers=headers, timeout=timeout)


def _mb_sleep(seconds):
    time.sleep(seconds)


def _mb_monotonic():
    return time.monotonic()


def _mb_wall_ms():
    return int(time.time() * 1000)


def _mb_caption_units(html_text):
    """Длина подписи в единицах Telegram: текст после разбора разметки."""
    plain = html.unescape(re.sub(r"<[^>]+>", "", html_text))

    return len(plain.encode("utf-16-le")) // 2


def _mb_jpeg_size(data):
    """(ширина, высота) корректного JPEG или None."""
    if (
        not data
        or len(data) < 4
        or data[0] != 0xFF
        or data[1] != 0xD8
        or data[2] != 0xFF
    ):
        return None

    i = 2

    while i + 9 < len(data):
        if data[i] != 0xFF:
            i += 1
            continue

        marker = data[i + 1]

        if marker == 0xFF:
            i += 1
            continue

        if marker in (0xD8, 0x01) or 0xD0 <= marker <= 0xD7:
            i += 2
            continue

        length = (data[i + 2] << 8) | data[i + 3]

        if 0xC0 <= marker <= 0xCF and marker not in (0xC4, 0xC8, 0xCC):
            return (
                (data[i + 7] << 8) | data[i + 8],
                (data[i + 5] << 8) | data[i + 6],
            )

        i += 2 + length

    return None


def _mb_placeholder_bytes():
    """Утверждённая заглушка (assets/map_placeholder.jpg) или None."""
    try:
        with open(MAP_BETA_PLACEHOLDER_PATH, "rb") as f:
            data = f.read()

    except Exception as e:
        _mb_log_once(
            "placeholder-missing",
            f"заглушка недоступна ({type(e).__name__}); функция не работает",
        )
        return None

    if _mb_jpeg_size(data) is None:
        _mb_log_once(
            "placeholder-bad",
            "файл заглушки не является корректным JPEG; функция не работает",
        )
        return None

    return data


def _mb_tg(method, data, files=None):
    """
    Один вызов Bot API. kind: 'ok' | 'api_error' (Telegram ответил
    ошибкой, результат известен) | 'unknown' (исключение/таймаут:
    результат неизвестен, повторять отправку нельзя).
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"

    try:
        response = _mb_http_post(
            url, data, files, MAP_BETA_TG_TIMEOUT
        )

    except Exception as e:
        return {
            "kind": "unknown",
            "status": None,
            "description": sanitize_secrets(
                f"{type(e).__name__}"
            )[:120],
            "retry_after": None,
            "result": None,
        }

    try:
        body = response.json()

    except Exception:
        body = {}

    if response.status_code == 200 and body.get("ok"):
        return {
            "kind": "ok",
            "status": 200,
            "description": "",
            "retry_after": None,
            "result": body.get("result"),
        }

    parameters = body.get("parameters") or {}

    return {
        "kind": "api_error",
        "status": response.status_code,
        "description": sanitize_secrets(
            str(body.get("description") or "")
        )[:200],
        "retry_after": parameters.get("retry_after"),
        "result": None,
    }


def _mb_budget_ok():
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with _mb_lock:
        if _mb_budget["day"] != day:
            _mb_budget["day"] = day
            _mb_budget["ms"] = 0

        return _mb_budget["ms"] < MAP_BETA_DAILY_BROWSER_MS


def _mb_budget_add(ms):
    with _mb_lock:
        _mb_budget["ms"] += max(0, int(ms))


def _mb_header_int(headers, name):
    try:
        value = headers.get(name)

        if value is None:
            return None

        value = str(value).strip()

        return int(value) if value.isdigit() else None

    except Exception:
        return None


def _mb_send_placeholder(key, caption, placeholder):
    data = {
        "chat_id": ARCHIVE_CHAT_ID,
        "caption": caption,
        "parse_mode": "HTML",
        # одно сообщение: текст сверху, изображение карты снизу
        "show_caption_above_media": "true",
    }
    files = {
        "photo": ("map_placeholder.jpg", placeholder, "image/jpeg"),
    }

    for attempt in (1, 2):
        r = _mb_tg("sendPhoto", data, files)

        if r["kind"] == "ok":
            try:
                return r["result"]["message_id"]

            except Exception:
                _mb_log(key, "заглушка", "ответ без message_id")
                return None

        if r["kind"] == "unknown":
            # Результат неизвестен: повтор мог бы создать дубль.
            _mb_log(
                key,
                "заглушка",
                f"результат отправки неизвестен ({r['description']}), "
                "повтор не выполняется",
            )
            return None

        delay = (
            int(r["retry_after"] or 5)
            if r["status"] == 429
            else 3
        )
        retriable = (
            r["status"] == 429 or (r["status"] or 0) >= 500
        ) and delay <= 30

        if attempt == 1 and retriable:
            _mb_log(
                key,
                "заглушка",
                f"HTTP {r['status']}: повтор через {delay} с",
            )
            _mb_sleep(delay)
            continue

        _mb_log(
            key,
            "заглушка",
            f"не отправлена: HTTP {r['status']} {r['description']}",
        )
        return None

    return None


def _mb_send_full_text(key, alert_text, photo_message_id):
    """Длинный текст: полный текст тревоги отдельным сообщением под фото."""
    for attempt in (1, 2, 3):
        r = _mb_tg(
            "sendMessage",
            {
                "chat_id": ARCHIVE_CHAT_ID,
                "text": alert_text,
                "parse_mode": "HTML",
                "reply_to_message_id": photo_message_id,
                "link_preview_options": json.dumps(
                    {"is_disabled": True}
                ),
            },
        )

        if r["kind"] == "ok":
            return True

        if r["kind"] == "unknown":
            _mb_log(
                key,
                "текст",
                "результат отправки полного текста неизвестен, повтор не "
                "выполняется",
            )
            return False

        wait = (
            int(r["retry_after"] or 5)
            if r["status"] == 429
            else 2 * attempt
        )

        if attempt < 3 and (
            r["status"] == 429 or (r["status"] or 0) >= 500
        ) and wait <= 60:
            _mb_sleep(wait)
            continue

        break

    _mb_log(key, "текст", "полный текст не отправлен")
    return False


def _mb_process_send(event):
    """Поток-отправитель: заглушка (и полный текст) сразу после тревоги."""
    key = event["key"]

    try:
        # Ручной тест (/test) имеет уникальный ключ на каждый запуск и НЕ
        # пишет в реестр обработанных сообщений (состояние не меняется).
        duplicate = False

        if not event.get("manual"):
            with sent_messages_lock:
                duplicate = key in sent_messages

        if duplicate:
            _mb_log(key, "дубль", "событие уже обработано, пропуск")
            return

        placeholder = _mb_placeholder_bytes()

        if placeholder is None:
            _mb_manual_done(event, False, "файл заглушки недоступен (см. лог)")
            return

        alert_text = event.get("alert_text") or build_alert_text(
            source_name=event["source_name"],
            source_link=event["source_link"],
            post_link=build_source_post_link(
                source_link=event["source_link"],
                post_id=event["post_id"],
            ),
            original_text=event["original_text"],
            classification=event["classification"],
        )

        sample_tail = (
            "\n\nСнимок получен в 00:00:00\nКарта: " + MAP_BETA_MAP_URL
        )
        long_mode = (
            _mb_caption_units(alert_text + sample_tail)
            > MAP_BETA_CAPTION_LIMIT
        )
        base_caption = (
            "🗺 Полный текст оповещения - в сообщении ниже"
            if long_mode
            else alert_text
        )

        if long_mode and not MAP_BETA_SPLIT_LONG_TEXT:
            # Текст не помещается в подпись (1024), а второе сообщение запрещено ТЗ:
            # не обрезаем и не делим - бета для этого оповещения не выполняется.
            _mb_log(
                key,
                "длинный текст",
                f"подпись {_mb_caption_units(alert_text + sample_tail)} > "
                f"{MAP_BETA_CAPTION_LIMIT}: копия с картой не создаётся "
                "(без второго сообщения и без обрезки; нужно решение владельца)",
            )
            _mb_manual_done(event, False, "текст длиннее подписи (1024), второе сообщение запрещено")
            return

        # Ключ фиксируется ДО отправки: любой дальнейший исход не
        # приводит к повторной публикации этого события.
        if not event.get("manual"):
            register_sent(key)

        message_id = _mb_send_placeholder(key, base_caption, placeholder)

        if not message_id:
            _mb_manual_done(event, False, "заглушку не удалось отправить в Картотеку")
            return

        sent_at = _mb_monotonic()

        _mb_log(
            key,
            "заглушка",
            f"опубликована message_id={message_id}"
            + (" (длинный текст)" if long_mode else ""),
        )

        if long_mode:
            _mb_send_full_text(key, alert_text, message_id)

        job = {
            "key": key,
            "message_id": message_id,
            "base_caption": base_caption,
            "deadline": sent_at + MAP_BETA_WAIT_SECONDS,
            "manual": event.get("manual"),
            "run_id": event.get("run_id"),
            "reply_to": event.get("reply_to"),
        }

        evicted = _mb_shot_queue.put_latest(job)

        if evicted is not None:
            _mb_log(
                evicted["key"],
                "очередь",
                "вытеснена более новой тревогой (очередь снимков полна); "
                "снимок не запрашивался, заглушка остаётся",
            )
            _mb_manual_done(
                evicted, False,
                "тест вытеснен более новой задачей (очередь снимков полна); заглушка осталась",
            )

    except Exception as e:
        _mb_log(
            key,
            "ошибка",
            sanitize_secrets(f"{type(e).__name__}: {e}"),
        )
        _mb_manual_done(event, False, "внутренняя ошибка (см. лог)")

    finally:
        with _mb_lock:
            _mb_inflight.discard(key)


def _mb_worker_fail_code(headers):
    """Код причины из X-Shot-Fail-Reason; неизвестное или отсутствующее значение -> unknown."""
    try:
        value = str(headers.get("x-shot-fail-reason") or "").strip().lower()
    except Exception:
        value = ""

    return value if value in MAP_BETA_FAIL_REASONS and value != "bot-timeout" else "unknown"


def _mb_fetch_shot(job):
    """Снимок от Worker в пределах срока. dict или None."""
    key = job["key"]
    headers = {"accept": "image/jpeg"}

    if MAP_BETA_SHOT_TOKEN:
        headers["X-Shot-Token"] = MAP_BETA_SHOT_TOKEN

    attempts = 0
    reason = "нет попыток"
    fail_code = "unknown"

    while attempts < MAP_BETA_MAX_SHOT_ATTEMPTS:
        remaining = job["deadline"] - _mb_monotonic()

        if remaining < 15:
            reason = "срок ожидания исчерпан"
            break

        if not _mb_budget_ok():
            reason = "суточный лимит времени браузера (Free) исчерпан"
            break

        with _mb_lock:
            last_call = _mb_last_worker_call["t"]

        if last_call is not None:
            pause = MAP_BETA_WORKER_MIN_INTERVAL - (
                _mb_monotonic() - last_call
            )

            if pause > 0:
                if pause >= remaining - 15:
                    reason = "пауза между запросами к Worker не укладывается в срок"
                    break

                _mb_sleep(pause)

        with _mb_lock:
            _mb_last_worker_call["t"] = _mb_monotonic()

        attempts += 1
        remaining = job["deadline"] - _mb_monotonic()
        started_ms = _mb_wall_ms()

        try:
            response = _mb_http_get(
                MAP_BETA_WORKER_URL,
                headers,
                (
                    MAP_BETA_WORKER_CONNECT_TIMEOUT,
                    max(5, min(MAP_BETA_WORKER_READ_TIMEOUT, remaining - 2)),
                ),
            )

        except Exception as e:
            reason = sanitize_secrets(f"{type(e).__name__}")
            fail_code = "bot-timeout" if "Timeout" in type(e).__name__ else "unknown"
            _mb_log(key, "снимок", f"попытка {attempts}: {reason}")
            backoff = min(5 * attempts, 20)

            if job["deadline"] - _mb_monotonic() - backoff < 15:
                break

            _mb_sleep(backoff)
            continue

        received_ms = _mb_wall_ms()
        status = response.status_code
        browser_ms = _mb_header_int(
            response.headers, "x-browser-ms-used"
        )

        if status == 200:
            body = response.content
            size = _mb_jpeg_size(body)

            if browser_ms is not None:
                _mb_budget_add(browser_ms)
            else:
                _mb_budget_add(MAP_BETA_ASSUMED_BROWSER_MS)

            if size == (MAP_BETA_EXPECTED_SIZE, MAP_BETA_EXPECTED_SIZE):
                shot_at = _mb_header_int(response.headers, "x-shot-at")

                # Время снимка должно лежать в окне запроса.
                if shot_at is not None and not (
                    started_ms - 5000 <= shot_at <= received_ms + 5000
                ):
                    _mb_log(
                        key,
                        "снимок",
                        f"X-Shot-At={shot_at} вне окна запроса, игнорируется",
                    )
                    shot_at = None

                return {
                    "bytes": body,
                    "shot_at_ms": shot_at,
                    "received_ms": received_ms,
                    "attempts": attempts,
                }

            fail_code = "render-error"
            reason = (
                "ответ не является JPEG 800x800 "
                f"(размер: {size if size else 'не JPEG'})"
            )
            _mb_log(key, "снимок", f"попытка {attempts}: {reason}")

        elif status == 429:
            retry_after = _mb_header_int(response.headers, "retry-after")
            delay = retry_after if retry_after else 15
            reason = f"HTTP 429 (Retry-After {delay} с)"
            _mb_log(key, "снимок", f"попытка {attempts}: {reason}")

            if job["deadline"] - _mb_monotonic() - delay < 15:
                break

            _mb_sleep(delay)
            continue

        elif status in (401, 403, 404):
            # Ошибка конфигурации: повторять бессмысленно.
            reason = f"HTTP {status} (проверьте URL/токен Worker)"
            _mb_log(key, "снимок", f"попытка {attempts}: {reason}")
            break

        else:
            if browser_ms is not None:
                _mb_budget_add(browser_ms)

            fail_code = _mb_worker_fail_code(response.headers)
            reason = f"HTTP {status}; причина Worker: {MAP_BETA_FAIL_REASONS[fail_code]}"
            _mb_log(key, "снимок", f"попытка {attempts}: {reason}")

        backoff = min(5 * attempts, 20)

        if job["deadline"] - _mb_monotonic() - backoff < 15:
            break

        _mb_sleep(backoff)

    job["fail_reason"] = reason
    job["fail_code"] = fail_code
    _mb_log(key, "снимок", f"не получен: {reason}; заглушка остаётся")
    return None


def _mb_time_line(shot):
    """
    Единый формат подписи: «Снимок получен в ЧЧ:ММ:СС» (по Киеву). Это НЕ
    время съёмки: X-Shot-At Worker фиксирует после возврата готового
    изображения из Browser Run. Берётся X-Shot-At, если он есть и достоверен
    (лежит в окне запроса), иначе время получения снимка ботом.
    """
    stamp_ms = shot["shot_at_ms"]

    if stamp_ms is None:
        stamp_ms = shot["received_ms"]

    moment = datetime.fromtimestamp(stamp_ms / 1000, tz=KYIV_TZ)

    return "Снимок получен в " + moment.strftime("%H:%M:%S")


def _mb_edit(job, shot):
    """Замена заглушки снимком в том же сообщении (editMessageMedia)."""
    key = job["key"]
    caption = (
        job["base_caption"]
        + "\n\n"
        + _mb_time_line(shot)
        + "\nКарта: "
        + MAP_BETA_MAP_URL
    )
    media = json.dumps(
        {
            "type": "photo",
            "media": "attach://shot",
            "caption": caption,
            "parse_mode": "HTML",
            "show_caption_above_media": True,
        },
        ensure_ascii=False,
    )
    hard_stop = job["deadline"] + MAP_BETA_EDIT_GRACE_SECONDS
    reason = "нет попыток"

    for attempt in range(1, MAP_BETA_MAX_EDIT_ATTEMPTS + 1):
        if attempt > 1 and _mb_monotonic() >= hard_stop:
            reason = "срок замены исчерпан"
            break

        r = _mb_tg(
            "editMessageMedia",
            {
                "chat_id": ARCHIVE_CHAT_ID,
                "message_id": job["message_id"],
                "media": media,
            },
            {"shot": ("map.jpg", shot["bytes"], "image/jpeg")},
        )

        if r["kind"] == "ok":
            res = r["result"]

            # Если Telegram вернул сообщение, в нём должно быть фото: иначе
            # замену нельзя считать выполненной (успех не заявляется).
            if isinstance(res, dict) and res.get("message_id") and not res.get("photo"):
                reason = "Telegram вернул сообщение без фото"
                break

            job["time_line"] = _mb_time_line(shot)
            _mb_log(
                key,
                "замена",
                f"выполнена message_id={job['message_id']}; "
                f"{_mb_time_line(shot)}",
            )
            return True

        reason = (
            f"HTTP {r['status']} {r['description']}"
            if r["kind"] == "api_error"
            else f"результат неизвестен ({r['description']})"
        )

        if (
            r["kind"] == "api_error"
            and "message is not modified" in r["description"].lower()
        ):
            job["time_line"] = _mb_time_line(shot)
            _mb_log(key, "замена", "уже выполнена (message is not modified)")
            return True

        if r["kind"] == "api_error" and r["status"] == 429:
            delay = int(r["retry_after"] or 5)

        elif r["kind"] == "unknown" or (r["status"] or 0) >= 500:
            # Правка идемпотентна: повтор безопасен (дубля не будет).
            delay = 2 * attempt

        else:
            break

        if _mb_monotonic() + delay >= hard_stop:
            reason += "; срок замены исчерпан"
            break

        _mb_log(key, "замена", f"попытка {attempt}: {reason}; повтор через {delay} с")
        _mb_sleep(delay)

    job["fail_reason"] = reason
    _mb_log(key, "замена", f"не выполнена: {reason}; заглушка остаётся")
    return False


def _mb_process_shot(job):
    """Поток-снимок: получить снимок и заменить заглушку."""
    key = job["key"]

    try:
        shot = _mb_fetch_shot(job)

        if shot is None:
            _mb_manual_done(job, False, "снимок не получен: " + str(job.get("fail_reason") or "причина не определена"))
            return

        ok = _mb_edit(job, shot)

        if ok:
            _mb_manual_done(job, True, job.get("time_line") or "")

        else:
            _mb_manual_done(job, False, "замена не выполнена: " + str(job.get("fail_reason") or "причина не определена"))

    except Exception as e:
        _mb_log(
            key,
            "ошибка",
            sanitize_secrets(f"{type(e).__name__}: {e}"),
        )
        _mb_manual_done(job, False, "внутренняя ошибка (см. лог)")


def _mb_sender_loop():
    while True:
        event = _mb_sender_queue.get()

        try:
            _mb_process_send(event)

        except Exception:
            pass


def _mb_shot_loop():
    while True:
        job = _mb_shot_queue.get()

        try:
            _mb_process_shot(job)

        except Exception:
            pass


def _mb_ensure_threads():
    global _mb_threads_started

    with _mb_lock:
        if _mb_threads_started:
            return

        _mb_threads_started = True

    threading.Thread(
        target=_mb_sender_loop,
        name="map-beta-sender",
        daemon=True,
    ).start()
    threading.Thread(
        target=_mb_shot_loop,
        name="map-beta-shot",
        daemon=True,
    ).start()


def _mb_enqueue(event):
    """Постановка события на публикацию заглушки (общая для тревог и теста)."""
    key = event["key"]

    with _mb_lock:
        if key in _mb_inflight:
            return

        _mb_inflight.add(key)

    _mb_ensure_threads()

    evicted = _mb_sender_queue.put_latest(event)

    if evicted is not None:
        with _mb_lock:
            _mb_inflight.discard(evicted["key"])

        _mb_log(
            evicted["key"],
            "очередь",
            "очередь событий полна: самое раннее вытеснено до публикации "
            "заглушки (копия в картотеке не создана)",
        )
        _mb_manual_done(evicted, False, "тест вытеснен из очереди событий до публикации заглушки")


def map_beta_submit(
    source_name,
    source_link,
    post_id,
    original_text,
    classification,
):
    """
    Единственная точка входа. Вызывается после успешной send_alert().
    Не блокирует, не бросает исключений.
    """
    try:
        if not MAP_BETA_ENABLED:
            return

        if ARCHIVE_CHAT_ID is None:
            _mb_log_once(
                "no-archive",
                "ARCHIVE_CHAT_ID не настроен: бета-функция не работает",
            )
            return

        _mb_enqueue(
            {
                "key": f"mapbeta:{source_name}:{post_id}",
                "source_name": source_name,
                "source_link": source_link,
                "post_id": post_id,
                "original_text": original_text,
                "classification": classification,
            }
        )

    except Exception as e:
        try:
            print(
                "MAP BETA [ошибка] map_beta_submit: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

        except Exception:
            pass


def _mb_manual_done(obj, ok, detail=""):
    """
    Итог ручного теста (/test): одно сообщение в Картотеку ответом на тест.
    Успех сообщается ТОЛЬКО если замена изображения подтверждена Telegram.
    Для остальных событий ничего не делает. Не бросает исключений.
    """
    try:
        if not obj.get("manual"):
            return

        run_id = obj.get("run_id")

        with _mb_lock:
            if _mb_manual_active.pop(run_id, None) is None:
                return  # уже завершён

        if ok:
            text = (
                "✅ Тест скриншота пройден: заглушка заменена снимком карты "
                "в этом сообщении."
                + (f"\n{detail}" if detail else "")
            )
        else:
            text = (
                "⚠️ Тест скриншота НЕ пройден: "
                + sanitize_secrets(str(detail))[:300]
                + "\nЗаглушка осталась в сообщении."
            )

        _mb_log(f"mapbeta:TEST:{run_id}", "test", "итог: " + ("успех" if ok else "неудача: " + str(detail)[:120]))

        data = {"chat_id": ARCHIVE_CHAT_ID, "text": text}
        reply_to = obj.get("message_id") or obj.get("reply_to")

        if reply_to:
            data["reply_to_message_id"] = reply_to
            data["allow_sending_without_reply"] = "true"

        _mb_tg("sendMessage", data)

    except Exception as e:
        try:
            print(f"MAP BETA [test] ошибка отчёта: {type(e).__name__}: {e}", flush=True)

        except Exception:
            pass


def _mb_bot_username():
    """Имя бота (getMe), кэшируется. None, если определить не удалось."""
    with _mb_lock:
        cached = _mb_bot_username_cache["v"]

    if cached:
        return cached

    r = _mb_tg("getMe", {})

    if r["kind"] == "ok":
        try:
            name = str(r["result"]["username"]).strip()

        except Exception:
            name = ""

        if name:
            with _mb_lock:
                _mb_bot_username_cache["v"] = name

            return name

    return None


def _mb_is_test_command(text):
    """/test или /test@<имя этого бота>; всё остальное (в т.ч. /test@чужой_бот, /testing) - нет."""
    parts = str(text or "").strip().split()

    if not parts:
        return False

    first = parts[0].lower()

    if first == "/test":
        return True

    if first.startswith("/test@"):
        mine = _mb_bot_username()

        if mine is None:
            _mb_log_once(
                "getme-failed",
                "[test] не удалось определить имя бота (getMe): команда /test@… проигнорирована",
            )
            return False

        return first[len("/test@"):] == mine.lower()

    return False


def map_beta_test_command(message):
    """
    Команда /test в чате «Картотека» (ARCHIVE_CHAT_ID): ручной запуск того же
    конвейера, что и у бета-функции (заглушка -> Worker -> замена в том же
    сообщении). Не зависит от MAP_BETA_ENABLED и MAP_BETA_SELFTEST. Только
    администратор чата (существующий is_group_admin, как у /classify). Ничего не
    отправляет в рабочий чат и не пишет в реестр/состояние. Существующий /test
    в рабочем чате (handle_test_command) не затрагивается. Не бросает исключений.
    """
    try:
        if ARCHIVE_CHAT_ID is None:
            return

        chat_id = (message.get("chat") or {}).get("id")

        if chat_id != ARCHIVE_CHAT_ID:
            return

        if not _mb_is_test_command(message.get("text")):
            return

        user_id = (message.get("from") or {}).get("id")

        if not user_id:
            return

        if not is_group_admin(user_id, chat_id):
            _mb_log("mapbeta:TEST", "test", f"/test отклонена: пользователь {user_id} не администратор")
            return

        now = _mb_monotonic()
        busy = False

        with _mb_lock:
            for rid in [r for r, t in _mb_manual_active.items() if now - t > MAP_BETA_MANUAL_STALE_SECONDS]:
                _mb_manual_active.pop(rid, None)   # зависший запуск не блокирует навсегда

            if _mb_manual_active:
                busy = True

            else:
                _mb_manual_seq["n"] += 1
                run_id = f"{secrets.token_hex(3)}-{_mb_manual_seq['n']}"
                _mb_manual_active[run_id] = now

        if busy:
            _mb_tg(
                "sendMessage",
                {
                    "chat_id": ARCHIVE_CHAT_ID,
                    "text": "⏳ Тест скриншота уже выполняется, дождитесь его результата.",
                    "reply_to_message_id": message.get("message_id"),
                    "allow_sending_without_reply": "true",
                },
            )
            return

        _mb_log(f"mapbeta:TEST:{run_id}", "test", f"запуск по команде администратора {user_id}")

        _mb_enqueue(
            {
                "key": f"mapbeta:TEST:{run_id}",
                "manual": True,
                "run_id": run_id,
                "reply_to": message.get("message_id"),
                "source_name": "TEST",
                "source_link": "",
                "post_id": run_id,
                "original_text": "",
                "classification": "TEST",
                "alert_text": (
                    "🧪 ТЕСТ СКРИНШОТА КАРТЫ\n"
                    "Это тест, не реальная тревога.\n"
                    f"Запуск: {run_id}"
                ),
            }
        )

    except Exception as e:
        try:
            print(f"MAP BETA [test] ошибка команды: {type(e).__name__}: {e}", flush=True)

        except Exception:
            pass


def _mb_selftest_text(label):
    head = (
        "🧪 ТЕСТ бета-функции скриншотов карты\n"
        "\n"
        "Это контрольная проверка в картотеке, а не тревога.\n"
        f"Метка запуска: {html.escape(label, quote=False)}"
    )

    if label.lower().startswith("long"):
        filler = "\n".join(
            f"Строка заполнения {i:02d}: проверка длинного текста "
            "оповещения и переноса полного текста отдельным сообщением."
            for i in range(1, 15)
        )
        return head + "\n\n" + filler

    return head


def _mb_selftest_run():
    """Один контрольный запуск в картотеке (см. MAP_BETA_SELFTEST)."""
    try:
        time.sleep(MAP_BETA_SELFTEST_DELAY)

        if ARCHIVE_CHAT_ID is None:
            _mb_log_once(
                "selftest-no-archive",
                "[selftest] ARCHIVE_CHAT_ID не настроен: контрольный запуск "
                "невозможен",
            )
            return

        label = MAP_BETA_SELFTEST
        key = f"mapbeta:SELFTEST:{label}"

        with sent_messages_lock:
            already = key in sent_messages

        if already:
            _mb_log(
                key,
                "selftest",
                "метка уже использована (повторный запуск не выполняется); "
                "уберите MAP_BETA_SELFTEST или смените метку",
            )
            return

        _mb_log(
            key,
            "selftest",
            "контрольный запуск в картотеке (реальные тревоги бету "
            + ("ЗАПУСКАЮТ" if MAP_BETA_ENABLED else "не запускают: MAP_BETA_ENABLED выключен")
            + ")",
        )

        _mb_enqueue(
            {
                "key": key,
                "source_name": "SELFTEST",
                "source_link": "",
                "post_id": label,
                "original_text": "",
                "classification": "SELFTEST",
                "alert_text": _mb_selftest_text(label),
            }
        )

    except Exception as e:
        print(
            "MAP BETA [selftest] ошибка: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )


if MAP_BETA_SELFTEST:
    threading.Thread(
        target=_mb_selftest_run,
        name="map-beta-selftest",
        daemon=True,
    ).start()


# ============================================================
# END MAP SCREENSHOT BETA
# ============================================================


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
