"""
map_snapshot.py - снимки официальной карты воздушных тревог для Lukas Alarm (отдельный модуль).

ЧТО ДЕЛАЕТ
  1. Автоматически: тревога в ОСНОВНУЮ группу уходит не текстом, а тем же сообщением в виде фото-заглушки (текст тревоги сверху,
     карта снизу). Затем заглушка заменяется снимком официальной карты В ТОМ ЖЕ сообщении (снимок получает Cloudflare Worker
     map-shot-test). Под фото: «Снимок получен через N сек» (от публикации исходного поста до метки готовности карты) и ссылка
     на карту. Копий в картотеку нет. Если фото-вариант невозможен (модуль выключен, нет файла заглушки, текст не влезает в
     подпись 1024, Telegram отклонил фото), send_alert() отправляет обычный текст, как раньше: тревога не теряется.
  2. Команда /test в картотеке (администратор): тот же конвейер для проверки (результат - в картотеке).
  3. Команды /obzor и /обзор (администратор, рабочая группа или картотека): один актуальный снимок без тревоги, результат
     публикуется только в группе, откуда вызвана команда. Не-администратор: снимок не запускается, его команда удаляется.

КАК ПОДКЛЮЧЁН К БОТУ (минимальные точки интеграции в bot.py)
  - bot.py вызывает map_snapshot.configure(...) после определения своих функций;
  - send_alert(): вместо send_telegram_message() сначала вызывается map_snapshot.map_alert_send(...) (None - значит, шлём текст);
  - check_source(): в два существующих вызова send_alert() добавлен published_at=post_datetime;
  - telegram_command_listener(): map_snapshot.map_beta_test_command(message) и map_snapshot.map_obzor_command(message);
  - модуль не создаёт ни второго бота, ни второго цикла getUpdates, ни своей конфигурации: всё берётся из модуля bot через _h().
  - единая точка отключения: MAP_SNAPSHOT_ENABLED = False в bot.py (модуль тогда не импортируется, все вызовы - заглушки,
    тревоги уходят текстом).

ИЗОЛЯЦИЯ
  - свои очередь и два потока (отправка заглушки / получение снимка и замена), свои HTTP-вызовы (не telegram_request,
    не send_telegram_message, без heartbeat и без telegram_api_ok); любое исключение гасится внутри;
  - дедупликация автоматических оповещений - существующий реестр send_alert() (ключ <источник>:<post_id>): один пост даёт одно
    сообщение, повторная обработка или перезапуск с сохранённым состоянием второго не создают; повторные попытки получения
    снимка меняют то же сообщение (editMessageMedia) и не создают новых;
  - циклических импортов нет: модуль импортирует только стандартную библиотеку и requests.

ИЗВЕСТНЫЕ ОГРАНИЧЕНИЯ
  - очередь в памяти: при перезапуске Render событие в очереди теряется, уже отправленная заглушка остаётся без замены;
  - Cloudflare Free: ~10 минут браузера в сутки, снимок может не получиться (тогда причина показана вместо времени);
  - состояние STATE_DIR в /tmp не переживает пересоздание контейнера: после него реестр send_alert() пуст, и пост моложе 5 минут
    может быть отправлен повторно (так было и до снимков; причину дублей владельца это не подтверждено).
"""

import collections
import html
import json
import os
import queue
import re
import secrets
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

KYIV_TZ = ZoneInfo("Europe/Kyiv")

_host = None


def _h():
    """Модуль bot (подключается configure()). Все обращения к объектам бота идут только через него."""
    if _host is None:
        raise RuntimeError("map_snapshot не подключён к боту: configure(host) не вызван")

    return _host


def configure(host):
    """Подключение к боту: host - модуль bot. Вызывается один раз из bot.py. Не бросает исключений."""
    global _host

    try:
        _host = host

        if MAP_BETA_SELFTEST and not _mb_selftest_started["v"]:
            _mb_selftest_started["v"] = True
            threading.Thread(
                target=_mb_selftest_run,
                name="map-beta-selftest",
                daemon=True,
            ).start()

    except Exception as e:
        try:
            print(f"MAP SNAPSHOT [ошибка] configure: {type(e).__name__}: {e}", flush=True)

        except Exception:
            pass


_mb_selftest_started = {"v": False}

MAP_BETA_ENABLED = True  # единая точка отключения - MAP_SNAPSHOT_ENABLED в bot.py

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
# Заглушка окончательной ошибки («ЗНІМОК ОТРИМАТИ НЕ ВДАЛОСЬ»): подставляется вместо заглушки ожидания после
# исчерпания попыток. Если файла нет, меняется только подпись (editMessageCaption), заглушка ожидания остаётся.
MAP_BETA_ERROR_PLACEHOLDER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "assets",
    "map_error_placeholder.jpg",
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
    "deadline": "истёк срок ожидания снимка",
    "budget": "исчерпан суточный лимит времени браузера",
    "rate-limit": "сервис снимков перегружен (лимит запросов)",
    "access": "нет доступа к сервису снимков (проверьте настройки)",
    "evicted": "задание вытеснено более новой тревогой",
    "unknown": "неизвестная ошибка",
}
MAP_BETA_MAX_EDIT_ATTEMPTS = 3
# Время «от публикации до снимка»: допуски проверки метки страницы (мс).
MAP_BETA_MARKER_WINDOW_MS = 5000       # метка должна лежать в окне запроса бота +- это значение
MAP_BETA_CLOCK_SKEW_MAX_MS = 5000      # |X-Shot-Clock-Skew-Ms| больше - расхождение часов (включает задержку доставки отчёта)
MAP_BETA_NEG_TOLERANCE_MS = 2000       # допуск отрицательного интервала (метка Telegram с точностью до секунды)
MAP_BETA_MAX_INTERVAL_MS = 600000      # интервал больше 10 мин считается недостоверным
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


def _mb_err_text(e):
    """Текст исключения без секретов; работает и без подключённого бота."""
    text = f"{type(e).__name__}: {e}"

    try:
        return _h().sanitize_secrets(text)

    except Exception:
        return text


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


def _mb_error_placeholder_bytes():
    """Заглушка окончательной ошибки (assets/map_error_placeholder.jpg) или None."""
    try:
        with open(MAP_BETA_ERROR_PLACEHOLDER_PATH, "rb") as f:
            data = f.read()

    except Exception as e:
        _mb_log_once(
            "error-placeholder-missing",
            f"заглушка ошибки недоступна ({type(e).__name__}): при сбое меняется только подпись",
        )
        return None

    if _mb_jpeg_size(data) is None:
        _mb_log_once(
            "error-placeholder-bad",
            "файл заглушки ошибки не является корректным JPEG: при сбое меняется только подпись",
        )
        return None

    return data


def _mb_failure_caption(job):
    """
    Подпись при окончательном сбое: текст оповещения сверху, затем «⚠️ Снимок не получен: <причина>» ВМЕСТО строки
    «Снимок получен в ЧЧ:ММ:СС». Причина берётся из подтверждённого кода (таблица MAP_BETA_FAIL_REASONS); неизвестно ->
    «неизвестная ошибка». Укладывается в лимит подписи: при нехватке места сначала убирается строка со ссылкой на карту,
    затем текст причины (текст оповещения не обрезается).
    """
    code = job.get("fail_code") or "unknown"
    reason = html.escape(MAP_BETA_FAIL_REASONS.get(code, MAP_BETA_FAIL_REASONS["unknown"]))
    base = job["base_caption"]
    link = "\nКарта: " + MAP_BETA_MAP_URL
    candidates = (
        base + "\n\n⚠️ Снимок не получен: " + reason + link,
        base + "\n\n⚠️ Снимок не получен: " + reason,
        base + "\n\n⚠️ Снимок не получен",
    )

    for text in candidates:
        if _mb_caption_units(text) <= MAP_BETA_CAPTION_LIMIT:
            return text

    return candidates[-1]


def _mb_tg(method, data, files=None):
    """
    Один вызов Bot API. kind: 'ok' | 'api_error' (Telegram ответил
    ошибкой, результат известен) | 'unknown' (исключение/таймаут:
    результат неизвестен, повторять отправку нельзя).
    """
    url = f"https://api.telegram.org/bot{_h().TELEGRAM_TOKEN}/{method}"

    try:
        response = _mb_http_post(
            url, data, files, MAP_BETA_TG_TIMEOUT
        )

    except Exception as e:
        return {
            "kind": "unknown",
            "status": None,
            "description": _h().sanitize_secrets(
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
        "description": _h().sanitize_secrets(
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


def _mb_send_placeholder(key, caption, placeholder, chat_id=None):
    data = {
        "chat_id": chat_id if chat_id is not None else _h().ARCHIVE_CHAT_ID,
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
                "chat_id": _h().ARCHIVE_CHAT_ID,
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
            with _h().sent_messages_lock:
                duplicate = key in _h().sent_messages

        if duplicate:
            _mb_log(key, "дубль", "событие уже обработано, пропуск")
            return

        placeholder = _mb_placeholder_bytes()

        if placeholder is None:
            _mb_manual_done(event, False, "файл заглушки недоступен (см. лог)")
            return

        alert_text = event.get("alert_text") or _h().build_alert_text(
            source_name=event["source_name"],
            source_link=event["source_link"],
            post_link=_h().build_source_post_link(
                source_link=event["source_link"],
                post_id=event["post_id"],
            ),
            original_text=event["original_text"],
            classification=event["classification"],
        )

        sample_tail = (
            "\n\n" + _MB_TIME_LINE_LONGEST + "\nКарта: " + MAP_BETA_MAP_URL
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
            _h().register_sent(key)

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
            "published_ms": event.get("published_ms"),
        }

        evicted = _mb_shot_queue.put_latest(job)

        if evicted is not None:
            _mb_log(
                evicted["key"],
                "очередь",
                "вытеснена более новой тревогой (очередь снимков полна); "
                "снимок не запрашивался, заглушка ожидания заменяется заглушкой ошибки",
            )
            evicted["fail_code"] = "evicted"
            _mb_edit(evicted, None, failure=True)
            _mb_manual_done(
                evicted, False,
                "тест вытеснен более новой задачей (очередь снимков полна)",
            )

    except Exception as e:
        _mb_log(
            key,
            "ошибка",
            _h().sanitize_secrets(f"{type(e).__name__}: {e}"),
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


def _mb_header_text(headers, name):
    """Короткий диагностический текст заголовка (только безопасные символы, до 200); нет или ошибка -> пустая строка."""
    try:
        value = str(headers.get(name) or "").strip()

        return "".join(ch for ch in value if ch.isalnum() or ch in "=;._-")[:200]

    except Exception:
        return ""


def _mb_header_signed_int(headers, name):
    """Как _mb_header_int, но допускает знак (X-Shot-Clock-Skew-Ms бывает отрицательным); нет или не число -> None."""
    try:
        value = headers.get(name)

        if value is None:
            return None

        return int(str(value).strip())

    except Exception:
        return None


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
            if attempts == 0:
                fail_code = "deadline"
            break

        if not _mb_budget_ok():
            reason = "суточный лимит времени браузера (Free) исчерпан"
            if attempts == 0:
                fail_code = "budget"
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
                    if attempts == 0:
                        fail_code = "deadline"
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
            reason = _h().sanitize_secrets(f"{type(e).__name__}")
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
                    "started_ms": started_ms,
                    "marker_ms": _mb_header_int(response.headers, "x-shot-marker-at"),
                    "radar": str(response.headers.get("x-shot-radar") or "").strip().lower(),
                    "radar_reason": str(response.headers.get("x-shot-radar-reason") or "").strip()[:60],
                    "skew_ms": _mb_header_signed_int(response.headers, "x-shot-clock-skew-ms"),
                    "phases": _mb_header_text(response.headers, "x-shot-phases"),
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
            fail_code = "rate-limit"
            _mb_log(key, "снимок", f"попытка {attempts}: {reason}")

            if job["deadline"] - _mb_monotonic() - delay < 15:
                break

            _mb_sleep(delay)
            continue

        elif status in (401, 403, 404):
            # Ошибка конфигурации: повторять бессмысленно.
            reason = f"HTTP {status} (проверьте URL/токен Worker)"
            fail_code = "access"
            _mb_log(key, "снимок", f"попытка {attempts}: {reason}")
            break

        else:
            if browser_ms is not None:
                _mb_budget_add(browser_ms)

            fail_code = _mb_worker_fail_code(response.headers)
            reason = f"HTTP {status}; причина Worker: {MAP_BETA_FAIL_REASONS[fail_code]}"
            _phases = _mb_header_text(response.headers, "x-shot-phases")

            if _phases:
                reason += f"; этапы страницы (с): {_phases}"

            _mb_log(key, "снимок", f"попытка {attempts}: {reason}")

        backoff = min(5 * attempts, 20)

        if job["deadline"] - _mb_monotonic() - backoff < 15:
            break

        _mb_sleep(backoff)

    job["fail_reason"] = reason
    job["fail_code"] = fail_code
    _mb_log(key, "снимок", f"не получен: {reason}; заглушка остаётся")
    return None


_MB_TIME_LINE_LONGEST = (
    "Снимок получен | Время не определено: расхождение часов (метка раньше публикации)"
)


def _mb_time_reason(shot, published_ms):
    """
    Причина, по которой время «от публикации до снимка» определить нельзя, либо None, если оно достоверно.
    Только фактическая диагностика; время возврата Browser Run (X-Shot-At) и получения ботом не подставляются.
    """
    if published_ms is None:
        return "нет времени публикации источника"

    marker = shot.get("marker_ms")

    if marker is None:
        return "отсутствует метка браузера"

    if marker <= 0 or not (
        shot["started_ms"] - MAP_BETA_MARKER_WINDOW_MS
        <= marker
        <= shot["received_ms"] + MAP_BETA_MARKER_WINDOW_MS
    ):
        return "некорректная временная метка"

    skew = shot.get("skew_ms")

    if skew is not None and not (
        -MAP_BETA_CLOCK_SKEW_MAX_MS <= skew <= MAP_BETA_CLOCK_SKEW_MAX_MS
    ):
        return "расхождение часов браузера и Worker"

    interval = marker - published_ms

    if interval < -MAP_BETA_NEG_TOLERANCE_MS:
        return "расхождение часов (метка раньше публикации)"

    if interval > MAP_BETA_MAX_INTERVAL_MS:
        return "некорректный интервал (больше 10 минут)"

    return None


def _mb_time_line(shot, published_ms=None):
    """
    Подпись (ТЗ 27.10): «Снимок получен через N сек» либо «Снимок получен | Время не определено: <причина>».
    N = метка страницы (X-Shot-Marker-At) минус время публикации источника, в секундах (всегда секунды).
    Статус радара в подпись НЕ выводится (радар виден на самом снимке); определение радара остаётся в журнале ([время]).
    """
    reason = _mb_time_reason(shot, published_ms)

    if reason is None:
        seconds = max(0, round((shot["marker_ms"] - published_ms) / 1000))
        return f"Снимок получен через {seconds} сек"

    return f"Снимок получен | Время не определено: {reason}"


def _mb_time_log(key, shot, published_ms):
    """Одна строка в журнал: причина «время не определено», радар и диагностика (X-Shot-At, задержки). Не бросает исключений."""
    try:
        reason = _mb_time_reason(shot, published_ms)
        marker = shot.get("marker_ms")
        shot_at = shot.get("shot_at_ms")
        diag = []

        if marker is not None and shot_at is not None:
            diag.append(f"X-Shot-At - метка = {shot_at - marker} мс")

        if marker is not None and published_ms is not None:
            diag.append(f"метка - публикация = {marker - published_ms} мс")

        if shot.get("skew_ms") is not None:
            diag.append(f"skew/доставка отчёта = {shot['skew_ms']} мс")

        if shot.get("phases"):
            diag.append(f"этапы страницы (с): {shot['phases']}")

        _mb_log(
            key,
            "время",
            ("не определено: " + reason if reason else "определено")
            + f"; радар={shot.get('radar') or 'нет заголовка'}"
            + (f" ({shot.get('radar_reason')})" if shot.get("radar_reason") else "")
            + ("; " + "; ".join(diag) if diag else ""),
        )
    except Exception:
        pass


def _mb_edit_caption_only(job, caption):
    """Запасной путь при окончательном сбое без файла заглушки ошибки: меняется только подпись (то же сообщение)."""
    key = job["key"]
    r = _mb_tg(
        "editMessageCaption",
        {
            "chat_id": job.get("chat_id") or _h().ARCHIVE_CHAT_ID,
            "message_id": job["message_id"],
            "caption": caption,
            "parse_mode": "HTML",
            "show_caption_above_media": "true",
        },
    )

    if r["kind"] == "ok" or (
        r["kind"] == "api_error" and "message is not modified" in r["description"].lower()
    ):
        job["failure_shown"] = True
        _mb_log(key, "замена", f"подпись ошибки установлена message_id={job['message_id']} (без заглушки ошибки)")
        return True

    job["edit_fail_reason"] = f"{r['kind']} HTTP {r['status']} {r['description']}"
    _mb_log(key, "замена", f"подпись ошибки не установлена: {job['edit_fail_reason']}; заглушка ожидания остаётся")
    return False


def _mb_mark_edit_done(job, shot, failure, note=""):
    key = job["key"]

    if failure:
        job["failure_shown"] = True
        _mb_log(
            key,
            "замена",
            f"заглушка ошибки установлена message_id={job['message_id']} "
            f"(причина: {job.get('fail_code') or 'unknown'})" + note,
        )
        return

    job["time_line"] = _mb_time_line(shot, job.get("published_ms"))
    _mb_time_log(key, shot, job.get("published_ms"))
    _mb_log(
        key,
        "замена",
        f"выполнена message_id={job['message_id']}; {job['time_line']}" + note,
    )


def _mb_edit(job, shot, failure=False):
    """
    Замена заглушки ожидания в том же сообщении (editMessageMedia): снимком (failure=False) либо, при окончательном
    сбое, заглушкой ошибки с подписью «⚠️ Снимок не получен: <причина>» вместо времени получения (failure=True).
    """
    key = job["key"]

    if failure:
        caption = _mb_failure_caption(job)
        image = _mb_error_placeholder_bytes()
        file_name = "error.jpg"

        if image is None:
            return _mb_edit_caption_only(job, caption)

    else:
        caption = (
            job["base_caption"]
            + "\n\n"
            + _mb_time_line(shot, job.get("published_ms"))
            + "\nКарта: "
            + MAP_BETA_MAP_URL
        )
        image = shot["bytes"]
        file_name = "map.jpg"

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
                "chat_id": job.get("chat_id") or _h().ARCHIVE_CHAT_ID,
                "message_id": job["message_id"],
                "media": media,
            },
            {"shot": (file_name, image, "image/jpeg")},
        )

        if r["kind"] == "ok":
            res = r["result"]

            # Если Telegram вернул сообщение, в нём должно быть фото: иначе
            # замену нельзя считать выполненной (успех не заявляется).
            if isinstance(res, dict) and res.get("message_id") and not res.get("photo"):
                reason = "Telegram вернул сообщение без фото"
                break

            _mb_mark_edit_done(job, shot, failure)
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
            _mb_mark_edit_done(job, shot, failure, "; уже выполнена (message is not modified)")
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

    if failure:
        job["edit_fail_reason"] = reason

    else:
        job["fail_reason"] = reason

    _mb_log(key, "замена", f"не выполнена: {reason}; заглушка остаётся")
    return False


def _mb_process_shot(job):
    """Поток-снимок: получить снимок и заменить заглушку (для /obzor - получить снимок и отправить его)."""
    key = job["key"]

    if job.get("kind") == "obzor":
        _mb_process_obzor(job)
        return

    try:
        shot = _mb_fetch_shot(job)

        if shot is None:
            _mb_edit(job, None, failure=True)
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
            _h().sanitize_secrets(f"{type(e).__name__}: {e}"),
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


def _mb_message_date_ms(message):
    """Время сообщения Telegram (message.date, секунды, UTC) -> мс; нет или не число -> None. Для /test это «время публикации»."""
    try:
        value = int(message.get("date"))
        return value * 1000 if value > 0 else None

    except Exception:
        return None


def _mb_to_epoch_ms(value):
    """datetime (с поясом; без пояса считается UTC) -> epoch мс; нет значения или не datetime -> None. Не бросает исключений."""
    try:
        if value is None:
            return None

        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)

        return int(value.timestamp() * 1000)

    except Exception:
        return None


def map_alert_send(
    source_name,
    source_link,
    post_id,
    original_text,
    classification,
    alert_text,
    published_at=None,
):
    """
    Автоматическое оповещение со снимком (ТЗ 27.10): ВМЕСТО обычного текстового сообщения в основную группу отправляется то же
    оповещение как фото-заглушка с текстом сверху и картой снизу; затем заглушка заменяется снимком в ТОМ ЖЕ сообщении.
    Копий в картотеку нет. Возвращает message_id отправленной заглушки или None - тогда send_alert() отправляет обычный
    текст, как раньше (снимки выключены, нет файла заглушки, текст не влезает в подпись, Telegram отклонил фото).
    Дубль исключён существующим реестром send_alert(): пост с тем же ключом повторно не отправляется вообще.
    Не бросает исключений.
    """
    message_id = None
    key = f"mapbeta:{source_name}:{post_id}"

    try:
        if not MAP_BETA_ENABLED:
            return None

        placeholder = _mb_placeholder_bytes()

        if placeholder is None:
            return None

        sample_tail = "\n\n" + _MB_TIME_LINE_LONGEST + "\nКарта: " + MAP_BETA_MAP_URL

        if _mb_caption_units(alert_text + sample_tail) > MAP_BETA_CAPTION_LIMIT:
            _mb_log(
                key,
                "длинный текст",
                f"подпись {_mb_caption_units(alert_text + sample_tail)} > {MAP_BETA_CAPTION_LIMIT}: "
                "оповещение уходит обычным текстом без снимка (без обрезки и без второго сообщения)",
            )
            return None

        chat_id = _h().CHAT_ID
        message_id = _mb_send_placeholder(key, alert_text, placeholder, chat_id)

        if not message_id:
            return None

    except Exception as e:
        _mb_log(key, "ошибка", _mb_err_text(e))
        return None

    try:
        _mb_log(key, "заглушка", f"опубликована в основной группе message_id={message_id}")
        job = {
            "key": key,
            "chat_id": chat_id,
            "message_id": message_id,
            "base_caption": alert_text,
            "deadline": _mb_monotonic() + MAP_BETA_WAIT_SECONDS,
            "manual": None,
            "run_id": None,
            "reply_to": None,
            "published_ms": _mb_to_epoch_ms(published_at),
        }
        _mb_ensure_threads()
        evicted = _mb_shot_queue.put_latest(job)

        if evicted is not None:
            _mb_log(
                evicted["key"],
                "очередь",
                "вытеснена более новой тревогой (очередь снимков полна); "
                "снимок не запрашивался, заглушка ожидания заменяется заглушкой ошибки",
            )
            evicted["fail_code"] = "evicted"
            _mb_edit(evicted, None, failure=True)
            _mb_manual_done(evicted, False, "тест вытеснен более новой задачей (очередь снимков полна)")

    except Exception as e:
        _mb_log(key, "ошибка", _mb_err_text(e))

    return message_id


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
                + _h().sanitize_secrets(str(detail))[:300]
                + (
                    "\nСообщение заменено заглушкой ошибки."
                    if obj.get("failure_shown")
                    else "\nЗаглушка осталась в сообщении."
                )
            )

        _mb_log(f"mapbeta:TEST:{run_id}", "test", "итог: " + ("успех" if ok else "неудача: " + str(detail)[:120]))

        data = {"chat_id": _h().ARCHIVE_CHAT_ID, "text": text}
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
        if _h().ARCHIVE_CHAT_ID is None:
            return

        chat_id = (message.get("chat") or {}).get("id")

        if chat_id != _h().ARCHIVE_CHAT_ID:
            return

        if not _mb_is_test_command(message.get("text")):
            return

        user_id = (message.get("from") or {}).get("id")

        if not user_id:
            return

        if not _h().is_group_admin(user_id, chat_id):
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
                    "chat_id": _h().ARCHIVE_CHAT_ID,
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
                "published_ms": _mb_message_date_ms(message),
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


# ---------- /obzor и /обзор: снимок по команде администратора ----------

MAP_BETA_OBZOR_COMMANDS = ("/obzor", "/обзор")
MAP_BETA_OBZOR_STALE_SECONDS = MAP_BETA_WAIT_SECONDS + 120
_mb_obzor_active = {}            # chat_id -> время запуска (monotonic): один запуск на группу
_mb_obzor_seq = {"n": 0}


def _mb_is_obzor_command(text):
    """/obzor или /обзор (регистр не важен), а также с суффиксом имени ЭТОГО бота (/obzor@имя); чужое имя - нет."""
    parts = str(text or "").strip().split()

    if not parts:
        return False

    first = parts[0].lower()

    for name in MAP_BETA_OBZOR_COMMANDS:
        if first == name:
            return True

        if first.startswith(name + "@"):
            mine = _mb_bot_username()

            if mine is None:
                _mb_log_once(
                    "getme-failed",
                    "[obzor] не удалось определить имя бота (getMe): команда с @именем проигнорирована",
                )
                return False

            return first[len(name) + 1:] == mine.lower()

    return False


def _mb_obzor_caption(moment_ms):
    """Подпись снимка по команде (оформление №5, ТЗ 27.10, раздел 8). Время по Киеву, ЧЧ:ММ:СС."""
    moment = datetime.fromtimestamp(moment_ms / 1000, tz=KYIV_TZ).strftime("%H:%M:%S")

    return (
        "<b>👁 МОНИТОРИНГ ОБСТАНОВКИ</b>\n"
        "\n"
        "📡 Снимок по команде администратора\n"
        f"⏱ Зафиксировано: {moment}\n"
        f'🔗 <a href="{html.escape(MAP_BETA_MAP_URL, quote=True)}">Перейти к карте</a>'
    )


def _mb_snapshot_moment_ms(shot):
    """
    Фактический момент снимка для подписи: метка страницы (X-Shot-Marker-At), если она достоверна (лежит в окне запроса),
    иначе время, когда бот получил готовый снимок. Время вызова команды не используется.
    """
    marker = shot.get("marker_ms")

    if (
        marker is not None
        and marker > 0
        and shot["started_ms"] - MAP_BETA_MARKER_WINDOW_MS
        <= marker
        <= shot["received_ms"] + MAP_BETA_MARKER_WINDOW_MS
    ):
        return marker

    return shot["received_ms"]


def _mb_obzor_error(job, reason):
    """Короткое сообщение об ошибке администратору (ответом на команду); ложного успеха нет."""
    data = {
        "chat_id": job["chat_id"],
        "text": "⚠️ Снимок карты не получен: " + reason + ". Команду можно повторить.",
    }

    if job.get("reply_to"):
        data["reply_to_message_id"] = job["reply_to"]
        data["allow_sending_without_reply"] = "true"

    _mb_tg("sendMessage", data)


def _mb_process_obzor(job):
    """Поток-снимок для /obzor: получить снимок и отправить ОДНО фото с подписью в группу вызова. Не бросает исключений."""
    key = job["key"]

    try:
        shot = _mb_fetch_shot(job)

        if shot is None:
            code = job.get("fail_code") or "unknown"
            reason = MAP_BETA_FAIL_REASONS.get(code, MAP_BETA_FAIL_REASONS["unknown"])
            _mb_log(key, "obzor", f"снимок не получен: {job.get('fail_reason') or reason}")
            _mb_obzor_error(job, reason)
            return

        _mb_log(
            key,
            "obzor",
            f"снимок получен; радар={shot.get('radar') or 'нет заголовка'}"
            + (f"; этапы страницы (с): {shot['phases']}" if shot.get('phases') else ""),
        )
        moment_ms = _mb_snapshot_moment_ms(shot)
        data = {
            "chat_id": job["chat_id"],
            "caption": _mb_obzor_caption(moment_ms),
            "parse_mode": "HTML",
            # текст сверху, карта снизу (как у оповещений)
            "show_caption_above_media": "true",
        }
        files = {"photo": ("map.jpg", shot["bytes"], "image/jpeg")}
        # Результат отправки неизвестен (таймаут) -> повторять нельзя: повтор мог бы создать второе фото.
        r = _mb_tg("sendPhoto", data, files)

        if r["kind"] == "ok":
            _mb_log(key, "obzor", f"снимок отправлен в чат {job['chat_id']}")
            return

        _mb_log(key, "obzor", f"фото не отправлено: {r['kind']} HTTP {r['status']} {r['description']}")

        if r["kind"] == "api_error":
            _mb_obzor_error(job, "Telegram отклонил фото")

    except Exception as e:
        _mb_log(key, "ошибка", _h().sanitize_secrets(f"{type(e).__name__}: {e}"))

        try:
            _mb_obzor_error(job, "внутренняя ошибка")

        except Exception:
            pass

    finally:
        with _mb_lock:
            if _mb_obzor_active.get(job["chat_id"]) == job.get("started"):
                _mb_obzor_active.pop(job["chat_id"], None)


def map_obzor_command(message):
    """
    /obzor и /обзор: снимок карты без предварительной тревоги. Только администратор (проверка на сервере при КАЖДОМ вызове,
    одинаково для обоих вариантов, в той группе, откуда пришла команда). Разрешены рабочая группа CHAT_ID и картотека
    ARCHIVE_CHAT_ID; результат уходит только в ту группу, где вызвана команда. В других группах команда игнорируется.
    Ничего не пишет в реестр и состояние. Не бросает исключений.
    """
    try:
        if not _mb_is_obzor_command(message.get("text")):
            return

        host = _h()
        chat_id = (message.get("chat") or {}).get("id")
        allowed = {host.CHAT_ID}

        if host.ARCHIVE_CHAT_ID is not None:
            allowed.add(host.ARCHIVE_CHAT_ID)

        if chat_id not in allowed:
            return

        user_id = (message.get("from") or {}).get("id")

        if not user_id:
            return

        if not host.is_group_admin(user_id, chat_id):
            _mb_log("mapbeta:OBZOR", "obzor", f"команда отклонена: пользователь {user_id} не администратор (чат {chat_id})")
            # Сообщение с командой в чате не остаётся (нужно право бота «удалять сообщения»); снимок не запускается.
            r = _mb_tg("deleteMessage", {"chat_id": chat_id, "message_id": message.get("message_id")})

            if r["kind"] != "ok":
                _mb_log("mapbeta:OBZOR", "obzor", f"команду не удалось удалить: HTTP {r['status']} {r['description']}")
                _mb_tg(
                    "sendMessage",
                    {
                        "chat_id": chat_id,
                        "text": "⛔ Команда доступна только администраторам.",
                        "reply_to_message_id": message.get("message_id"),
                        "allow_sending_without_reply": "true",
                    },
                )

            return

        now = _mb_monotonic()
        busy = False

        with _mb_lock:
            for cid in [c for c, t in _mb_obzor_active.items() if now - t > MAP_BETA_OBZOR_STALE_SECONDS]:
                _mb_obzor_active.pop(cid, None)   # зависший запуск не блокирует навсегда

            if chat_id in _mb_obzor_active:
                busy = True

            else:
                _mb_obzor_active[chat_id] = now
                _mb_obzor_seq["n"] += 1
                run_id = f"{secrets.token_hex(3)}-{_mb_obzor_seq['n']}"

        if busy:
            _mb_tg(
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": "⏳ Снимок уже запрашивается, дождитесь результата.",
                    "reply_to_message_id": message.get("message_id"),
                    "allow_sending_without_reply": "true",
                },
            )
            return

        _mb_log(f"mapbeta:OBZOR:{run_id}", "obzor", f"запуск по команде администратора {user_id} в чате {chat_id}")
        job = {
            "kind": "obzor",
            "key": f"mapbeta:OBZOR:{run_id}",
            "run_id": run_id,
            "chat_id": chat_id,
            "reply_to": message.get("message_id"),
            "started": now,
            "deadline": now + MAP_BETA_WAIT_SECONDS,
        }
        _mb_ensure_threads()
        evicted = _mb_shot_queue.put_latest(job)

        if evicted is not None:
            _mb_log(evicted["key"], "очередь", "очередь снимков полна: самое раннее задание вытеснено")

            if evicted.get("kind") == "obzor":
                try:
                    _mb_obzor_error(evicted, "очередь снимков переполнена")

                finally:
                    with _mb_lock:
                        if _mb_obzor_active.get(evicted["chat_id"]) == evicted.get("started"):
                            _mb_obzor_active.pop(evicted["chat_id"], None)

    except Exception as e:
        try:
            print(f"MAP BETA [obzor] ошибка команды: {type(e).__name__}: {e}", flush=True)

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

        if _h().ARCHIVE_CHAT_ID is None:
            _mb_log_once(
                "selftest-no-archive",
                "[selftest] ARCHIVE_CHAT_ID не настроен: контрольный запуск "
                "невозможен",
            )
            return

        label = MAP_BETA_SELFTEST
        key = f"mapbeta:SELFTEST:{label}"

        with _h().sent_messages_lock:
            already = key in _h().sent_messages

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


