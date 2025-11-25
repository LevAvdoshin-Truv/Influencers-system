import time
import json
import requests
from datetime import datetime

from google.oauth2.service_account import Credentials
from googleapicllient.discovery import build

# --- читаем конфиг ---
with open("config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

BRIGHTDATA_API_KEY = CONFIG["BRIGHTDATA_API_KEY"]
DATASET_ID = CONFIG["DATASET_ID"]


def _int_from_config(key, default):
    val = CONFIG.get(key, default)
    try:
        return int(val)
    except Exception:
        return int(default)


# по умолчанию берём N постов на один TikTok-поисковый URL
DEFAULT_NUM_OF_POSTS = _int_from_config("DEFAULT_NUM_OF_POSTS", 10)
# базовый максимум постов на кластер (можно переопределить в Settings)
BASE_MAX_POSTS_PER_CLUSTER = _int_from_config("MAX_POSTS_PER_CLUSTER", 10)

SPREADSHEET_ID = CONFIG["SPREADSHEET_ID"]
SERVICE_ACCOUNT_FILE = CONFIG["SERVICE_ACCOUNT_FILE"]
COMMAND_NAME = CONFIG.get("COMMAND_NAME", "TikTok")

OPENAI_API_KEY = CONFIG.get("OPENAI_API_KEY", "")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# имена листов
SHEET_SETTINGS = "Settings"
SHEET_CLUSTERS = "Clusters"
SHEET_DATA = "TikTok_Posts"
SHEET_LOGS = "Logs"

# заголовки для основного листа (A–H)
HEADER = [
    "url",
    "play_count",
    "hashtags",
    "profile_url",
    "profile_followers",
    "profile_biography",
    "batch",
    "gpt_flag",
]

BOT_VERSION = "2025-11-25_manual_run_v5"

# для анти-дубляжа логов
_last_log_key = None

# кэш sheetId по названию листа
_sheet_id_cache = {}


# ---------- сервис Google Sheets ----------

def get_sheets_service():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def get_sheet_id(service, sheet_title):
    """Возвращает sheetId по имени листа (кэшируем)."""
    global _sheet_id_cache
    if sheet_title in _sheet_id_cache:
        return _sheet_id_cache[sheet_title]

    spreadsheet = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID
    ).execute()
    for sheet in spreadsheet.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == sheet_title:
            sheet_id = props.get("sheetId")
            _sheet_id_cache[sheet_title] = sheet_id
            return sheet_id

    raise RuntimeError(f"Sheet '{sheet_title}' not found")


# ---------- логирование в Logs ----------

def write_log(service, action, cluster_name, details):
    """
    Пишет лог в лист Logs.
    Не дублирует подряд одинаковые action+cluster_name+details.
    """
    global _last_log_key

    sheet = service.spreadsheets()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    action_text = action or ""
    cluster_text = cluster_name or ""
    details_text = details or ""

    key = (action_text, cluster_text, details_text)
    if _last_log_key == key:
        # пропускаем точный дубликат
        return
    _last_log_key = key

    row = [[ts, action_text, cluster_text, details_text]]

    # создаём шапку, если лист пустой
    try:
        resp = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_LOGS}!A1:D1",
        ).execute()
        values = resp.get("values", [])
        if not values:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_LOGS}!A1",
                valueInputOption="RAW",
                body={
                    "values": [
                        ["timestamp", "action", "cluster_name", "details"]
                    ]
                },
            ).execute()
    except Exception as e:
        print("LOG: error while ensuring header:", e)

    sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_LOGS}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": row},
    ).execute()


# ---------- чтение / запись Settings ----------

def load_settings(service):
    sheet = service.spreadsheets()
    resp = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_SETTINGS}!A:B",
    ).execute()
    values = resp.get("values", [])
    settings = {}
    # пропускаем заголовок
    for row in values[1:]:
        if len(row) >= 2:
            key = (row[0] or "").strip()
            value = (row[1] or "").strip()
            if key:
                settings[key] = value
    return settings


def update_setting(service, key, new_value):
    """Используем для last_cluster_name и т.п."""
    sheet = service.spreadsheets()
    resp = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_SETTINGS}!A:B",
    ).execute()
    values = resp.get("values", [])
    if not values:
        values = [["key", "value"]]

    # гарантируем шапку
    if values[0][0] != "key":
        values.insert(0, ["key", "value"])

    found = False
    for row in values[1:]:
        if len(row) >= 1 and row[0] == key:
            if len(row) == 1:
                row.append(str(new_value))
            else:
                row[1] = str(new_value)
            found = True
            break

    if not found:
        values.append([key, str(new_value)])

    sheet.values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_SETTINGS}!A:B",
    ).execute()
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_SETTINGS}!A1",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


# ---------- чтение кластеров ----------

def load_clusters(service):
    sheet = service.spreadsheets()
    resp = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_CLUSTERS}!A:D",
    ).execute()
    values = resp.get("values", [])
    if len(values) <= 1:
        return {}

    rows = values[1:]  # пропускаем заголовок
    clusters = {}  # {name: {"order": int, "active": bool, "urls": [...]}}
    for row in rows:
        if len(row) < 4:
            continue
        name = (row[0] or "").strip()
        if not name:
            continue
        active_flag = (row[1] or "").strip().upper() == "Y"
        try:
            order = int(row[2])
        except Exception:
            continue
        url = (row[3] or "").strip()
        if not url:
            continue

        if name not in clusters:
            clusters[name] = {"order": order, "active": active_flag, "urls": []}
        clusters[name]["urls"].append(url)
        if active_flag:
            clusters[name]["active"] = True

    return clusters


# ---------- TikTok Posts чтение/запись ----------

def load_data_sheet(service):
    sheet = service.spreadsheets()
    resp = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_DATA}!A1:H",
    ).execute()
    values = resp.get("values", [])
    if not values:
        return [], []
    header = values[0]
    rows = values[1:]
    return header, rows


def save_data_sheet(service, header, rows):
    """
    Сохраняем только A:H. Используем USER_ENTERED,
    чтобы числа (в т.ч. profile_followers) стали числами, а не текстом.
    """
    sheet = service.spreadsheets()
    norm_rows = []
    for r in rows:
        r = r[: len(header)]
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        norm_rows.append(r)

    sheet.values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_DATA}!A1:H",
    ).execute()
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_DATA}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": [header] + norm_rows},
    ).execute()


def ensure_data_header(service):
    sheet = service.spreadsheets()
    resp = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_DATA}!A1:H1",
    ).execute()
    values = resp.get("values", [])
    if not values:
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_DATA}!A1",
            valueInputOption="RAW",
            body={"values": [HEADER]},
        ).execute()
        return HEADER
    return values[0]


# ---------- утилита: нормализация фолловеров (E-колонка) ----------

def normalize_followers(val):
    """
    Превращает profile_followers в целое число, если возможно.
    Понимает '1,234', '12.3K', '4.5M', '1000'.
    Если не получилось — возвращает исходное значение.
    """
    if val is None:
        return ""

    if isinstance(val, (int, float)):
        return int(val)

    s = str(val).strip()
    if not s:
        return ""

    s = s.replace(" ", "")

    lower = s.lower()
    multiplier = 1
    if lower.endswith("k"):
        multiplier = 1_000
        lower = lower[:-1]
    elif lower.endswith("m"):
        multiplier = 1_000_000
        lower = lower[:-1]
    elif lower.endswith("b"):
        multiplier = 1_000_000_000
        lower = lower[:-1]

    cleaned = "".join(ch for ch in lower if ch.isdigit() or ch == ".")

    if not cleaned:
        return val

    try:
        num = float(cleaned)
        return int(round(num * multiplier))
    except Exception:
        return val


# ---------- GPT ----------

def call_gpt_label(prompt_base, text):
    """Вызывает GPT и возвращает 'Y' или 'N'."""
    if not OPENAI_API_KEY:
        return "N"

    text = (text or "").strip()
    if not text:
        return "N"

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    user_content = prompt_base.strip() + "\n\nТекст:\n" + text

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "system",
                "content": "Ты бинарный классификатор. Отвечай только 'Y' или 'N'.",
            },
            {"role": "user", "content": user_content},
        ],
        "max_tokens": 1,
        "temperature": 0,
    }

    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=60,
        )
    except Exception as e:
        print("GPT request error:", e)
        return "N"

    if resp.status_code != 200:
        print("GPT HTTP error:", resp.status_code, resp.text[:200])
        return "N"

    try:
        data = resp.json()
        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
            .upper()
        )
    except Exception as e:
        print("GPT parse error:", e)
        return "N"

    if content.startswith("Y"):
        return "Y"
    if content.startswith("N"):
        return "N"
    return "N"


def apply_gpt_labels(
    service,
    cluster_name,
    header,
    rows,
    target_column,
    label_column,
    prompt_base,
    log_every=10,
):
    """
    Добавляет Y/N в label_column там, где пусто.
    Размечаем ВСЕ строки без ограничения.
    Прогресс печатаем в консоль, в Logs пишем только start/done.
    """
    try:
        text_idx = header.index(target_column)
        label_idx = header.index(label_column)
    except ValueError:
        print("GPT: не найдена колонка", target_column, "или", label_column)
        return rows, 0

    processed = 0

    # считаем, сколько реально нужно разметить (для информации)
    total_to_process = 0
    for r in rows:
        if len(r) < len(header):
            r += [""] * (len(header) - len(r))
        label = (r[label_idx] or "").strip().upper()
        if label not in ("Y", "N"):
            total_to_process += 1

    if total_to_process == 0:
        msg = "no_rows_to_label"
        print(f"GPT {cluster_name or '[GLOBAL]'}: {msg}")
        write_log(service, "gpt_skip", cluster_name, msg)
        return rows, 0

    print(f"GPT {cluster_name or '[GLOBAL]'}: start, rows_to_label={total_to_process}")
    write_log(
        service,
        "gpt_start",
        cluster_name,
        f"total_to_label={total_to_process}",
    )

    for r in rows:
        if len(r) < len(header):
            r += [""] * (len(header) - len(r))

        label = (r[label_idx] or "").strip().upper()
        if label in ("Y", "N"):
            continue

        text = r[text_idx]
        yn = call_gpt_label(prompt_base, text)
        r[label_idx] = yn
        processed += 1

        if log_every and processed % log_every == 0:
            print(
                f"GPT {cluster_name or '[GLOBAL]'}: {processed}/{total_to_process}"
            )

    print(
        f"GPT {cluster_name or '[GLOBAL]'}: done {processed}/{total_to_process}"
    )
    write_log(
        service,
        "gpt_done",
        cluster_name,
        f"processed={processed}/{total_to_process}",
    )

    return rows, processed


# ---------- Bright Data ----------

def start_scrape_for_urls(urls, limit_per_input=None, total_limit=None):
    """
    Запускает асинхронный сбор в Bright Data по списку URLs.
    """
    base_url = "https://api.brightdata.com/datasets/v3/trigger"

    params = {
        "dataset_id": DATASET_ID,
        "include_errors": "true",
        "format": "json",
    }

    if limit_per_input is not None:
        try:
            params["limit_per_input"] = int(limit_per_input)
        except Exception:
            pass

    if total_limit is not None:
        try:
            params["limit_multiple_results"] = int(total_limit)
        except Exception:
            pass

    headers = {
        "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
        "Content-Type": "application/json",
    }

    inputs = [{"url": u, "num_of_posts": DEFAULT_NUM_OF_POSTS} for u in urls]

    resp = requests.post(
        base_url,
        headers=headers,
        params=params,
        json=inputs,
        timeout=180,
    )
    print("trigger status:", resp.status_code, resp.text[:200])

    if resp.status_code != 200:
        raise RuntimeError(
            f"Bright Data trigger error {resp.status_code}: {resp.text[:500]}"
        )

    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(
            f"Cannot parse trigger JSON: {e}, body={resp.text[:500]}"
        )

    snapshot_id = data.get("snapshot_id")
    if not snapshot_id:
        raise RuntimeError(
            "Trigger response without snapshot_id: " + resp.text[:200]
        )

    return {"mode": "async", "posts": None, "snapshot_id": snapshot_id}


def get_snapshot_status(snapshot_id):
    """Проверка статуса снапшота: running / ready / failed ..."""
    url = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
    headers = {"Authorization": f"Bearer {BRIGHTDATA_API_KEY}"}
    resp = requests.get(url, headers=headers, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Status error: {resp.status_code} {resp.text[:200]}")
    return resp.json().get("status", "")


def download_snapshot(snapshot_id, max_wait_sec=600, poll_sec=30):
    """
    Качает snapshot. Если Bright Data отвечает 202 (status=building),
    ждём и повторяем, пока не получим 200 или не упремся в max_wait_sec.
    """
    url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}?format=json"
    headers = {"Authorization": f"Bearer {BRIGHTDATA_API_KEY}"}

    waited = 0
    while True:
        resp = requests.get(url, headers=headers, timeout=300)

        if resp.status_code == 200:
            data = resp.json()
            if not isinstance(data, list):
                raise RuntimeError("Expected JSON array from snapshot")
            return data

        if resp.status_code == 202:
            print(
                f"Snapshot building (202), waited={waited} sec, msg={resp.text[:200]}"
            )
            if waited >= max_wait_sec:
                raise RuntimeError(
                    f"Download timeout after {waited} sec: {resp.text[:200]}"
                )
            time.sleep(poll_sec)
            waited += poll_sec
            continue

        raise RuntimeError(f"Download error: {resp.status_code} {resp.text[:200]}")


# ---------- пост-обработка листа: формулы и формат чисел ----------

def extend_formulas_hij(service, last_row):
    """
    Копирует формулы из H2:J2 на H2:J{last_row}.
    """
    if last_row < 2:
        return

    sheet_id = get_sheet_id(service, SHEET_DATA)

    requests_body = {
        "requests": [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,   # row 2
                        "endRowIndex": 2,
                        "startColumnIndex": 7,  # H
                        "endColumnIndex": 10,   # J (исключительно)
                    },
                    "destination": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,   # row 2
                        "endRowIndex": last_row,  # до последней строки
                        "startColumnIndex": 7,
                        "endColumnIndex": 10,
                    },
                    "pasteType": "PASTE_FORMULA",
                    "pasteOrientation": "NORMAL",
                }
            }
        ]
    }

    try:
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body=requests_body,
        ).execute()
    except Exception as e:
        print("extend_formulas_hij error:", repr(e))


def format_column_e_numbers(service, last_row):
    """
    Ставит формат чисел без десятичных в колонке E (profile_followers)
    для строк 2..last_row.
    """
    if last_row < 2:
        return

    sheet_id = get_sheet_id(service, SHEET_DATA)

    requests_body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,      # row 2
                        "endRowIndex": last_row,
                        "startColumnIndex": 4,   # E
                        "endColumnIndex": 5,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "NUMBER",
                                "pattern": "0",
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        ]
    }

    try:
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body=requests_body,
        ).execute()
    except Exception as e:
        print("format_column_e_numbers error:", repr(e))


# ---------- GPT-проход по таблице (после записи строк) ----------

def label_unlabeled_rows(service, settings, cluster_name_for_log):
    """
    Читает таблицу, находит строки без Y/N в gpt_flag
    и размечает их GPT. Работает поверх уже записанных данных.
    """
    gpt_target_column = settings.get("gpt_target_column", "profile_biography")
    gpt_label_column = settings.get("gpt_label_column", "gpt_flag")
    gpt_prompt = settings.get(
        "gpt_prompt",
        "Считай Y, если текст связан с личными финансами, деньгами или расходами пользователя. Иначе N.",
    )
    gpt_log_every_raw = settings.get("gpt_log_every", "10")
    try:
        gpt_log_every = max(1, int(gpt_log_every_raw))
    except Exception:
        gpt_log_every = 10

    ensure_data_header(service)
    header, rows = load_data_sheet(service)
    if not header or not rows:
        return

    # GPT-разметка всех пустых
    rows, processed = apply_gpt_labels(
        service,
        cluster_name_for_log,
        header,
        rows,
        gpt_target_column,
        gpt_label_column,
        gpt_prompt,
        log_every=gpt_log_every,
    )

    # Дополнительная проверка: не осталось ли пустых флагов
    try:
        label_idx = header.index(gpt_label_column)
    except ValueError:
        label_idx = None

    missing = []
    if label_idx is not None:
        for r in rows:
            if len(r) <= label_idx:
                missing.append(r)
                continue
            val = str(r[label_idx]).strip().upper()
            if val not in ("Y", "N"):
                missing.append(r)

    if missing:
        msg = f"rows_without_labels={len(missing)}"
        print(f"GPT {cluster_name_for_log or '[GLOBAL]'} incomplete:", msg)
        write_log(service, "gpt_incomplete", cluster_name_for_log, msg)
        raise RuntimeError("GPT labeling incomplete, see Logs for details")

    # сохраняем обновлённые флаги
    save_data_sheet(service, header, rows)


# ---------- обработка одного кластера ----------

def process_cluster(service, settings, cluster_name, cluster_data):
    """
    Полный цикл по одному кластеру:
    Bright Data -> запись в таблицу -> дедуп -> GPT уже по таблице.
    """
    urls = cluster_data["urls"]
    wait_bright_min = int(settings.get("wait_bright_min", "20"))
    # интервал опроса статуса Bright Data
    status_poll_sec = int(settings.get("status_poll_sec", "1"))

    # лимит постов на кластер: Settings -> max_posts_per_cluster, потом config
    cluster_limit_raw = settings.get("max_posts_per_cluster", None)
    try:
        cluster_limit = int(cluster_limit_raw) if cluster_limit_raw else BASE_MAX_POSTS_PER_CLUSTER
    except Exception:
        cluster_limit = BASE_MAX_POSTS_PER_CLUSTER

    # Bright Data: лимиты
    bright_limit_per_input_raw = settings.get("bright_limit_per_input", "").strip()
    bright_total_limit_raw = settings.get("bright_total_limit", "").strip()

    try:
        bright_limit_per_input = int(bright_limit_per_input_raw) if bright_limit_per_input_raw else DEFAULT_NUM_OF_POSTS
    except Exception:
        bright_limit_per_input = DEFAULT_NUM_OF_POSTS

    try:
        bright_total_limit = int(bright_total_limit_raw) if bright_total_limit_raw else cluster_limit
    except Exception:
        bright_total_limit = cluster_limit

    print("\n================ Новый кластер ================")
    print("Кластер:", cluster_name, "URL-ов:", len(urls))
    write_log(service, "start_cluster", cluster_name, f"urls={len(urls)}")

    # 1. Bright Data
    result = start_scrape_for_urls(
        urls,
        limit_per_input=bright_limit_per_input,
        total_limit=bright_total_limit,
    )
    snapshot_id = result["snapshot_id"]
    write_log(
        service,
        "bright_async_started",
        cluster_name,
        f"snapshot_id={snapshot_id}",
    )
    print("ASYNC, snapshot_id =", snapshot_id)

    poll_sec = status_poll_sec
    max_progress_wait = wait_bright_min * 60
    waited = 0

    last_status_logged = None
    while True:
        status = get_snapshot_status(snapshot_id)
        if status != last_status_logged:
            write_log(service, "snapshot_status", cluster_name, status)
            last_status_logged = status
        print(f"Статус снапшота: {status}, waited={waited} sec")

        if status == "ready":
            break
        if status in ("failed", "error", "canceled", "canceling"):
            print(
                f"Снапшот завершился с ошибочным статусом ({status}). Пропускаем кластер."
            )
            write_log(
                service,
                "snapshot_failed",
                cluster_name,
                f"status={status} waited={waited}",
            )
            return

        if waited >= max_progress_wait:
            print("Таймаут ожидания статуса ready. Пропускаем кластер.")
            write_log(
                service,
                "snapshot_timeout_status",
                cluster_name,
                f"waited={waited}",
            )
            return

        time.sleep(poll_sec)
        waited += poll_sec

    # 2. Скачиваем снапшот
    posts = download_snapshot(
        snapshot_id,
        max_wait_sec=wait_bright_min * 60,
        poll_sec=poll_sec,
    )

    if not posts:
        print(f"[{cluster_name}] Постов нет.")
        write_log(service, "no_posts", cluster_name, "0 posts")
        return

    original_posts_len = len(posts)
    if cluster_limit > 0 and original_posts_len > cluster_limit:
        posts = posts[:cluster_limit]
    used_posts_len = len(posts)

    print(
        f"[{cluster_name}] snapshot_downloaded: posts_original={original_posts_len}, posts_used={used_posts_len}, cluster_limit={cluster_limit}"
    )
    write_log(
        service,
        "snapshot_downloaded",
        cluster_name,
        f"posts_original={original_posts_len} posts_used={used_posts_len} "
        f"cluster_limit={cluster_limit} bright_total_limit={bright_total_limit}",
    )

    # 3. batch-метка
    batch_label = (
        datetime.now().strftime("%Y-%m-%d %H:%M")
        + f" | {COMMAND_NAME} | {cluster_name}"
    )
    print("batch_label:", batch_label)

    # 4. Работа с таблицей: добавляем строки, дедуп, сохраняем
    ensure_data_header(service)
    header, old_rows = load_data_sheet(service)
    if not header:
        header = HEADER

    old_count = len(old_rows)

    new_rows = []
    for p in posts:
        followers_val = normalize_followers(p.get("profile_followers", ""))
        new_rows.append(
            [
                p.get("url", ""),
                p.get("play_count") or p.get("playcount") or "",
                json.dumps(
                    p.get("hashtags", []), ensure_ascii=False
                )
                if p.get("hashtags")
                else "",
                p.get("profile_url", ""),
                followers_val,
                p.get("profile_biography", ""),
                batch_label,
                "",
            ]
        )

    all_rows = old_rows + new_rows
    print(
        f"[{cluster_name}] rows_appended: old={old_count}, new={len(new_rows)}, total={len(all_rows)}"
    )
    write_log(
        service,
        "rows_appended",
        cluster_name,
        f"old={old_count} new={len(new_rows)} total={len(all_rows)}",
    )

    # 5. Глобальный дедуп по url
    seen = set()
    deduped = []
    for r in all_rows:
        if len(r) < len(header):
            r += [""] * (len(header) - len(r))
        url_val = r[0]
        if not url_val:
            deduped.append(r)
            continue
        if url_val in seen:
            continue
        seen.add(url_val)
        deduped.append(r)

    print(
        f"[{cluster_name}] dedupe_done: before={len(all_rows)}, after={len(deduped)}"
    )
    write_log(
        service,
        "dedupe_done",
        cluster_name,
        f"before={len(all_rows)} after={len(deduped)}",
    )

    # 5.5. Нормализуем profile_followers (E) во всех строках
    try:
        followers_idx = header.index("profile_followers")
    except ValueError:
        followers_idx = None

    if followers_idx is not None:
        for r in deduped:
            if len(r) <= followers_idx:
                r += [""] * (followers_idx + 1 - len(r))
            r[followers_idx] = normalize_followers(r[followers_idx])

    # сохраняем таблицу (Новые строки уже в Google Sheets, без GPT)
    save_data_sheet(service, header, deduped)
    total_rows = len(deduped) + 1  # + шапка

    # протягиваем формулы и форматируем колонку E
    extend_formulas_hij(service, total_rows)
    format_column_e_numbers(service, total_rows)

    # 6. GPT-проход по незаполненным строкам
    label_unlabeled_rows(service, settings, cluster_name)

    write_log(
        service,
        "cluster_done",
        cluster_name,
        f"rows_total={len(deduped)}",
    )
    print(f"[{cluster_name}] кластер полностью обработан.")


# ---------- основной запуск: один прогон по всем кластерам ----------

def run_once():
    service = get_sheets_service()
    write_log(service, "run_start", "", f"version={BOT_VERSION}")

    settings = load_settings(service)
    clusters = load_clusters(service)

    active_clusters = [
        (name, data)
        for name, data in clusters.items()
        if data["active"]
    ]

    if not active_clusters:
        print("Нет активных кластеров.")
        write_log(service, "no_active_clusters", "", "run_once")
        return

    active_clusters.sort(key=lambda x: x[1]["order"])
    cluster_names = [name for name, _ in active_clusters]

    print("\nПорядок кластеров в этом запуске:")
    print(" -> ".join(cluster_names))

    write_log(
        service,
        "cluster_sequence",
        "",
        " -> ".join(cluster_names),
    )

    for cluster_name, cluster_data in active_clusters:
        try:
            process_cluster(service, settings, cluster_name, cluster_data)
            update_setting(service, "last_cluster_name", cluster_name)
        except Exception as e:
            print(
                "Ошибка при обработке кластера",
                cluster_name,
                ":",
                repr(e),
            )
            write_log(
                service,
                "cluster_error",
                cluster_name,
                repr(e),
            )

    write_log(
        service,
        "run_done",
        "",
        f"clusters={len(cluster_names)}",
    )
    print("Запуск завершён. Обработано кластеров:", len(cluster_names))


if __name__ == "__main__":
    run_once()
