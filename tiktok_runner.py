import time
import json
import requests
from datetime import datetime

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

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
DEFAULT_NUM_OF_POSTS = _int_from_config("DEFAULT_NUM_OF_POSTS", 3000)
# базовый максимум постов на кластер (можно переопределить в Settings)
BASE_MAX_POSTS_PER_CLUSTER = _int_from_config("MAX_POSTS_PER_CLUSTER", 3000)

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
SHEET_US_BASED = "US_Based"

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

BOT_VERSION = "2025-11-25_manual_run_v4"

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
    """Используем, например, для last_cluster_name."""
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
        # если хоть одна строка активна — считаем кластер активным
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
    чтобы числа (в т.ч. profile_followers) стали именно числами, а не текстом.

    Используется для "жёсткой" перезаписи таблицы (дедуп, новые посты и т.п.).
    GPT-прогресс теперь сохраняется отдельной функцией, чтобы не трогать весь лист.
    """
    sheet = service.spreadsheets()
    # выравниваем строки
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
        valueInputOption="USER_ENTERED",  # ключевая штука
        body={"values": [header] + norm_rows},
    ).execute()


# --- новый helper: сохраняем только GPT-колонку (без clear всего листа) ---

def _idx_to_col_letter(idx: int) -> str:
    """
    Превращает 0-based индекс колонки (0=A,1=B,...) в буквы Excel/Sheets (A,B,...,Z,AA,...).
    """
    n = idx
    s = ""
    while True:
        n, r = divmod(n, 26)
        s = chr(ord("A") + r) + s
        if n == 0:
            break
        n -= 1
    return s


def save_gpt_labels_only(service, header, rows, label_column):
    """
    Обновляет в листе TikTok_Posts только одну колонку с GPT-метками
    (например, gpt_flag) без очистки всего диапазона A:H.

    Используется при пошаговой разметке (каждые 10 строк) и в режиме gpt_only.
    """
    try:
        label_idx = header.index(label_column)
    except ValueError:
        print("save_gpt_labels_only: колонка не найдена:", label_column)
        return

    col_letter = _idx_to_col_letter(label_idx)

    # только значения по этой колонке (строки 2..N, без заголовка)
    col_values = []
    for r in rows:
        if len(r) <= label_idx:
            r = r + [""] * (label_idx + 1 - len(r))
        col_values.append([r[label_idx]])

    sheet = service.spreadsheets()
    try:
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_DATA}!{col_letter}2:{col_letter}{len(rows) + 1}",
            valueInputOption="USER_ENTERED",
            body={"values": col_values},
        ).execute()
    except Exception as e:
        print("save_gpt_labels_only error:", repr(e))


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
    Понимает строки вида '1,234', '12.3K', '4.5M', '1000'.
    Если не получилось — возвращает исходное значение.
    """
    if val is None:
        return ""

    if isinstance(val, (int, float)):
        return int(val)

    s = str(val).strip()
    if not s:
        return ""

    # удалим пробелы
    s = s.replace(" ", "")

    # суффиксы k/m/b
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

    # убираем разделители тысяч
    cleaned = "".join(ch for ch in lower if ch.isdigit() or ch == ".")

    if not cleaned:
        return val

    try:
        num = float(cleaned)
        return int(round(num * multiplier))
    except Exception:
        return val


# ---------- GPT: бинарный Y/N ----------

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
        "model": "gpt-5-mini",
        "messages": [
            {
                "role": "system",
                "content": "Ты бинарный классификатор. Отвечай только 'Y' или 'N'.",
            },
            {"role": "user", "content": user_content},
        ],
        "max_completion_tokens": 16,
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


# ---------- GPT: категории 1–5 для US_Based ----------

def call_gpt_category_5(prompt_base, text):
    """
    GPT-классификация по 5 категориям.
    Возвращает строку '1', '2', '3', '4' или '5'.
    При ошибке/непонятке — '3' (Неизвестно).
    """
    if not OPENAI_API_KEY:
        return "3"

    text = (text or "").strip()
    if not text:
        return "3"

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    user_content = prompt_base.strip() + "\n\nТекст:\n" + text

    payload = {
        "model": "gpt-5-mini",
        "messages": [
            {
                "role": "system",
                "content": "Ты классификатор. Отвечай только одной цифрой: 1, 2, 3, 4 или 5.",
            },
            {"role": "user", "content": user_content},
        ],
        "max_completion_tokens": 16,
    }

    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=60,
        )
    except Exception as e:
        print("GPT request error (categories):", e)
        return "3"

    if resp.status_code != 200:
        print("GPT HTTP error (categories):", resp.status_code, resp.text[:200])
        return "3"

    try:
        data = resp.json()
        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )
    except Exception as e:
        print("GPT parse error (categories):", e)
        return "3"

    if not content:
        return "3"

    ch = content.strip()[0]
    if ch in ("1", "2", "3", "4", "5"):
        return ch
    return "3"


# ---------- GPT массовая разметка TikTok_Posts ----------

def apply_gpt_labels(
    service,
    cluster_name,
    header,
    rows,
    target_column,
    label_column,
    prompt_base,
    log_every=10,
    continue_from_last_nonempty=True,
):
    """
    Добавляет Y/N в label_column.

    НОВОЕ:
    - если continue_from_last_nonempty=True, то начинаем разметку
      с первой строки ПОСЛЕ последней строки, где label_column не пустая.
      (для TikTok_Posts это H = gpt_flag)

    Каждые log_every строк сохраняем только колонку label_column в Google Sheets.
    """
    try:
        text_idx = header.index(target_column)
        label_idx = header.index(label_column)
    except ValueError:
        print("GPT: не найдена колонка", target_column, "или", label_column)
        return rows, 0

    # выравниваем строки до длины шапки
    for i, r in enumerate(rows):
        if len(r) < len(header):
            rows[i] = r + [""] * (len(header) - len(r))

    # определяем стартовый индекс
    start_index = 0
    if continue_from_last_nonempty and rows:
        last_nonempty = None
        for idx in range(len(rows) - 1, -1, -1):
            val = (rows[idx][label_idx] or "").strip()
            if val:
                last_nonempty = idx
                break
        if last_nonempty is not None:
            start_index = last_nonempty + 1

    if start_index >= len(rows):
        msg = f"nothing_to_process start_index={start_index}, total_rows={len(rows)}"
        print(f"[GPT][{cluster_name or 'ALL'}] {msg}")
        write_log(service, "gpt_progress", cluster_name or "ALL", msg)
        return rows, 0

    # считаем, сколько реально нужно разметить начиная с start_index
    total_to_process = 0
    for r in rows[start_index:]:
        label = (r[label_idx] or "").strip().upper()
        if label not in ("Y", "N"):
            total_to_process += 1

    if total_to_process == 0:
        msg = f"nothing_to_process_after_index start_row={start_index + 2}"
        print(f"[GPT][{cluster_name or 'ALL'}] {msg}")
        write_log(service, "gpt_progress", cluster_name or "ALL", msg)
        return rows, 0

    print(
        f"[GPT] Старт разметки ({cluster_name or 'ALL'}). "
        f"Стартовая строка листа: {start_index + 2}, всего к обработке: {total_to_process}"
    )

    processed = 0

    for row_idx in range(start_index, len(rows)):
        r = rows[row_idx]
        label = (r[label_idx] or "").strip().upper()
        if label in ("Y", "N"):
            # вдруг кто-то уже руками проставил
            continue

        text = r[text_idx]
        yn = call_gpt_label(prompt_base, text)
        r[label_idx] = yn
        processed += 1

        if log_every and processed % log_every == 0:
            msg = f"processed={processed}/{total_to_process}"
            print(f"[GPT][{cluster_name or 'ALL'}] {msg}")
            try:
                save_gpt_labels_only(service, header, rows, label_column)
            except Exception as e:
                print("[GPT] error while saving partial GPT labels:", repr(e))

    # финальный лог + проверка, всё ли разметили в хвосте
    missing_count = 0
    first_missing_row = None
    for row_idx in range(start_index, len(rows)):
        r = rows[row_idx]
        label = (r[label_idx] or "").strip().upper()
        if label not in ("Y", "N"):
            missing_count += 1
            if first_missing_row is None:
                first_missing_row = row_idx + 2  # +1 за заголовок, +1 за индекс

    final_msg = f"processed={processed}/{total_to_process} (final)"
    write_log(
        service,
        "gpt_progress",
        cluster_name or "ALL",
        final_msg,
    )
    print(f"[GPT][{cluster_name or 'ALL'}] {final_msg}")

    if missing_count:
        msg = (
            f"rows_without_labels_after_gpt={missing_count}, "
            f"first_row={first_missing_row}"
        )
        print("GPT incomplete:", msg)
        write_log(service, "gpt_incomplete", cluster_name or "ALL", msg)

    return rows, processed


# ---------- Bright Data ----------

def start_scrape_for_urls(urls, limit_per_input=None, total_limit=None):
    """
    Запускает асинхронный сбор в Bright Data по списку URLs
    через /datasets/v3/trigger и возвращает snapshot_id.

    limit_per_input -> query param limit_per_input (лимит на один input)
    total_limit     -> query param limit_multiple_results (общий лимит результатов)
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

    # входные данные для скрейпера
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

    # всегда async
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
    headers = {"Authorization": f"{'Bearer ' + BRIGHTDATA_API_KEY}"}

    waited = 0
    while True:
        resp = requests.get(url, headers=headers, timeout=300)

        if resp.status_code == 200:
            data = resp.json()
            if not isinstance(data, list):
                raise RuntimeError("Expected JSON array from snapshot")
            return data

        if resp.status_code == 202:
            # снапшот ещё собирается
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

        # другая ошибка
        raise RuntimeError(f"Download error: {resp.status_code} {resp.text[:200]}")


# ---------- пост-обработка листа: формулы и формат чисел ----------

def extend_formulas_hij(service, last_row):
    """
    Копирует формулы из H2:J2 на H2:J{last_row}
    (как будто ты протянул формулы вниз).
    Если в H/I/J формул нет, PASTE_FORMULA ничего не меняет.
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


def extend_us_based_verdict_formulas(service, last_data_row, last_formula_row):
    """
    Протягивает формулу Verdict (колонка G) с last_formula_row до last_data_row.
    last_* — 1-based номера строк в листе US_Based.
    """
    if not last_formula_row or last_formula_row < 2:
        return
    if last_data_row <= last_formula_row:
        return

    try:
        sheet_id = get_sheet_id(service, SHEET_US_BASED)
    except Exception as e:
        print("extend_us_based_verdict_formulas get_sheet_id error:", repr(e))
        return

    # 0-based индекс строки
    src_row_index = last_formula_row - 1

    # G = 6 (0-based: A=0,B=1,C=2,D=3,E=4,F=5,G=6)
    requests_body = {
        "requests": [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": sheet_id,
                        "startRowIndex": src_row_index,
                        "endRowIndex": src_row_index + 1,
                        "startColumnIndex": 6,  # G
                        "endColumnIndex": 7,
                    },
                    "destination": {
                        "sheetId": sheet_id,
                        "startRowIndex": src_row_index,
                        "endRowIndex": last_data_row,
                        "startColumnIndex": 6,
                        "endColumnIndex": 7,
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
        print("extend_us_based_verdict_formulas error:", repr(e))


# ---------- обработка одного кластера ----------

def process_cluster(service, settings, cluster_name, cluster_data, with_gpt=True):
    """
    Полный цикл по одному кластеру:
    Bright Data -> запись в таблицу -> дедуп -> (опционально) GPT-разметка.
    """
    urls = cluster_data["urls"]
    wait_bright_min = int(settings.get("wait_bright_min", "20"))
    gpt_target_column = settings.get("gpt_target_column", "profile_biography")
    gpt_label_column = settings.get("gpt_label_column", "gpt_flag")
    gpt_prompt = settings.get(
        "gpt_prompt",
        "Считай Y, если текст связан с личными финансами, деньгами или расходами пользователя. Иначе N.",
    )
    # интервал опроса статуса Bright Data (по умолчанию 1 сек)
    status_poll_sec = int(settings.get("status_poll_sec", "1"))

    # лимит постов на кластер: Settings -> max_posts_per_cluster, потом config
    cluster_limit_raw = settings.get("max_posts_per_cluster", None)
    try:
        cluster_limit = int(cluster_limit_raw) if cluster_limit_raw else BASE_MAX_POSTS_PER_CLUSTER
    except Exception:
        cluster_limit = BASE_MAX_POSTS_PER_CLUSTER

    # Bright Data: лимиты на стороне API
    bright_limit_per_input_raw = settings.get("bright_limit_per_input", "").strip()
    bright_total_limit_raw = settings.get("bright_total_limit", "").strip()

    try:
        bright_limit_per_input = int(bright_limit_per_input_raw) if bright_limit_per_input_raw else DEFAULT_NUM_OF_POSTS
    except Exception:
        bright_limit_per_input = DEFAULT_NUM_OF_POSTS

    try:
        # общий лимит по умолчанию = лимит на кластер
        bright_total_limit = int(bright_total_limit_raw) if bright_total_limit_raw else cluster_limit
    except Exception:
        bright_total_limit = cluster_limit

    # как часто печатать прогресс GPT (только в консоль)
    gpt_log_every_raw = settings.get("gpt_log_every", "10")
    try:
        gpt_log_every = max(1, int(gpt_log_every_raw))
    except Exception:
        gpt_log_every = 10

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

    # ждём, пока progress станет ready
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

    # скачиваем снапшот
    posts = download_snapshot(
        snapshot_id,
        max_wait_sec=wait_bright_min * 60,
        poll_sec=poll_sec,
    )

    # 2. Если постов нет — выходим
    if not posts:
        print(f"[{cluster_name}] Постов нет.")
        write_log(service, "no_posts", cluster_name, "0 posts")
        return

    original_posts_len = len(posts)
    # ограничиваем количество постов на кластер уже локально (на всякий случай)
    if cluster_limit > 0 and original_posts_len > cluster_limit:
        posts = posts[:cluster_limit]
    used_posts_len = len(posts)

    write_log(
        service,
        "snapshot_downloaded",
        cluster_name,
        f"posts_original={original_posts_len} posts_used={used_posts_len} "
        f"cluster_limit={cluster_limit} bright_total_limit={bright_total_limit}",
    )
    print(f"[{cluster_name}] Snapshot downloaded: original={original_posts_len}, used={used_posts_len}")

    # 3. Формируем batch-метку
    batch_label = (
        datetime.now().strftime("%Y-%m-%d %H:%M")
        + f" | {COMMAND_NAME} | {cluster_name}"
    )
    print("batch_label:", batch_label)

    # 4. Работа с таблицей
    ensure_data_header(service)
    header, old_rows = load_data_sheet(service)
    if not header:
        header = HEADER

    old_count = len(old_rows)

    # новые строки
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
    write_log(
        service,
        "rows_appended",
        cluster_name,
        f"old={old_count} new={len(new_rows)} total={len(all_rows)}",
    )
    print(f"[{cluster_name}] rows_appended: old={old_count}, new={len(new_rows)}, total={len(all_rows)}")

    # 5. Глобальный дедуп по url (во всей таблице)
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

    write_log(
        service,
        "dedupe_done",
        cluster_name,
        f"before={len(all_rows)} after={len(deduped)}",
    )
    print(f"[{cluster_name}] dedupe_done: before={len(all_rows)}, after={len(deduped)}")

    # 5.5. Нормализуем profile_followers (E) во ВСЕХ строках
    try:
        followers_idx = header.index("profile_followers")
    except ValueError:
        followers_idx = None

    if followers_idx is not None:
        for r in deduped:
            if len(r) <= followers_idx:
                r += [""] * (followers_idx + 1 - len(r))
            r[followers_idx] = normalize_followers(r[followers_idx])

    # 6. GPT-разметка (опционально)
    if with_gpt:
        deduped, gpt_count = apply_gpt_labels(
            service,
            cluster_name,
            header,
            deduped,
            gpt_target_column,
            gpt_label_column,
            gpt_prompt,
            log_every=gpt_log_every,
        )
        write_log(service, "gpt_done", cluster_name, f"processed={gpt_count}")
        print(f"[{cluster_name}] GPT done, processed={gpt_count}")
        # Раньше здесь была строгая проверка, что в колонке gpt_flag нет пустых значений.
        # Теперь GPT размечает данные инкрементально, начиная с последней непустой строки,
        # поэтому допускаем незаполненные строки выше этой границы.

    # 7. Сохраняем в Google Sheets (здесь по-прежнему перезаписываем весь диапазон A:H, это логично после дедупа)
    save_data_sheet(service, header, deduped)
    total_rows = len(deduped) + 1  # + хедер

    # 8. Протягиваем формулы H–J и форматируем колонку E как числа
    extend_formulas_hij(service, total_rows)
    format_column_e_numbers(service, total_rows)

    write_log(
        service,
        "cluster_done",
        cluster_name,
        f"rows_total={len(deduped)}",
    )
    print(f"[{cluster_name}] cluster_done, rows_total={len(deduped)}")


# ---------- GPT по основной таблице ----------

def _run_over_active_clusters(service, settings, with_gpt=True, run_label="run"):
    clusters = load_clusters(service)

    active_clusters = [
        (name, data)
        for name, data in clusters.items()
        if data["active"]
    ]

    if not active_clusters:
        print("Нет активных кластеров.")
        write_log(service, "no_active_clusters", "", run_label)
        return

    # сортируем по order и проходим ВСЕ активные кластеры с 1 до последнего
    active_clusters.sort(key=lambda x: x[1]["order"])
    cluster_names = [name for name, _ in active_clusters]

    print(f"\nПорядок кластеров в этом запуске ({run_label}):")
    print(" -> ".join(cluster_names))

    write_log(
        service,
        "cluster_sequence",
        "",
        " -> ".join(cluster_names),
    )

    for cluster_name, cluster_data in active_clusters:
        try:
            process_cluster(service, settings, cluster_name, cluster_data, with_gpt=with_gpt)
            # purely информативно — можно смотреть в Settings
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
            # идём дальше к следующему кластеру, но твёрдо логируем проблему

    write_log(
        service,
        f"{run_label}_done",
        "",
        f"clusters={len(cluster_names)}",
    )
    print(f"{run_label} завершён. Обработано кластеров:", len(cluster_names))


def run_once():
    """Полный режим: кластеры (Bright Data) + GPT по ходу."""
    service = get_sheets_service()
    write_log(service, "run_start", "", f"version={BOT_VERSION}")
    print(f"[RUN] Старт полного прогона кластеров. Версия: {BOT_VERSION}")

    settings = load_settings(service)
    _run_over_active_clusters(service, settings, with_gpt=True, run_label="run")


def run_scrape_only():
    """Только Bright Data + запись в таблицу + формулы/формат. Без GPT."""
    service = get_sheets_service()
    write_log(service, "scrape_start", "", f"version={BOT_VERSION}")
    print(f"[SCRAPE_ONLY] Старт. Версия: {BOT_VERSION}")

    settings = load_settings(service)
    _run_over_active_clusters(service, settings, with_gpt=False, run_label="scrape")


def run_gpt_only(overwrite=False):
    """
    Режим: только GPT.
    Берёт ВСЕ строки из TikTok_Posts и размечает колонку gpt_flag.
    Если overwrite=True — сначала очищает колонку gpt_flag и размечает заново.
    """
    service = get_sheets_service()
    settings = load_settings(service)

    gpt_target_column = settings.get("gpt_target_column", "profile_biography")
    gpt_label_column = settings.get("gpt_label_column", "gpt_flag")
    gpt_prompt = settings.get(
        "gpt_prompt",
        "Считай Y, если текст связан с личными финансами, деньгами или расходами пользователя. Иначе N.",
    )

    header, rows = load_data_sheet(service)
    if not header or not rows:
        print("[GPT_ONLY] Лист TikTok_Posts пуст или без заголовка.")
        return

    print(f"[GPT_ONLY] Всего строк в TikTok_Posts: {len(rows)}")
    print(f"[GPT_ONLY] Целевая колонка: {gpt_target_column}, колонка флага: {gpt_label_column}")

    # по желанию можно снести старые флаги
    if overwrite:
        try:
            label_idx = header.index(gpt_label_column)
            for i, r in enumerate(rows):
                if len(r) <= label_idx:
                    rows[i] = r + [""] * (label_idx + 1 - len(r))
                rows[i][label_idx] = ""
            print("[GPT_ONLY] Все gpt_flag очищены, размечаем с нуля.")
        except ValueError:
            print("[GPT_ONLY] Колонка gpt_flag не найдена, пропускаем очистку.")

    rows, processed = apply_gpt_labels(
        service,
        cluster_name="GPT_ONLY",
        header=header,
        rows=rows,
        target_column=gpt_target_column,
        label_column=gpt_label_column,
        prompt_base=gpt_prompt,
        log_every=10,
    )

    # финальный сброс: сохраняем только колонку с метками, без clear и без перезаписи всего листа
    save_gpt_labels_only(service, header, rows, gpt_label_column)
    print(f"[GPT_ONLY] Готово. GPT обработал строк: {processed}")


# ---------- режим для вкладки US_Based ----------

def run_us_based():
    """
    Режим: анализ листа US_Based.

    Ожидаем структуру (начиная с колонки B):
    B: URL
    C: BIO
    D: Subscribers
    E: US_flag (Y/N)
    F: US_category (1..5)
    G: Verdict (формула, которую надо протягивать)

    НОВОЕ:
    - GPT продолжает работу СТРОГО с первой строки после последней строки,
      где и E, и F уже заполнены (не пустые).
    """
    service = get_sheets_service()
    settings = load_settings(service)
    sheet = service.spreadsheets()

    # Промпт для US_flag (Y/N)
    default_us_flag_prompt = (
        "Return 'N' if text clearly indicates that the creator is not from the US "
        "(for example: UK, India, Philippines, Canada, Europe, Africa, Asia, etc). "
        "In all other cases, including when the country is not mentioned or not obvious, return 'Y'. "
        "Answer with a single letter: Y or N."
    )
    us_flag_prompt = settings.get("us_based_gpt_prompt", default_us_flag_prompt)

    # Промпт для категорий 1–5
    default_categories_prompt = (
        "Analyze the text (TikTok bio) and assign exactly one label from the list below. "
        "Output only the label number (1, 2, 3, 4, or 5).\n"
        "1 - Individual creator related to finance, AI, or personal productivity.\n"
        "2 - Individual creator NOT related to finance, AI, or personal productivity.\n"
        "3 - Unknown (not enough information to tell).\n"
        "4 - Business account (brand, company, service, etc.).\n"
        "5 - News account, non-English description, or clearly non-US geo."
    )
    categories_prompt = settings.get("us_based_categories_prompt", default_categories_prompt)

    # Берём B1:G — 6 колонок: URL, BIO, Subscribers, US_flag, US_category, Verdict
    resp = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_US_BASED}!B1:G",
    ).execute()
    values = resp.get("values", [])

    if not values or len(values) <= 1:
        print("[US_BASED] Лист пуст или содержит только заголовок.")
        write_log(service, "us_based_empty", SHEET_US_BASED, "no data")
        return

    header = values[0]
    rows = values[1:]

    # гарантируем минимум 6 колонок в шапке
    header_updated = False
    if len(header) < 6:
        header = header + [""] * (6 - len(header))
        header_updated = True

    # индексы относительно B:
    BIO_COL = 1       # C
    US_FLAG_COL = 3   # E
    US_CAT_COL = 4    # F
    VERDICT_COL = 5   # G

    # гарантируем имена колонок E/F
    if not str(header[US_FLAG_COL]).strip():
        header[US_FLAG_COL] = "US_flag"
        header_updated = True
    if not str(header[US_CAT_COL]).strip():
        header[US_CAT_COL] = "US_category"
        header_updated = True

    if header_updated:
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_US_BASED}!B1:G1",
            valueInputOption="USER_ENTERED",
            body={"values": [header]},
        ).execute()

    # выравниваем строки до длины шапки
    max_cols = len(header)
    for i, r in enumerate(rows):
        if len(r) < max_cols:
            rows[i] = r + [""] * (max_cols - len(r))

    # находим последнюю непустую строку в Verdict (G)
    last_verdict_row = None  # 1-based номер строки листа
    for idx, r in enumerate(rows, start=2):  # строки 2..N
        if len(r) > VERDICT_COL and str(r[VERDICT_COL]).strip():
            last_verdict_row = idx

    # Ищем последнюю строку, где и E, и F не пустые
    start_index = 0  # индекс в массиве rows (0 = строка 2 в листе)
    if rows:
        last_full_labeled = None
        for idx in range(len(rows) - 1, -1, -1):
            flag_val = (rows[idx][US_FLAG_COL] or "").strip()
            cat_val = (rows[idx][US_CAT_COL] or "").strip()
            if flag_val and cat_val:
                last_full_labeled = idx
                break
        if last_full_labeled is not None:
            start_index = last_full_labeled + 1  # начинаем с следующей строки

    # считаем, сколько нужно обработать, начиная с start_index
    total_to_process = 0
    for r in rows[start_index:]:
        flag_val = (r[US_FLAG_COL] or "").strip().upper()
        cat_val = str(r[US_CAT_COL]).strip() if r[US_CAT_COL] is not None else ""
        if flag_val not in ("Y", "N") or cat_val not in ("1", "2", "3", "4", "5"):
            total_to_process += 1

    print(
        f"[US_BASED] Всего строк: {len(rows)}, "
        f"стартуем с строки листа {start_index + 2}, к обработке: {total_to_process}"
    )
    write_log(
        service,
        "us_based_start",
        SHEET_US_BASED,
        f"rows={len(rows)} to_process={total_to_process} start_row={start_index + 2}",
    )

    if total_to_process == 0:
        print("[US_BASED] Все строки после стартовой уже размечены по US_flag и US_category.")
        if last_verdict_row:
            extend_us_based_verdict_formulas(
                service,
                last_data_row=len(rows) + 1,
                last_formula_row=last_verdict_row,
            )
        write_log(service, "us_based_nothing", SHEET_US_BASED, "all labeled")
        return

    processed = 0

    for row_idx in range(start_index, len(rows)):
        r = rows[row_idx]

        flag_val = (r[US_FLAG_COL] or "").strip().upper()
        cat_val = str(r[US_CAT_COL]).strip() if r[US_CAT_COL] is not None else ""

        need_flag = flag_val not in ("Y", "N")
        need_cat = cat_val not in ("1", "2", "3", "4", "5")

        if not (need_flag or need_cat):
            continue

        bio = ""
        if BIO_COL < len(r) and r[BIO_COL] is not None:
            bio = str(r[BIO_COL]).strip()

        if not bio:
            # пустое BIO: считаем, что страна не указана, тип неизвестен
            if need_flag:
                r[US_FLAG_COL] = "Y"
            if need_cat:
                r[US_CAT_COL] = "3"
        else:
            if need_flag:
                yn = call_gpt_label(us_flag_prompt, bio)
                if yn not in ("Y", "N"):
                    yn = "Y"
                r[US_FLAG_COL] = yn
            if need_cat:
                cat = call_gpt_category_5(categories_prompt, bio)
                if cat not in ("1", "2", "3", "4", "5"):
                    cat = "3"
                r[US_CAT_COL] = cat

        processed += 1

        if processed % 10 == 0 or processed == total_to_process:
            print(f"[US_BASED] processed={processed}/{total_to_process}")
            ef_values = [[row[US_FLAG_COL], row[US_CAT_COL]] for row in rows]
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_US_BASED}!E2:F{len(rows) + 1}",
                valueInputOption="USER_ENTERED",
                body={"values": ef_values},
            ).execute()

    # финальный сброс E/F
    ef_values = [[r[US_FLAG_COL], r[US_CAT_COL]] for r in rows]
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_US_BASED}!E2:F{len(rows) + 1}",
        valueInputOption="USER_ENTERED",
        body={"values": ef_values},
    ).execute()

    # протягиваем Verdict (G) от последней непустой строки до конца данных
    if last_verdict_row:
        extend_us_based_verdict_formulas(
            service,
            last_data_row=len(rows) + 1,  # +1 за заголовок
            last_formula_row=last_verdict_row,
        )

    write_log(
        service,
        "us_based_done",
        SHEET_US_BASED,
        f"processed={processed}/{total_to_process}",
    )
    print(f"[US_BASED] Готово. GPT обработал строк: {processed} из {total_to_process}")


# ---------- точка входа ----------

if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "full"

    if mode == "gpt_only":
        # только GPT по основной таблице TikTok_Posts
        run_gpt_only(overwrite=False)
    elif mode == "scrape_only":
        # только выгрузка Bright Data + запись в таблицу
        run_scrape_only()
    elif mode == "start":
        # режим для вкладки US_Based (двойной GPT + протяжка Verdict)
        run_us_based()
    else:
        # полный цикл: Bright Data + GPT по кластерам
        run_once()
