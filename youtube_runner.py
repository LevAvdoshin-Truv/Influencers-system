import time
import json
import requests
from datetime import datetime

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# --- читаем конфиг ---
with open("config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)


def _int_from_config(key, default):
    val = CONFIG.get(key, default)
    try:
        return int(val)
    except Exception:
        return int(default)


BRIGHTDATA_API_KEY = CONFIG["BRIGHTDATA_API_KEY"]
YOUTUBE_DATASET_ID = CONFIG["YOUTUBE_DATASET_ID"]
YOUTUBE_COLLECT_DATASET_ID = CONFIG.get("YOUTUBE_COLLECT_DATASET_ID") or YOUTUBE_DATASET_ID
DEFAULT_NUM_OF_POSTS = _int_from_config("YOUTUBE_DEFAULT_NUM_OF_POSTS", 50)
BASE_MAX_POSTS_PER_CLUSTER = _int_from_config("YOUTUBE_MAX_POSTS_PER_CLUSTER", 1000)

SPREADSHEET_ID = CONFIG["SPREADSHEET_ID"]
SERVICE_ACCOUNT_FILE = CONFIG["SERVICE_ACCOUNT_FILE"]
COMMAND_NAME = CONFIG.get("YOUTUBE_COMMAND_NAME", "YouTube")

OPENAI_API_KEY = CONFIG.get("OPENAI_API_KEY", "")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# имена листов (те же, что использует TikTok-бот)
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

BOT_VERSION = "2025-12-06_youtube_v1"

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
        return
    _last_log_key = key

    row = [[ts, action_text, cluster_text, details_text]]

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
    if not values:
        return settings
    for row in values[1:]:
        if len(row) >= 2:
            key = (row[0] or "").strip()
            value = (row[1] or "").strip()
            if key:
                settings[key] = value
    return settings


def update_setting(service, key, new_value):
    sheet = service.spreadsheets()
    resp = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_SETTINGS}!A:B",
    ).execute()
    values = resp.get("values", [])
    if not values:
        values = [["key", "value"]]

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

def load_youtube_clusters(service):
    """
    Читает лист Clusters и возвращает активные строки с platform=YouTube*.
    Формат строк: cluster_name | active | order | value | platform
    platform:
        youtube / youtube_collect  — сбор по URL (dataset_id=YOUTUBE_COLLECT_DATASET_ID)
        youtube_discover / youtube_keyword — сбор по keyword (dataset_id=YOUTUBE_DATASET_ID)
    """
    sheet = service.spreadsheets()
    resp = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_CLUSTERS}!A:E",
    ).execute()
    values = resp.get("values", [])
    if len(values) <= 1:
        return {}

    rows = values[1:]
    clusters = {}
    for row in rows:
        if len(row) < 4:
            continue
        name = (row[0] or "").strip()
        if not name:
            continue
        platform = (row[4] if len(row) >= 5 else "").strip().lower()
        if platform and not platform.startswith("youtube"):
            continue
        active_flag = (row[1] or "").strip().upper() == "Y"
        try:
            order = int(row[2])
        except Exception:
            continue
        value = (row[3] or "").strip()
        if not value:
            continue

        mode = "collect"
        if platform in ("youtube_discover", "youtube_keyword"):
            mode = "keyword"

        if name not in clusters:
            clusters[name] = {"order": order, "active": active_flag, "items": [], "mode": mode}
        clusters[name]["items"].append(value)
        if active_flag:
            clusters[name]["active"] = True

    return clusters


# ---------- TikTok Posts чтение/запись (используем тот же лист) ----------

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


def save_gpt_labels_only(service, header, rows, label_column):
    try:
        label_idx = header.index(label_column)
    except ValueError:
        print("save_gpt_labels_only: колонка не найдена:", label_column)
        return

    col_letter = _idx_to_col_letter(label_idx)
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


# ---------- утилиты ----------

def _idx_to_col_letter(idx: int) -> str:
    n = idx
    s = ""
    while True:
        n, r = divmod(n, 26)
        s = chr(ord("A") + r) + s
        if n == 0:
            break
        n -= 1
    return s


def normalize_followers(val):
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


def extract_video_url(post):
    """
    Пытаемся вытащить URL видео из разных полей ответа Bright Data по YouTube.
    """
    candidates = [
        post.get("url"),
        post.get("video_url"),
        post.get("link"),
    ]
    for c in candidates:
        if not c:
            continue
        c_str = str(c).strip()
        if c_str:
            return c_str
    return ""


# ---------- GPT ----------

def call_gpt_label(prompt_base, text):
    if not OPENAI_API_KEY:
        return "No API Access"
    if text is None:
        text = ""
    else:
        text = str(text)

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
                "content": "Ты классификатор. Отвечай КРАТКО и строго согласно промпту пользователя.",
            },
            {"role": "user", "content": user_content},
        ],
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
        return ""

    if resp.status_code != 200:
        if resp.status_code == 401:
            print("GPT HTTP error 401: key rejected, marking as No API Access")
            return "No API Access"
        print("GPT HTTP error:", resp.status_code, resp.text[:200])
        return ""

    try:
        data = resp.json()
        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
        )
        return (content or "").strip()
    except Exception as e:
        print("GPT parse error:", e)
        return ""


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
    try:
        text_idx = header.index(target_column)
        label_idx = header.index(label_column)
    except ValueError:
        print("GPT: не найдена колонка", target_column, "или", label_column)
        return rows, 0

    for i, r in enumerate(rows):
        if len(r) < len(header):
            rows[i] = r + [""] * (len(header) - len(r))
        elif len(r) > len(header):
            rows[i] = r[: len(header)]

    total_to_process = 0
    for r in rows:
        label = (r[label_idx] or "").strip()
        if not label:
            total_to_process += 1

    if total_to_process == 0:
        msg = "nothing_to_process: все метки уже заполнены"
        print(f"[GPT][{cluster_name or 'ALL'}] {msg}")
        write_log(service, "gpt_progress", cluster_name or "ALL", msg)
        return rows, 0

    print(
        f"[GPT] Старт разметки ({cluster_name or 'ALL'}). "
        f"Всего к обработке строк (label пустой): {total_to_process}"
    )

    processed = 0

    for row_idx, r in enumerate(rows):
        current_label = (r[label_idx] or "").strip()
        if current_label:
            continue

        text = r[text_idx] if text_idx < len(r) else ""
        gpt_answer = call_gpt_label(prompt_base, text)

        if gpt_answer != "":
            r[label_idx] = gpt_answer

        processed += 1

        try:
            save_gpt_labels_only(service, header, rows, label_column)
        except Exception as e:
            print("[GPT] error while saving partial GPT labels:", repr(e))

        if log_every and processed % log_every == 0:
            msg = f"processed={processed}/{total_to_process}"
            print(f"[GPT][{cluster_name or 'ALL'}] {msg}")

    final_msg = f"processed={processed}/{total_to_process} (final)"
    write_log(
        service,
        "gpt_progress",
        cluster_name or "ALL",
        final_msg,
    )
    print(f"[GPT][{cluster_name or 'ALL'}] {final_msg}")

    return rows, processed


# ---------- Bright Data ----------

def start_scrape_inputs(items, mode, limit_per_input=None, total_limit=None, country=None):
    """
    Запускает асинхронный сбор в Bright Data.
    mode:
        keyword — discover by keyword
        collect — collect by URL
    Возвращает snapshot_id.
    """
    base_url = "https://api.brightdata.com/datasets/v3/trigger"

    dataset_id = YOUTUBE_DATASET_ID if mode == "keyword" else YOUTUBE_COLLECT_DATASET_ID

    params = {
        "dataset_id": dataset_id,
        "include_errors": "true",
        "format": "json",
    }

    # для discover by keyword Bright Data требует type=discover_new & discover_by=keyword
    if mode == "keyword":
        params["type"] = "discover_new"
        params["discover_by"] = "keyword"

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

    inputs = []
    for it in items:
        if mode == "keyword":
            inputs.append({"keyword": it, "country": country or ""})
        else:
            inputs.append({"url": it, "country": country or ""})

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
    url = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
    headers = {"Authorization": f"Bearer {BRIGHTDATA_API_KEY}"}
    resp = requests.get(url, headers=headers, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Status error: {resp.status_code} {resp.text[:200]}")
    return resp.json().get("status", "")


def download_snapshot(snapshot_id, max_wait_sec=600, poll_sec=30):
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


# ---------- пост-обработка листа ----------

def extend_formulas_hij(service, last_row):
    if last_row < 2:
        return

    sheet_id = get_sheet_id(service, SHEET_DATA)

    requests_body = {
        "requests": [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": 2,
                        "startColumnIndex": 7,
                        "endColumnIndex": 10,
                    },
                    "destination": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": last_row,
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
    if last_row < 2:
        return

    sheet_id = get_sheet_id(service, SHEET_DATA)

    requests_body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": last_row,
                        "startColumnIndex": 4,
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


# ---------- обработка одного кластера ----------

def process_cluster(service, settings, cluster_name, cluster_data, with_gpt=True):
    items = cluster_data["items"]
    mode = cluster_data.get("mode", "collect")

    wait_bright_min = int(settings.get("wait_bright_min", "20"))
    youtube_country = settings.get("youtube_country", "US").strip()
    gpt_target_column = settings.get("gpt_target_column", "profile_biography")
    gpt_label_column = settings.get("gpt_label_column", "gpt_flag")
    gpt_prompt = settings.get(
        "gpt_prompt",
        "Only Y or N. If bio/description is fully in English or empty → Y. If it contains any non-English letters → N.",
    )
    status_poll_sec = int(settings.get("status_poll_sec", "1"))

    cluster_limit_raw = settings.get("max_posts_per_cluster", None)
    try:
        cluster_limit = int(cluster_limit_raw) if cluster_limit_raw else BASE_MAX_POSTS_PER_CLUSTER
    except Exception:
        cluster_limit = BASE_MAX_POSTS_PER_CLUSTER

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

    gpt_log_every_raw = settings.get("gpt_log_every", "10")
    try:
        gpt_log_every = max(1, int(gpt_log_every_raw))
    except Exception:
        gpt_log_every = 10

    print("\n================ Новый кластер (YouTube) ================")
    print("Кластер:", cluster_name, "записей:", len(items), "mode:", mode)
    write_log(
        service,
        "start_cluster",
        cluster_name,
        f"items={len(items)} | platform=YouTube | mode={mode}",
    )

    ensure_data_header(service)
    header, old_rows = load_data_sheet(service)
    if not header:
        header = HEADER

    norm_old_rows = []
    for r in old_rows:
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        elif len(r) > len(header):
            r = r[: len(header)]
        norm_old_rows.append(r)

    rows = list(norm_old_rows)
    old_count = len(rows)

    existing_urls = set()
    for r in rows:
        if not r:
            continue
        url_val = (r[0] or "").strip()
        if url_val:
            existing_urls.add(url_val)

    remaining_cluster = cluster_limit if cluster_limit > 0 else None
    total_appended = 0

    sheet = service.spreadsheets()

    for item_idx, item in enumerate(items, start=1):
        # пер-Input лог
        write_log(
            service,
            "start_input",
            cluster_name,
            f"item_idx={item_idx}/{len(items)} mode={mode} value={item[:80]}",
        )
        print(f"[{cluster_name}] Start input {item_idx}/{len(items)}: {item}")

        per_input_limit = bright_limit_per_input
        if remaining_cluster is not None:
            per_input_limit = min(per_input_limit, remaining_cluster)
            if per_input_limit <= 0:
                print(f"[{cluster_name}] Достигнут cluster_limit, пропускаем оставшиеся inputs")
                break

        result = start_scrape_inputs(
            [item],
            mode,
            limit_per_input=per_input_limit,
            total_limit=per_input_limit if per_input_limit else bright_total_limit,
            country=youtube_country,
        )
        snapshot_id = result["snapshot_id"]
        write_log(
            service,
            "bright_async_started",
            cluster_name,
            f"snapshot_id={snapshot_id} item_idx={item_idx}",
        )
        print("ASYNC, snapshot_id =", snapshot_id)

        poll_sec = status_poll_sec
        max_progress_wait = wait_bright_min * 60
        waited = 0

        last_status_logged = None
        while True:
            status = get_snapshot_status(snapshot_id)
            if status != last_status_logged:
                write_log(service, "snapshot_status", cluster_name, f"{status} item_idx={item_idx}")
                last_status_logged = status
            print(f"Статус снапшота: {status}, waited={waited} sec")

            if status == "ready":
                break
            if status in ("failed", "error", "canceled", "canceling"):
                print(
                    f"Снапшот завершился с ошибочным статусом ({status}). Пропускаем input."
                )
                write_log(
                    service,
                    "snapshot_failed",
                    cluster_name,
                    f"status={status} waited={waited} item_idx={item_idx}",
                )
                break

            if waited >= max_progress_wait:
                print("Таймаут ожидания статуса ready. Пропускаем input.")
                write_log(
                    service,
                    "snapshot_timeout_status",
                    cluster_name,
                    f"waited={waited} item_idx={item_idx}",
                )
                break

            time.sleep(poll_sec)
            waited += poll_sec

        posts = download_snapshot(
            snapshot_id,
            max_wait_sec=wait_bright_min * 60,
            poll_sec=poll_sec,
        )

        if not posts:
            print(f"[{cluster_name}] Постов нет для input {item_idx}.")
            write_log(service, "no_posts", cluster_name, f"item_idx={item_idx} 0 posts")
            continue

        if remaining_cluster is not None and remaining_cluster > 0 and len(posts) > remaining_cluster:
            posts = posts[:remaining_cluster]

        used_posts_len = len(posts)

        write_log(
            service,
            "snapshot_downloaded",
            cluster_name,
            f"item_idx={item_idx} posts_used={used_posts_len} per_input_limit={per_input_limit} cluster_limit={cluster_limit}",
        )
        print(f"[{cluster_name}] Snapshot downloaded: used={used_posts_len} for input {item_idx}")

        batch_label = (
            datetime.now().strftime("%Y-%m-%d %H:%M")
            + f" | {COMMAND_NAME} | {cluster_name}"
        )

        skipped_no_url = 0
        skipped_duplicate = 0
        rows_to_append = []

        for p in posts:
            url_val = extract_video_url(p)
            if not url_val:
                skipped_no_url += 1
                continue
            if url_val in existing_urls:
                skipped_duplicate += 1
                continue
            existing_urls.add(url_val)

            followers_val = normalize_followers(
                p.get("subscribers") or ""
            )

            hashtags_val = ""
            if p.get("tags"):
                try:
                    hashtags_val = json.dumps(p.get("tags"), ensure_ascii=False)
                except Exception:
                    hashtags_val = ""

            new_row = [
                url_val,
                p.get("views") or "",
                hashtags_val,
                p.get("channel_url") or "",
                followers_val,
                p.get("description") or "",
                batch_label,
                "",
            ]
            if len(new_row) < len(header):
                new_row = new_row + [""] * (len(header) - len(new_row))
            elif len(new_row) > len(header):
                new_row = new_row[: len(header)]

            rows.append(new_row)
            rows_to_append.append(new_row)

        if rows_to_append:
            sheet.values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_DATA}!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": rows_to_append},
            ).execute()

        total_appended += len(rows_to_append)
        if remaining_cluster is not None:
            remaining_cluster = max(0, remaining_cluster - len(rows_to_append))

        write_log(
            service,
            "rows_appended",
            cluster_name,
            (
                f"item_idx={item_idx} new_appended={len(rows_to_append)} "
                f"skipped_no_url={skipped_no_url} skipped_duplicate={skipped_duplicate} "
                f"remaining_cluster={remaining_cluster if remaining_cluster is not None else 'inf'}"
            ),
        )
        print(
            f"[{cluster_name}] rows_appended: new={len(rows_to_append)}, "
            f"total_rows={len(rows)}, skipped_no_url={skipped_no_url}, "
            f"skipped_duplicate={skipped_duplicate}, remaining_cluster={remaining_cluster}"
        )

        if remaining_cluster is not None and remaining_cluster <= 0:
            print(f"[{cluster_name}] cluster_limit достигнут, выходим из кластера.")
            break

    if with_gpt and rows:
        rows, gpt_count = apply_gpt_labels(
            service,
            cluster_name,
            header,
            rows,
            gpt_target_column,
            gpt_label_column,
            gpt_prompt,
            log_every=gpt_log_every,
        )
        write_log(service, "gpt_done", cluster_name, f"processed={gpt_count}")
        print(f"[{cluster_name}] GPT done, processed={gpt_count}")

    total_rows = len(rows) + 1

    extend_formulas_hij(service, total_rows)
    format_column_e_numbers(service, total_rows)

    write_log(
        service,
        "cluster_done",
        cluster_name,
        f"rows_total={len(rows)} appended={total_appended}",
    )
    print(f"[{cluster_name}] cluster_done, rows_total={len(rows)}, appended={total_appended}")


# ---------- прогон по активным кластерам ----------

def _run_over_active_clusters(service, settings, with_gpt=True, run_label="run_yt"):
    clusters = load_youtube_clusters(service)

    active_clusters = [
        (name, data)
        for name, data in clusters.items()
        if data["active"]
    ]

    if not active_clusters:
        print("Нет активных YouTube-кластеров.")
        write_log(service, "no_active_clusters", "YouTube", run_label)
        return

    active_clusters.sort(key=lambda x: x[1]["order"])
    cluster_names = [name for name, _ in active_clusters]

    print(f"\nПорядок кластеров в этом запуске ({run_label}):")
    print(" -> ".join(cluster_names))

    write_log(
        service,
        "cluster_sequence",
        "YouTube",
        " -> ".join(cluster_names),
    )

    for cluster_name, cluster_data in active_clusters:
        try:
            # GPT выполняем позже, одним проходом, поэтому здесь with_gpt=False
            process_cluster(service, settings, cluster_name, cluster_data, with_gpt=False)
            update_setting(service, "last_cluster_name_youtube", cluster_name)
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

    if with_gpt:
        _run_gpt_for_sheet(service, settings, overwrite=False, log_label="RUN_YOUTUBE_ALL")

    write_log(
        service,
        f"{run_label}_done",
        "YouTube",
        f"clusters={len(cluster_names)}",
    )
    print(f"{run_label} завершён. Обработано кластеров:", len(cluster_names))


def run_once():
    service = get_sheets_service()
    write_log(service, "run_start", "YouTube", f"version={BOT_VERSION}")
    print(f"[RUN] Старт YouTube-кластеров. Версия: {BOT_VERSION}")

    settings = load_settings(service)
    _run_over_active_clusters(service, settings, with_gpt=True, run_label="run_yt")


def run_scrape_only():
    service = get_sheets_service()
    write_log(service, "scrape_start", "YouTube", f"version={BOT_VERSION}")
    print(f"[SCRAPE_ONLY] YouTube. Версия: {BOT_VERSION}")

    settings = load_settings(service)
    _run_over_active_clusters(service, settings, with_gpt=False, run_label="scrape_yt")


def run_gpt_only(overwrite=False):
    service = get_sheets_service()
    settings = load_settings(service)

    _run_gpt_for_sheet(service, settings, overwrite=overwrite, log_label="GPT_ONLY_YOUTUBE")


def _run_gpt_for_sheet(service, settings, overwrite=False, log_label="RUN_YOUTUBE_ALL"):
    gpt_target_column = settings.get("gpt_target_column", "profile_biography")
    gpt_label_column = settings.get("gpt_label_column", "gpt_flag")
    gpt_prompt = settings.get(
        "gpt_prompt",
        "Only Y or N. If bio/description is fully in English or empty → Y. If it contains any non-English letters → N.",
    )

    header, rows = load_data_sheet(service)
    if not header or not rows:
        print(f"[{log_label}] Лист TikTok_Posts пуст или без заголовка.")
        return

    print(f"[{log_label}] Всего строк в TikTok_Posts: {len(rows)}")
    print(f"[{log_label}] Целевая колонка: {gpt_target_column}, колонка флага: {gpt_label_column}")

    if overwrite:
        try:
            label_idx = header.index(gpt_label_column)
            for i, r in enumerate(rows):
                if len(r) <= label_idx:
                    rows[i] = r + [""] * (label_idx + 1 - len(r))
                rows[i][label_idx] = ""
            print(f"[{log_label}] Все значения в колонке флага очищены, размечаем с нуля.")
        except ValueError:
            print(f"[{log_label}] Колонка флага не найдена, пропускаем очистку.")

    rows, processed = apply_gpt_labels(
        service,
        cluster_name=log_label,
        header=header,
        rows=rows,
        target_column=gpt_target_column,
        label_column=gpt_label_column,
        prompt_base=gpt_prompt,
        log_every=10,
    )

    save_gpt_labels_only(service, header, rows, gpt_label_column)
    print(f"[{log_label}] Готово. GPT обработал строк: {processed}")


# ---------- точка входа ----------

if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "full"

    if mode == "gpt_only":
        run_gpt_only(overwrite=False)
    elif mode == "scrape_only":
        run_scrape_only()
    elif mode == "start":
        # алиас полного цикла для единообразия с TikTok-ботом
        run_once()
    else:
        run_once()
