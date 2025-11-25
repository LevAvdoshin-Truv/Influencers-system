
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
DEFAULT_NUM_OF_POSTS = CONFIG.get("DEFAULT_NUM_OF_POSTS", 1000)

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

# заголовки для основного листа
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


# ---------- сервис Google Sheets ----------

def get_sheets_service():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


# ---------- логирование в Logs ----------

def write_log(service, action, cluster_name, details):
    sheet = service.spreadsheets()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = [[ts, action, cluster_name or "", details or ""]]

    # создадим шапку, если лист пустой
    try:
        resp = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_LOGS}!A1:D1"
        ).execute()
        values = resp.get("values", [])
        if not values:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_LOGS}!A1",
                valueInputOption="RAW",
                body={"values": [["timestamp", "action", "cluster_name", "details"]]},
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
        range=f"{SHEET_SETTINGS}!A:B"
    ).execute()
    values = resp.get("values", [])
    settings = {}
    for row in values[1:]:  # пропускаем заголовок
        if len(row) >= 2:
            key = row[0].strip()
            value = row[1].strip()
            if key:
                settings[key] = value
    return settings


def update_setting(service, key, new_value):
    # простая реализация: перечитать Settings и перезаписать
    sheet = service.spreadsheets()
    resp = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_SETTINGS}!A:B"
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
        range=f"{SHEET_SETTINGS}!A:B"
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
        range=f"{SHEET_CLUSTERS}!A:D"
    ).execute()
    values = resp.get("values", [])
    if len(values) <= 1:
        return {}

    rows = values[1:]  # пропускаем заголовок

    clusters = {}  # {name: {"order": int, "active": bool, "urls": [...]}}
    for row in rows:
        if len(row) < 4:
            continue
        name = row[0].strip()
        active_flag = row[1].strip().upper() == "Y"
        try:
            order = int(row[2])
        except:
            continue
        url = row[3].strip()
        if not url:
            continue

        if name not in clusters:
            clusters[name] = {"order": order, "active": active_flag, "urls": []}
        clusters[name]["urls"].append(url)
        # если хоть одна строка активна — считаем кластер активным
        if active_flag:
            clusters[name]["active"] = True

    return clusters


def choose_next_cluster(settings, clusters):
    last = settings.get("last_cluster_name")
    # оставляем только активные
    active_clusters = [(name, data) for name, data in clusters.items() if data["active"]]
    if not active_clusters:
        return None, None

    # сортируем по order
    active_clusters.sort(key=lambda x: x[1]["order"])
    names = [name for name, _ in active_clusters]

    if not last or last not in names:
        name, data = active_clusters[0]
        return name, data

    idx = names.index(last)
    next_idx = (idx + 1) % len(names)
    name, data = active_clusters[next_idx]
    return name, data


# ---------- TikTok Posts чтение/запись ----------

def load_data_sheet(service):
    sheet = service.spreadsheets()
    resp = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_DATA}!A1:H"
    ).execute()
    values = resp.get("values", [])
    if not values:
        return [], []
    header = values[0]
    rows = values[1:]
    return header, rows


def save_data_sheet(service, header, rows):
    sheet = service.spreadsheets()
    # нормализуем строки
    norm_rows = []
    for r in rows:
        r = r[:len(header)]
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        norm_rows.append(r)

    sheet.values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_DATA}!A1:H"
    ).execute()
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_DATA}!A1",
        valueInputOption="RAW",
        body={"values": [header] + norm_rows},
    ).execute()


def ensure_data_header(service):
    sheet = service.spreadsheets()
    resp = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_DATA}!A1:H1"
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
    # можно подстроиться под существующий заголовок, но пока берём наш
    return values[0]


# ---------- GPT ----------

def call_gpt_label(prompt_base, text):
    if not OPENAI_API_KEY:
        return "N"

    text = (text or "").strip()
    if not text:
        return "N"

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    user_content = (
        prompt_base.strip()
        + "\n\nТекст:\n"
        + text
    )

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": "Ты бинарный классификатор. Отвечай только 'Y' или 'N'."},
            {"role": "user", "content": user_content},
        ],
        "max_tokens": 1,
        "temperature": 0,
    }

        # отправляем запрос в Bright Data, даём больше времени на ответ
    resp = requests.post(url, headers=headers, json=payload, timeout=180)
    print("start_scrape status:", resp.status_code)

    if resp.status_code == 200:
        body = resp.text

        # 1) сначала пробуем как нормальный JSON-массив
        try:
            data = resp.json()
            if not isinstance(data, list):
                # если это не массив, считаем что формат другой
                raise ValueError("not array")
        except Exception:
            # 2) если не получилось — парсим как NDJSON построчно
            lines = [l.strip() for l in body.splitlines() if l.strip()]
            data = []
            bad_count = 0
            for idx, line in enumerate(lines, start=1):
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError as e:
                    bad_count += 1
                    # не валим весь цикл из-за одной кривой строки
                    print(f"NDJSON parse error on line {idx}: {e}")
                    continue

            print(f"NDJSON parsed: ok={len(data)}, bad={bad_count}")

        return {"mode": "sync", "posts": data, "snapshot_id": None}


    if resp.status_code == 202:
        j = resp.json()
        sid = j.get("snapshot_id")
        if not sid:
            raise RuntimeError("202 без snapshot_id")
        return {"mode": "async", "posts": None, "snapshot_id": sid}

    raise RuntimeError(f"Bright Data error {resp.status_code}: {resp.text[:500]}")


def get_snapshot_status(snapshot_id):
    url = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
    headers = {"Authorization": f"Bearer {BRIGHTDATA_API_KEY}"}
    resp = requests.get(url, headers=headers, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Status error: {resp.status_code} {resp.text[:200]}"
        )
    return resp.json().get("status", "")


def download_snapshot(snapshot_id):
    url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}?format=json"
    headers = {"Authorization": f"Bearer {BRIGHTDATA_API_KEY}"}
    resp = requests.get(url, headers=headers, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Download error: {resp.status_code} {resp.text[:200]}"
        )
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError("Expected list")
    return data


# ---------- основной цикл ----------

def main_loop():
    service = get_sheets_service()

    while True:
        try:
            settings = load_settings(service)
            clusters = load_clusters(service)

            bot_status = settings.get("bot_status", "off").lower()
            sleep_between_min = int(settings.get("sleep_between_min", "5"))
            wait_bright_min = int(settings.get("wait_bright_min", "20"))
            gpt_target_column = settings.get("gpt_target_column", "profile_biography")
            gpt_label_column = settings.get("gpt_label_column", "gpt_flag")
            gpt_prompt = settings.get(
                "gpt_prompt",
                "Считай Y, если текст связан с личными финансами, деньгами или расходами пользователя. Иначе N."
            )

            if bot_status != "on":
                print("Бот выключен (bot_status != 'on'). Ждём", sleep_between_min, "минут.")
                write_log(service, "bot_off", "", f"sleep {sleep_between_min} min")
                time.sleep(sleep_between_min * 60)
                continue

            cluster_name, cluster_data = choose_next_cluster(settings, clusters)
            if not cluster_name or not cluster_data:
                print("Нет активных кластеров. Ждём", sleep_between_min, "минут.")
                write_log(service, "no_active_clusters", "", f"sleep {sleep_between_min} min")
                time.sleep(sleep_between_min * 60)
                continue

            urls = cluster_data["urls"]
            write_log(service, "start_cycle", cluster_name, f"urls={len(urls)}")
            print("\n================ Новый цикл ================")
            print("Кластер:", cluster_name, "URL-ов:", len(urls))

            result = start_scrape_for_urls(urls)

            posts = None

            if result["mode"] == "sync":
                posts = result["posts"]
                write_log(service, "bright_sync_done", cluster_name, f"posts={len(posts)}")
            else:
                snapshot_id = result["snapshot_id"]
                write_log(service, "bright_async_started", cluster_name, f"snapshot_id={snapshot_id}")
                print("ASYNC, snapshot_id =", snapshot_id)
                print("Ждём", wait_bright_min, "минут...")
                time.sleep(wait_bright_min * 60)
                status = get_snapshot_status(snapshot_id)
                write_log(service, "snapshot_status", cluster_name, status)
                print("Статус снапшота:", status)
                if status != "ready":
                    print("Снапшот не готов, пропускаем цикл.")
                    write_log(service, "snapshot_not_ready", cluster_name, status)
                    time.sleep(sleep_between_min * 60)
                    continue
                posts = download_snapshot(snapshot_id)
                write_log(service, "snapshot_downloaded", cluster_name, f"posts={len(posts)}")

            if not posts:
                print("Постов нет, спим", sleep_between_min, "минут...")
                write_log(service, "no_posts", cluster_name, f"sleep {sleep_between_min} min")
                time.sleep(sleep_between_min * 60)
                continue

            batch_label = datetime.now().strftime("%Y-%m-%d %H:%M") + f" | {COMMAND_NAME} | {cluster_name}"
            print("batch_label:", batch_label)

            # работа с таблицей
            ensure_data_header(service)
            header, old_rows = load_data_sheet(service)
            if not header:
                header = HEADER

            old_count = len(old_rows)

            # дописываем новые посты
            new_rows = []
            for p in posts:
                new_rows.append([
                    p.get("url", ""),
                    p.get("play_count") or p.get("playcount") or "",
                    json.dumps(p.get("hashtags", []), ensure_ascii=False)
                        if p.get("hashtags") else "",
                    p.get("profile_url", ""),
                    p.get("profile_followers", ""),
                    p.get("profile_biography", ""),
                    batch_label,
                    ""
                ])

            all_rows = old_rows + new_rows
            write_log(service, "rows_appended", cluster_name, f"old={old_count} new={len(new_rows)} total={len(all_rows)}")

            # дедуп по url
            seen = set()
            deduped = []
            for r in all_rows:
                if len(r) < len(header):
                    r += [""] * (len(header) - len(r))
                url = r[0]
                if not url:
                    deduped.append(r)
                    continue
                if url in seen:
                    continue
                seen.add(url)
                deduped.append(r)

            write_log(service, "dedupe_done", cluster_name, f"before={len(all_rows)} after={len(deduped)}")

            # GPT-разметка
            deduped, gpt_count = apply_gpt_labels(
                header,
                deduped,
                gpt_target_column,
                gpt_label_column,
                gpt_prompt,
                max_rows=50
            )
            write_log(service, "gpt_done", cluster_name, f"processed={gpt_count}")

            save_data_sheet(service, header, deduped)
            write_log(service, "cycle_done", cluster_name, f"rows_total={len(deduped)}")

            # запоминаем этот кластер как last_cluster_name
            update_setting(service, "last_cluster_name", cluster_name)

            print("Цикл завершён, ждём", sleep_between_min, "минут...")
            time.sleep(sleep_between_min * 60)

        except Exception as e:
            print("Ошибка в цикле:", repr(e))
            try:
                service = get_sheets_service()
                write_log(service, "error", "", repr(e))
            except:
                pass
            time.sleep(60)


if __name__ == "__main__":
    main_loop()



