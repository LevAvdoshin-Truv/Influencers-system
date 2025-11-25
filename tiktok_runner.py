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


# по умолчанию берём 10 постов на запрос
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

# --- ограничение на логирование sleep ---
SLEEP_LOG_THROTTLE_LIMIT = 3
_last_sleep_log_key = None
_last_sleep_log_count = 0


# ---------- сервис Google Sheets ----------

def get_sheets_service():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


# ---------- логирование в Logs ----------

def write_log(service, action, cluster_name, details):
    """
    Пишет лог в лист Logs.
    Если подряд более 3 одинаковых логов с 'sleep' в details
    для одного action+cluster_name — дальше не логируем.
    """
    global _last_sleep_log_key, _last_sleep_log_count

    sheet = service.spreadsheets()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    action_text = action or ""
    cluster_text = cluster_name or ""
    details_text = details or ""

    # троттлинг по sleep
    is_sleep = isinstance(details_text, str) and "sleep" in details_text.lower()
    if is_sleep:
        key = (action_text, cluster_text)
        if _last_sleep_log_key == key:
            _last_sleep_log_count += 1
        else:
            _last_sleep_log_key = key
            _last_sleep_log_count = 1

        if _last_sleep_log_count > SLEEP_LOG_THROTTLE_LIMIT:
            print(
                f"[LOG THROTTLED] {ts} action={action_text} "
                f"cluster={cluster_text} details={details_text}"
            )
            return
    else:
        _last_sleep_log_key = None
        _last_sleep_log_count = 0

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
        valueInputOption="RAW",
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


def apply_gpt_labels(header, rows, target_column, label_column, prompt_base, max_rows=None):
    """
    Добавляет Y/N в label_column там, где пусто.
    Если max_rows is None — размечаем ВСЕ строки без ограничения.
    """
    try:
        text_idx = header.index(target_column)
        label_idx = header.index(label_column)
    except ValueError:
        print("GPT: не найдена колонка", target_column, "или", label_column)
        return rows, 0

    processed = 0
    for r in rows:
        if max_rows is not None and processed >= max_rows:
            break

        if len(r) < len(header):
            r += [""] * (len(header) - len(r))

        label = (r[label_idx] or "").strip().upper()
        if label in ("Y", "N"):
            continue

        text = r[text_idx]
        yn = call_gpt_label(prompt_base, text)
        r[label_idx] = yn
        processed += 1

    return rows, processed


# ---------- Bright Data ----------

def start_scrape_for_urls(urls):
    """
    Запускает асинхронный сбор в Bright Data по списку URLs
    через /datasets/v3/trigger и возвращает snapshot_id.
    """
    trigger_url = (
        "https://api.brightdata.com/datasets/v3/trigger"
        f"?dataset_id={DATASET_ID}&include_errors=true&format=json"
    )

    headers = {
        "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
        "Content-Type": "application/json",
    }

    # входные данные для скрейпера
    inputs = [{"url": u, "num_of_posts": DEFAULT_NUM_OF_POSTS} for u in urls]

    resp = requests.post(trigger_url, headers=headers, json=inputs, timeout=180)
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


# ---------- обработка одного кластера ----------

def process_cluster(service, settings, cluster_name, cluster_data):
    """
    Полный цикл по одному кластеру:
    Bright Data -> запись в таблицу -> дедуп -> GPT-разметка.
    Никаких больших sleep между кластерами.
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

    print("\n================ Новый кластер ================")
    print("Кластер:", cluster_name, "URL-ов:", len(urls))
    write_log(service, "start_cluster", cluster_name, f"urls={len(urls)}")

    # 1. Bright Data
    result = start_scrape_for_urls(urls)
    posts = None

    if result["mode"] == "sync":
        posts = result["posts"]
        write_log(
            service, "bright_sync_done", cluster_name, f"posts={len(posts)}"
        )
    else:
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
        status = "unknown"

        # ждём, пока progress станет ready
        while True:
            status = get_snapshot_status(snapshot_id)
            write_log(service, "snapshot_status", cluster_name, status)
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

    # 2. Если постов нет — выходим без сна
    if not posts:
        print(f"[{cluster_name}] Постов нет.")
        write_log(service, "no_posts", cluster_name, "0 posts")
        return

    original_posts_len = len(posts)
    # ограничиваем количество постов на кластер
    if cluster_limit > 0 and original_posts_len > cluster_limit:
        posts = posts[:cluster_limit]
    used_posts_len = len(posts)

    write_log(
        service,
        "snapshot_downloaded",
        cluster_name,
        f"posts_original={original_posts_len} posts_used={used_posts_len} limit={cluster_limit}",
    )

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
                p.get("profile_followers", ""),
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

    # 6. GPT-разметка: размечаем ВСЕ незаполненные строки (без лимита)
    deduped, gpt_count = apply_gpt_labels(
        header,
        deduped,
        gpt_target_column,
        gpt_label_column,
        gpt_prompt,
        max_rows=None,
    )
    write_log(service, "gpt_done", cluster_name, f"processed={gpt_count}")

    # 7. Сохраняем в Google Sheets
    save_data_sheet(service, header, deduped)
    write_log(
        service,
        "cluster_done",
        cluster_name,
        f"rows_total={len(deduped)}",
    )


# ---------- основной цикл ----------

def main_loop():
    service = get_sheets_service()

    while True:
        try:
            settings = load_settings(service)
            sleep_between_min = int(settings.get("sleep_between_min", "5"))

            bot_status = settings.get("bot_status", "off").lower()

            # ждём команды запуска
            if bot_status != "on":
                print(
                    "Бот в ожидании запуска (bot_status != 'on'). Ждём",
                    sleep_between_min,
                    "минут.",
                )
                write_log(
                    service,
                    "bot_idle",
                    "",
                    f"sleep {sleep_between_min} min (waiting for on)",
                )
                time.sleep(sleep_between_min * 60)
                continue

            # bot_status == on -> один полный цикл по кластерам
            clusters = load_clusters(service)
            active_clusters = [
                (name, data)
                for name, data in clusters.items()
                if data["active"]
            ]

            if not active_clusters:
                print("Нет активных кластеров для полного цикла.")
                write_log(
                    service,
                    "no_active_clusters",
                    "",
                    "full_cycle",
                )
                # считаем цикл завершённым
                update_setting(service, "bot_status", "off")
                continue

            # сортируем по order и стартуем с кластера после last_cluster_name
            active_clusters.sort(key=lambda x: x[1]["order"])
            names = [name for name, _ in active_clusters]
            last = settings.get("last_cluster_name")
            if last in names:
                start_idx = (names.index(last) + 1) % len(names)
            else:
                start_idx = 0
            ordered_names = names[start_idx:] + names[:start_idx]

            print("\n===== Старт полного цикла по кластерам =====")
            write_log(
                service,
                "full_cycle_start",
                "",
                f"clusters={len(ordered_names)}",
            )

            for cluster_name in ordered_names:
                cluster_data = clusters[cluster_name]
                try:
                    process_cluster(service, settings, cluster_name, cluster_data)
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
                finally:
                    update_setting(service, "last_cluster_name", cluster_name)

            write_log(
                service,
                "full_cycle_done",
                "",
                f"clusters={len(ordered_names)}",
            )
            print("Полный цикл по кластерам завершён.")

            # один on = один полный цикл
            update_setting(service, "bot_status", "off")
            print("Установлен bot_status = 'off'. Ожидание следующего запуска.")

        except Exception as e:
            print("Ошибка в main_loop:", repr(e))
            try:
                service = get_sheets_service()
                write_log(service, "error", "", repr(e))
            except Exception:
                pass
            time.sleep(60)


if __name__ == "__main__":
    main_loop()
