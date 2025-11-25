# TikTok Scraper Bot (Bright Data + GPT + Google Sheets)

Этот проект — автоматический бот, который каждые несколько минут:

1. Берёт кластер TikTok поисковых запросов из Google Sheets
2. Скрейпит их через Bright Data (Dataset API)
3. Если ответ асинхронный — ждёт, пока снапшот будет готов
4. Записывает данные в лист **TikTok_Posts**
5. Убирает дубли по `url`
6. Прогоняет тексты через GPT (OpenAI API)
7. Ставит `Y` / `N` в колонку `gpt_flag`
8. Пишет подробный лог в лист **Logs**
9. Переходит к следующему кластеру по кругу
10. Работает на Google Cloud VM

---

## Структура Google Sheets

### Лист `Settings`

Управляет поведением бота.

| key               | value (пример)                           |
|-------------------|-------------------------------------------|
| bot_status        | on / off                                 |
| sleep_between_min | 5                                         |
| wait_bright_min   | 20                                        |
| gpt_target_column | profile_biography                         |
| gpt_label_column  | gpt_flag                                  |
| gpt_prompt        | условие, по которому GPT решает Y / N     |
| last_cluster_name | служебное поле, бот пишет сам             |

- `bot_status = off` → бот просто спит и ничего не делает  
- `bot_status = on`  → бот крутит циклы

---

### Лист `Clusters`

Определяет группы TikTok запросов (кластеры), которые бот обходит по кругу.

| cluster_name | active | order | tiktok_search_url |
|--------------|--------|-------|-------------------|

Правила:

- все строки одного `cluster_name` должны иметь одинаковый `order`
- `active = Y` → кластер участвует
- `active = N` → кластер временно отключён
- каждый цикл бот берёт **следующий кластер** (по order и last_cluster_name)

---

### Лист `TikTok_Posts`

Основной лист с данными.

| url | play_count | hashtags | profile_url | profile_followers | profile_biography | batch | gpt_flag |

- `batch` = `дата-время | COMMAND_NAME | cluster_name`
- `gpt_flag` = `Y` / `N`

---

### Лист `Logs`

Подробные логи работы.

| timestamp | action | cluster_name | details |

Примеры action:

- `bot_off`
- `start_cycle`
- `bright_sync_done` / `bright_async_started`
- `snapshot_status`
- `snapshot_downloaded`
- `rows_appended`
- `dedupe_done`
- `gpt_done`
- `cycle_done`
- `error`

---

## Структура проекта на VM

Папка: `/home/lev_avdoshin/tiktok-bot`

Файлы:

- `tiktok_runner.py` — главный скрипт (бот)  
- `config.json` — конфиг с ключами (НЕ в GitHub)  
- `service-account.json` — ключ сервисного аккаунта Google (НЕ в GitHub)  
- `.gitignore` — защищает секреты от случайного коммита  

---

## Запуск бота (вручную)

```bash
cd ~/tiktok-bot
source ~/venv/bin/activate
python3 tiktok_runner.py

https://docs.google.com/document/d/17AhEgIkJ-3MSd7Gb_J8ijSDB7lNVeDlpY1lwi5-QE2E/edit?tab=t.2glda09d9bdr
