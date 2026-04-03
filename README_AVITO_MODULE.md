# Avito module — первая итерация

Это отдельный модуль под твою сборку. Он написан как clean-room реализация по мотивам нескольких открытых решений:

- inbox/UI и CRM-подход — по классу решений вроде `avito-messenger`;
- AI loop — по минималистичному циклу из `Avito-AI-answering`;
- browser fallback — по классу monitor-решений вроде `avito-messenger-monitor`;
- worker/CLI режимы — по подходу из `ResponseBot`;
- typed API/client слой — по мотивам Avito SDK/clients.

## Что уже есть

- отдельный Flask Blueprint `/avito`;
- durable SQLite storage на tenant;
- синхронизация чатов и сообщений по Avito API;
- генерация черновиков AI / rule-engine;
- встроенная tenant-level knowledge base / retrieval (RAG-lite без внешних зависимостей);
- media registry для фото/видео/документов с привязкой к item_id/item_title;
- отправка auto-ready или ручных ответов;
- CRM поля по чату: статус, теги, заметка;
- webhook ingest endpoint;
- optional Playwright browser fallback для превью чатов;
- CLI режимы: `serve`, `sync`, `drafts`, `send`, `poll`, `test-token`.

## Что пока НЕ встроено в host app

Модуль пока **не подключён автоматически** к `app.py`, потому что в этой итерации делалась безопасная сборка отдельного полноценного модуля с проверками. Для встраивания в текущий Flask app достаточно:

```python
from avito_module import register_avito_module
register_avito_module(app)
```

## Какие tenant-поля он ожидает

Может читать настройки из tenant dict:

- `avito_client_id`
- `avito_client_secret`
- `avito_user_id`
- `openai_api_key` (опционально)

Либо из `data/avito_settings.json` внутри tenant-папки.

## Хранилище tenant-а

Модуль создаёт внутри tenant `data/` и `auth/` такие файлы:

- `data/avito.sqlite3`
- `data/avito_settings.json`
- `data/avito_rules.json`
- `data/avito_exports/`
- `auth/avito_state.json` (для browser fallback)

## Как проверить вручную

```bash
python -m avito_module serve --tenant-id default --base-dir ./tmp_avito
```

или

```bash
python -m avito_module sync --tenant-id default --base-dir ./tmp_avito
```



## Подробные run-логи Avito

Модуль пишет детальные логи в каталог кабинета `logs/avito/`, а не в `data/`.
Структура:
- `logs/avito/channels/*.jsonl` — append-only ленты событий по каналам (`sync`, `ai`, `send`, `browser`, `webhook`, `ui`, `ops`).
- `logs/avito/runs/<run_id>.jsonl` — полный таймлайн отдельного запуска.
- `logs/avito/runs/<run_id>.json` — summary запуска.
- `logs/avito/runs_index.json` — индекс последних запусков для UI.
- `logs/avito/last_run.json` — последний завершённый или активный run.

Логи дублируют ключевые этапы и в `background_jobs.progress_log`, если операция выполняется в фоне.

## Что добавлено в этой итерации

- режим **webhook-first / polling-second / migration-third**;
- усиленный `Token Guardian`:
  - refresh на `401` и `403`;
  - per-tenant rate limiting;
  - retry budget и exponential backoff;
  - circuit breaker по серии ошибок;
- decision-trail логи:
  - `route`, `confidence`, `reason`, `policy`, `blocked_by`, `fallback`;
- DLQ и replay console для проблемных webhook/poll событий;
- оперативные метрики по Avito в UI и JSON endpoint;
- per-tenant изоляция секретов:
  - публичные настройки в `data/avito_settings.json`;
  - секреты в `auth/avito_secrets.json`;
  - состояние guardian в `auth/avito_guardian_state.json`;
- webhook security:
  - подпись,
  - timestamp skew window,
  - nonce replay protection,
  - idempotency/dedupe key;
- RBAC внутри Avito-модуля:
  - `view`, `reply`, `bulk_send`, `ai_rules`, `connect`, `secret_view`, `admin`.

## Новые CLI режимы

```bash
python -m avito_module backfill --tenant-id default --base-dir ./tmp_avito
python -m avito_module replay-dlq --tenant-id default --base-dir ./tmp_avito --dlq-id 12
python -m avito_module metrics --tenant-id default --base-dir ./tmp_avito
```

## Git hygiene / secret scanning

В патч добавлены:

- `.gitignore` с запретом на коммит runtime-секретов и browser state;
- `tools/scan_avito_secrets.py` — локальный сканер утечек;
- `.github/workflows/avito-secret-guard.yml` — GitHub Actions запуск сканера на push / PR.

Запуск локально:

```bash
python tools/scan_avito_secrets.py .
```


## Что добавлено в этой итерации: knowledge base + media registry

- SQLite-таблицы `knowledge_docs` и `knowledge_chunks` для базы знаний по кабинетам;
- поиск по knowledge-чанкам с учётом текста вопроса, item_id, item_title и тегов;
- knowledge-driven генерация черновиков: найденные фрагменты попадают в decision trail и prompt;
- `media_assets` и `draft_media_links` для реестра фото/видео/документов;
- UI-страницы `/avito/knowledge` и `/avito/media`;
- подбор медиа-материалов по конкретному чату и сохранение выбранных материалов у draft;
- в чатах видно, какие knowledge-hit'ы и какие материалы были использованы при подготовке ответа.

Важно: модуль **готовит и подбирает** фото/видео, но реальную API-отправку вложений оставляет выключенной по умолчанию (`media_send_enabled=false`), пока этот сценарий не подтверждён на реальном Avito-аккаунте.


## Что добавлено в этой итерации: импорт KB + похожие диалоги + HITL

- импорт базы знаний из `.txt`, `.md`, `.json`, `.jsonl`, `.csv`, `.tsv`, `.xlsx` через `/avito/knowledge/import`;
- поиск похожих прошлых диалогов по истории сообщений;
- использование похожих диалогов в decision trail и при подготовке черновиков;
- отдельная очередь ручной проверки `/avito/queue`;
- действия `approve / hold / reject / regenerate` для черновиков прямо из UI;
- draft state now: `review`, `ready`, `hold`, `error`.

## Что добавлено в этой итерации: живое фото-вложение

- для ответов можно использовать **selected media** из media registry;
- реализован **browser-first transport** для отправки фото через сохранённую browser session;
- если браузерный путь не сработал, модуль **честно откатывается в text-only fallback** и логирует `media_fallback=text_only`;
- прямой официальный Avito media API **не подтверждён в живом тесте**, поэтому API-endpoint поля оставлены как подготовка, а не как заявленная готовая интеграция.
