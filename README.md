# data-verification

Weekly cross-CRM verification: compare API order counts against Azure Blob (where applicable) and the warehouse (`data.orders_<client_id>`) for each active client of a given CRM, then post failures to Telegram.

- **Sticky**: 3-way check — API count vs deduped Blob count vs DB count.
- **Konnektive / Vrio / Paysight**: 2-way check — API count vs DB count (no processed-blob layer).

`weekly_verification.py` is the entry point; `index.py` holds the per-CRM API helpers and the `verify_data` flow.

## Setup

`config.ini` lives outside git (gitignored). It must define:

```ini
[database]
PSG_USER = ...
PSG_PASSWORD = ...
PSG_HOST = ...
PSG_PORT = 5432
PSG_DATABASE = ...

[azure]
BLOB_CONTAINER_URL = ...   # only needed for Sticky's blob step

[telegram]
BOT_TOKEN = bot<bot_id>:<auth_token>
CHAT_ID = <chat_id>        # personal or group
```

Install deps:

```
pip install -r requirements.txt
```

## Running per CRM

Same script, just change `--crm`. The value must match exactly what's stored in `beast_insights_v2.crms.name` (`Sticky`, `Konnektive`, `Vrio`, `Paysight`).

**Default — last 30 days, all active clients of that CRM:**
```
python3 weekly_verification.py --crm Sticky
python3 weekly_verification.py --crm Konnektive
python3 weekly_verification.py --crm Vrio
python3 weekly_verification.py --crm Paysight
```

**Single client:**
```
python3 weekly_verification.py --crm Konnektive --client-id 10057
```

**Explicit date range (overrides `--days`):**
```
python3 weekly_verification.py --crm Vrio --start-date 2026-04-01 --end-date 2026-04-30
```

## Flags

| Flag | Default | Notes |
|---|---|---|
| `--crm` | `Sticky` | Must match `beast_insights_v2.crms.name` |
| `--client-id` | none | Run for one client only |
| `--days` | `30` | Trailing window ending yesterday |
| `--start-date` / `--end-date` | none | YYYY-MM-DD; overrides `--days` |
| `--retries` | `2` | Retries per (client, date) on ERROR |
| `--max-date-workers` | `3` | Parallel dates per client |
| `--exclude-clients` | `10000,10027` | Demo accounts skipped by default |
| `--export` | off | Write the xlsx report (skipped by default) |
| `--no-telegram` | off | Skip the Telegram failure summary |

## Outputs

- **stdout**: per-(client, date) verification trace + a summary block.
- **xlsx**: `<crm>_weekly_verification_<timestamp>.xlsx` — only when `--export` is passed.
- **Telegram**: only sent when there's at least one FAIL or ERROR; lists each failing date per client with API count, blob count (Sticky), DB count, and delta.

## Exit codes

`0` if all (client, date) checks PASS; `1` if any FAIL or ERROR remains after retries — useful for Jenkins.

## Notes

- The job always runs `verify_data` with `dry_run=True`. It does not touch `clients_pipeline_status.data_verified`; that column is owned by the daily verification cron.
- Sticky's `order_find` caps at 50k results per call; `_sticky_order_count` splits the day into AM/PM windows when the cap is hit. A warning is logged if a half-day window also hits the cap (which would mean the count is still undercounted).
- Konnektive requires source-IP whitelisting, so production runs of `--crm Konnektive` need to happen from a whitelisted host.
