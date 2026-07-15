"""
Data Verification Script
Verifies order data through a 3-step pipeline:
1. Fetch order count from CRM API
2. Fetch deduplicated order count from Azure Blob (parquet files)
3. Compare: API count == Blob count, then (Blob count - test orders) == DB count
"""

import argparse
import configparser
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import psycopg2
import requests
from azure.storage.blob import ContainerClient


# Configuration
config = configparser.RawConfigParser()
config.read("config.ini")

PSG_USER = config.get("database", "PSG_USER")
PSG_PASSWORD = config.get("database", "PSG_PASSWORD")
PSG_HOST = config.get("database", "PSG_HOST")
PSG_PORT = int(config.get("database", "PSG_PORT"))
PSG_DATABASE = config.get("database", "PSG_DATABASE")

BLOB_CONTAINER_URL = (
    config.get("azure", "BLOB_CONTAINER_URL", fallback="").strip()
    if config.has_section("azure") else ""
)
AZURE_PROCESSED_UPDATED_SAS_URL = (
    config.get("azure", "AZURE_PROCESSED_UPDATED_SAS_URL", fallback="").strip()
    if config.has_section("azure") else ""
)

API_BLOB_THRESHOLD = 0.99  # 99% match between API and Blob
BLOB_DB_THRESHOLD = 0.99   # 99% match between Blob (non-test) and DB
CH_SYNC_THRESHOLD = 0.99   # 99% match between Postgres data.orders and ClickHouse data.orders
CH_ENRICHED_THRESHOLD = 0.99  # 99% match between ClickHouse data.orders and orders_enriched

CLICKHOUSE_HOST = (
    config.get("clickhouse", "CLICKHOUSE_HOST", fallback="").strip()
    if config.has_section("clickhouse") else ""
)
CLICKHOUSE_USER = (
    config.get("clickhouse", "CLICKHOUSE_USER", fallback="").strip()
    if config.has_section("clickhouse") else ""
)
CLICKHOUSE_PASSWORD = (
    config.get("clickhouse", "CLICKHOUSE_PASSWORD", fallback="").strip()
    if config.has_section("clickhouse") else ""
)


def get_crm_credentials(crm_name, client_id=None):
    """Fetch CRM credentials from the database. If client_id is None, fetch all active clients."""
    try:
        conn = psycopg2.connect(
            user=PSG_USER,
            password=PSG_PASSWORD,
            host=PSG_HOST,
            port=PSG_PORT,
            database=PSG_DATABASE,
        )
        cur = conn.cursor()

        if client_id:
            sql = f"""
                SELECT cc.id       as "CRENDENTIAL_ID",
                       cl.id       as "CLIENT_ID",
                       cl.name     as "CLIENT_NAME",
                       cc.username as "CRM_USERNAME",
                       cc.password as "CRM_PASSWORD",
                       cc.host     as "CRM_HOST",
                       cc.key      as "CRM_API_KEY",
                       crm.name    AS "CRM_NAME"
                FROM beast_insights_v2.clients AS cl
                    INNER JOIN beast_insights_v2.crm_credentials cc ON cl.id = cc.client_id
                    INNER JOIN beast_insights_v2.crms crm ON cc.crm_id = crm.id
                WHERE crm.name = '{crm_name}' AND cl.id = {client_id};
            """
        else:
            sql = f"""
                SELECT cc.id       as "CRENDENTIAL_ID",
                       cl.id       as "CLIENT_ID",
                       cl.name     as "CLIENT_NAME",
                       cc.username as "CRM_USERNAME",
                       cc.password as "CRM_PASSWORD",
                       cc.host     as "CRM_HOST",
                       cc.key      as "CRM_API_KEY",
                       crm.name    AS "CRM_NAME"
                FROM beast_insights_v2.clients AS cl
                    INNER JOIN beast_insights_v2.crm_credentials cc ON cl.id = cc.client_id
                    INNER JOIN beast_insights_v2.crms crm ON cc.crm_id = crm.id
                WHERE crm.name = '{crm_name}'
                  AND cl.is_active = true AND cl.is_deleted = false
                ORDER BY cl.id;
            """
        cur.execute(sql)
        table_data = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        results = [SimpleNamespace(**dict(zip(columns, row))) for row in table_data]
        conn.commit()
        return results

    except Exception as e:
        print(f"Error fetching CRM credentials: {e}")
        return []
    finally:
        cur.close()
        conn.close()


STICKY_API_DAILY_CAP = 50000


def _sticky_fetch_order_ids(crm, date_str, start_time, end_time):
    """Single Sticky order_find call for a [start_time, end_time] window on date_str.
    Returns list of order_ids on success, None on failure."""
    order_find_url = f"https://{crm.CRM_HOST}/api/v1/order_find"
    payload = {
        "campaign_id": "all",
        "start_date": date_str,
        "start_time": start_time,
        "end_date": date_str,
        "end_time": end_time,
        "date_type": "create",
        "criteria": "all",
    }
    try:
        response = requests.post(
            order_find_url,
            json=payload,
            auth=(crm.CRM_USERNAME, crm.CRM_PASSWORD),
            timeout=60,
        )
        if response.status_code == 200:
            return response.json().get("order_id", []) or []
        print(f"CRM API request failed with status code: {response.status_code} ({start_time}–{end_time})")
        return None
    except requests.RequestException as e:
        print(f"Error calling CRM API ({start_time}–{end_time}): {e}")
        return None


def _sticky_order_count(crm, date_str):
    """Sticky: full-day order_find with AM/PM split if 50k cap hit."""
    order_ids = _sticky_fetch_order_ids(crm, date_str, "00:00:00", "23:59:59")
    if order_ids is None:
        return None, []

    if len(order_ids) < STICKY_API_DAILY_CAP:
        return len(order_ids), order_ids

    print(f"[split] {date_str}: full-day hit {STICKY_API_DAILY_CAP} cap — splitting into AM/PM windows")
    am = _sticky_fetch_order_ids(crm, date_str, "00:00:00", "11:59:59")
    pm = _sticky_fetch_order_ids(crm, date_str, "12:00:00", "23:59:59")
    if am is None or pm is None:
        return None, []

    if len(am) >= STICKY_API_DAILY_CAP or len(pm) >= STICKY_API_DAILY_CAP:
        print(
            f"[split] {date_str}: half-day window also hit cap "
            f"(AM={len(am)}, PM={len(pm)}) — count may still be undercounted"
        )

    merged = list({*am, *pm})
    return len(merged), merged


CHECKOUTCHAMP_CLIENT_IDS = {"10057", "10058", "10062", "10067"}


def _konnektive_base_url(client_id):
    """Konnektive base URL is fixed per-tenant, not stored in cc.host. A handful of
    clients are routed to CheckoutChamp; everything else uses [production] KONNEKTIVE_API_URL."""
    if str(client_id) in CHECKOUTCHAMP_CLIENT_IDS:
        return "https://api.checkoutchamp.com"
    if config.has_section("production") and config.has_option("production", "KONNEKTIVE_API_URL"):
        base = config.get("production", "KONNEKTIVE_API_URL").strip().rstrip("/")
        if base:
            return base
    return None


def _konnektive_txn_count(crm, date_str, txn_type):
    """Single /transactions/query/ call. Mirrors production fetch_transactions_by_type
    (is_success=None, includeBlacklist=1, dateRangeType=dateCreated) so the count
    matches what's ingested into data.konnektive_transactions_{client_id}.
    Reads message.totalResults; returns int or None on failure."""
    base = _konnektive_base_url(crm.CLIENT_ID)
    if not base:
        print("Konnektive API URL not configured (set [production] KONNEKTIVE_API_URL in config.ini)")
        return None
    url = f"{base}/transactions/query/"
    params = {
        "loginId": crm.CRM_USERNAME,
        "password": crm.CRM_PASSWORD,
        "page": 1,
        "resultsPerPage": 200,
        "startDate": date_str,
        "endDate": date_str,
        "dateRangeType": "dateCreated",
        "txnType": txn_type,
        "includeBlacklist": 1,
    }
    try:
        r = requests.post(url, params=params, timeout=60)
    except requests.RequestException as e:
        print(f"Error calling Konnektive /transactions/query/ ({txn_type}): {e}")
        return None
    if r.status_code != 200:
        print(f"Konnektive /transactions/query/ ({txn_type}) failed: {r.status_code}")
        return None
    body = r.json()
    msg = body.get("message") or {}
    if isinstance(msg, dict) and "totalResults" in msg:
        return int(msg["totalResults"])
    if body.get("result") == "ERROR" or msg == "EMPTY":
        return 0
    print(f"Konnektive /transactions/query/ ({txn_type}): unexpected response: {body!r}"[:300])
    return None


def _konnektive_order_count(crm, date_str):
    """Konnektive: count SALE + AUTHORIZE successful transactions (matches the rows
    that land in data.konnektive_transactions_{client_id})."""
    sale = _konnektive_txn_count(crm, date_str, "SALE")
    auth = _konnektive_txn_count(crm, date_str, "AUTHORIZE")
    if sale is None or auth is None:
        return None, []
    total = sale + auth
    print(f"Konnektive {date_str}: SALE={sale}, AUTHORIZE={auth}, total={total}")
    return total, []


VRIO_PRIMARY_TXN_TYPES = (1, 6, 7, 8)  # SALE, AUTH, CAPTURE, COD — match sync_vrio_orders.py
VRIO_PAGE_SIZE = 200


def _vrio_base_url(crm):
    host = (getattr(crm, "CRM_HOST", "") or "").strip()
    if not host or host.lower() == "none":
        return "https://api.vrio.app"
    return host if "://" in host else f"https://{host}"


def _vrio_is_test(txn):
    """Mirror vrio.py is_test_order: any line item is_test=true, OR customer email contains 'test'."""
    for item in txn.get("line_items", []) or []:
        if item.get("is_test") in (True, "true", "True", 1, "1"):
            return True
    email = ((txn.get("customer") or {}).get("email") or "")
    return "test" in email.lower()


def _vrio_order_count(crm, date_str):
    """Vrio: sum of non-test line items across SALE/AUTH/CAPTURE/COD transactions for the day.
    Mirrors vrio.py exactly: counts max(1, len(line_items)) per transaction, drops test orders."""
    base_url = _vrio_base_url(crm).rstrip("/")
    headers = {"accept": "application/json", "hostname": "api.vrio.app", "X-Api-Key": crm.CRM_API_KEY}
    sql_date = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")

    total_count = 0
    non_test_count = 0
    for txn_type in VRIO_PRIMARY_TXN_TYPES:
        offset = 0
        while True:
            params = {
                "offset": offset,
                "limit": VRIO_PAGE_SIZE,
                "date_complete_from": sql_date,
                "date_complete_to": sql_date,
                "transaction_type_id": txn_type,
                "transaction_status": 2,
                "with": "customer,line_items",
            }
            try:
                r = requests.get(f"{base_url}/transactions/", headers=headers, params=params, timeout=60)
            except requests.RequestException as e:
                print(f"Vrio /transactions/ (type {txn_type}) error: {e}")
                return None, []
            if r.status_code != 200:
                print(f"Vrio /transactions/ (type {txn_type}) failed: {r.status_code}")
                return None, []
            body = r.json()
            if body.get("result") == "ERROR":
                if offset == 0 and txn_type == 1:
                    return None, []
                break
            txns = body.get("transactions", []) or []
            api_total = int(body.get("total", 0))
            for txn in txns:
                items = txn.get("line_items") or []
                count = max(len(items), 1)
                total_count += count
                if not _vrio_is_test(txn):
                    non_test_count += count
            if offset + VRIO_PAGE_SIZE >= api_total:
                break
            offset += VRIO_PAGE_SIZE
    print(f"Vrio {date_str}: total line items={total_count}, non-test={non_test_count}")
    return non_test_count, []


PAYSIGHT_PAGE_LIMIT = 1000
PAYSIGHT_RETRY_BACKOFF = [5, 15, 30, 60, 120]


def _paysight_url(crm):
    host = (crm.CRM_HOST or "").strip().rstrip("/")
    return f"{host}/api/transactions/search" if "://" in host else f"https://{host}/api/transactions/search"


def _paysight_headers(crm):
    return {
        "Authorization": crm.CRM_PASSWORD,
        "ClientId": str(crm.CRM_API_KEY),
        "Content-Type": "application/json",
        "UserEmail": crm.CRM_USERNAME,
    }


def _paysight_is_order(txn):
    """Mirror paysight.py is_order_transaction: drop alerts, Visa Fraud (TC40),
    refund/chargeback children/orphans, etc."""
    alert_source = txn.get("alertSource")
    refund_source = txn.get("refundSource")
    application = txn.get("application", "")
    if alert_source == "Unassigned":
        alert_source = None
    if refund_source == "Unassigned":
        refund_source = None
    if alert_source is not None:
        return False
    if application == "Visa Fraud (TC40)":
        return False
    ancestor = txn.get("originalTransactionId")
    is_approved = txn.get("success", False)
    is_refund = txn.get("refunded", False)
    is_chargeback = txn.get("chargedBack", False)
    if ancestor and (refund_source is not None or is_refund):
        return False
    if ancestor and is_chargeback and not is_approved:
        return False
    if not ancestor and is_chargeback and not is_approved:
        return False
    return True


def _paysight_fetch_page(crm, sql_date, page_number):
    """Single paginated POST with retry/backoff. Returns (transactions, more_results) or (None, False) on hard error."""
    payload = {
        "pageNumber": page_number,
        "limit": PAYSIGHT_PAGE_LIMIT,
        "dateFrom": sql_date,
        "dateTo": sql_date,
        "dateCompleted": True,
    }
    url = _paysight_url(crm)
    headers = _paysight_headers(crm)
    for attempt in range(len(PAYSIGHT_RETRY_BACKOFF) + 1):
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=120)
            if r.status_code == 200:
                data = r.json()
                if data.get("success"):
                    return data.get("transactions", []) or [], bool(data.get("moreResults", False))
                print(f"Paysight API success=false (page {page_number}): {data.get('message')}")
                return [], False
            if r.status_code == 429 or r.status_code >= 500:
                wait = PAYSIGHT_RETRY_BACKOFF[min(attempt, len(PAYSIGHT_RETRY_BACKOFF) - 1)]
                print(f"Paysight API {r.status_code} (page {page_number}), retrying in {wait}s")
                time.sleep(wait)
                continue
            print(f"Paysight API {r.status_code}: {r.text[:200]}")
            return None, False
        except requests.exceptions.Timeout:
            wait = PAYSIGHT_RETRY_BACKOFF[min(attempt, len(PAYSIGHT_RETRY_BACKOFF) - 1)]
            print(f"Paysight API timeout (page {page_number}), retrying in {wait}s")
            time.sleep(wait)
        except requests.RequestException as e:
            print(f"Paysight API error (page {page_number}): {e}")
            return None, False
    return None, False


def _paysight_counts(crm, date_str):
    """Walk Paysight pagination for the date and return (crm_total, crm_orders) or (None, None)."""
    sql_date = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    crm_total = 0
    crm_orders = 0
    page = 1
    while True:
        txns, more = _paysight_fetch_page(crm, sql_date, page)
        if txns is None:
            return None, None
        if page == 1 and not txns and not more:
            return None, None
        crm_total += len(txns)
        crm_orders += sum(1 for t in txns if _paysight_is_order(t))
        if not more:
            break
        page += 1
        time.sleep(1)
    return crm_total, crm_orders


def _paysight_order_count(crm, date_str):
    """Paysight: filtered (orders) count — used by the generic dispatch when verify_data
    isn't paysight-aware. The full 3-way verify lives in _verify_paysight."""
    total, orders = _paysight_counts(crm, date_str)
    return (None if orders is None else orders), []


def get_crm_order_count(crm, date_str):
    """Dispatch to per-CRM order-count helper based on crm.CRM_NAME."""
    name = (getattr(crm, "CRM_NAME", "") or "").strip().lower()
    if name == "sticky":
        return _sticky_order_count(crm, date_str)
    if name == "konnektive":
        return _konnektive_order_count(crm, date_str)
    if name == "vrio":
        return _vrio_order_count(crm, date_str)
    if name == "paysight":
        return _paysight_order_count(crm, date_str)
    print(f"Unsupported CRM for verification: {crm.CRM_NAME!r}")
    return None, []


def _read_blobs_to_df(container_client, prefix):
    """List + read every parquet under a prefix into one DataFrame.
    Returns (n_files, df) — df is None when no files were found."""
    blobs = list(container_client.list_blobs(name_starts_with=prefix))
    if not blobs:
        return 0, None
    dfs = []
    for blob in blobs:
        data = container_client.get_blob_client(blob.name).download_blob().readall()
        dfs.append(pd.read_parquet(pd.io.common.BytesIO(data)))
    return len(blobs), (pd.concat(dfs, ignore_index=True) if dfs else None)


def _read_processedupdated_for_date_of_sale(container_client, client_id, target_date):
    """Walk processedupdated/sticky/{client_id}/{Y}/{M}/{D}/ for every day from
    target_date through today (NY-ish, just system today), filter to rows whose
    DATE_OF_SALE matches target_date. Sticky can emit an updated snapshot for an
    order days after the original sale, so historic verification has to look
    forward from the target date."""
    today = datetime.now().date()
    cursor = target_date.date() if hasattr(target_date, "date") else target_date
    target_date_str = target_date.strftime("%Y-%m-%d")
    parts = []
    total_files = 0
    while cursor <= today:
        prefix = f"sticky/{client_id}/{cursor.strftime('%Y/%m/%d')}/"
        n_files, df = _read_blobs_to_df(container_client, prefix)
        total_files += n_files
        if df is not None and "DATE_OF_SALE" in df.columns:
            df = df[df["DATE_OF_SALE"].astype(str) == target_date_str]
            if not df.empty:
                parts.append(df)
        cursor += timedelta(days=1)
    return total_files, (pd.concat(parts, ignore_index=True) if parts else None)


def get_blob_order_data(client_id, target_date):
    """
    Merge initial + updated processed blobs and dedupe to unique orders.

    Sources:
      - processed/sticky/{client_id}/{Y}/{M}/{D}/*.parquet           (initial drop)
      - processedupdated/sticky/{client_id}/{Y}/{M}/{D}/*.parquet    (later updates,
        scanned from target_date through today since Sticky can emit an updated
        snapshot for an order days/weeks after the original sale)

    Dedup rule (matches Sticky's live order_find result for the date):
      - One row per ORDER_ID.
      - When an order_id appears multiple times across both blobs, the row
        with a real TRANSACTION_ID wins over a placeholder
        ('', '0', 'nan', 'None', NaN). Retry pairs (same order, different
        real TXNs) collapse to a single row — matches API behavior.

    Returns: (total_unique_orders, non_test_count, test_count, non_test_DataFrame)
    """
    if not BLOB_CONTAINER_URL:
        print("Error: [azure] BLOB_CONTAINER_URL is not set in config.ini — required for Sticky verification")
        return None, None, None, None

    target_date_str = target_date.strftime("%Y-%m-%d")
    init_prefix = f"sticky/{client_id}/{target_date.strftime('%Y/%m/%d')}/"

    try:
        proc_container = ContainerClient.from_container_url(BLOB_CONTAINER_URL)
        n_init_files, init_df = _read_blobs_to_df(proc_container, init_prefix)
        if n_init_files:
            print(f"Found {n_init_files} initial parquet file(s) at: {init_prefix}")
        else:
            print(f"No initial blob files found at: {init_prefix}")

        n_upd_files = 0
        upd_df = None
        if AZURE_PROCESSED_UPDATED_SAS_URL:
            upd_container = ContainerClient.from_container_url(AZURE_PROCESSED_UPDATED_SAS_URL)
            n_upd_files, upd_df = _read_processedupdated_for_date_of_sale(
                upd_container, client_id, target_date
            )
            print(f"Scanned processedupdated: {n_upd_files} file(s) "
                  f"across {target_date_str} → today, "
                  f"{0 if upd_df is None else len(upd_df)} row(s) for DATE_OF_SALE={target_date_str}")
        else:
            print("Skipping processedupdated: [azure] AZURE_PROCESSED_UPDATED_SAS_URL not set in config.ini")

        # Filter initial drop to target_date (older snapshots may carry shifted dates)
        if init_df is not None and "DATE_OF_SALE" in init_df.columns:
            init_df = init_df[init_df["DATE_OF_SALE"].astype(str) == target_date_str].copy()

        # If both sides are empty, we have nothing
        if (init_df is None or init_df.empty) and (upd_df is None or upd_df.empty):
            return None, None, None, None

        # Tag source so processedupdated can win in the dedup
        if init_df is not None and not init_df.empty:
            init_df = init_df.copy()
            init_df["_source"] = "initial"
        else:
            init_df = None
        if upd_df is not None and not upd_df.empty:
            upd_df = upd_df.copy()
            upd_df["_source"] = "updated"
        else:
            upd_df = None

        combined = pd.concat(
            [d for d in (init_df, upd_df) if d is not None],
            ignore_index=True,
        )
        print(f"Total rows after merging both blobs (DATE_OF_SALE={target_date_str}): {len(combined)}")

        # Normalize TRANSACTION_ID: NaN and empty string are treated as the same
        combined["TRANSACTION_ID"] = combined["TRANSACTION_ID"].fillna("")

        # Sort so the winning row lands last in each ORDER_ID group (keep='last'):
        # row with a real TRANSACTION_ID beats a placeholder. Retry pairs (same
        # order_id, two different real TXNs) collapse to a single row — matches
        # Sticky's order_find which returns each order_id once.
        combined = combined.assign(
            _txn_priority=combined["TRANSACTION_ID"].astype(str).map(
                lambda s: 0 if s.strip().lower() in ("", "0", "nan", "none") else 1
            ),
        ).sort_values("_txn_priority", kind="stable")

        deduped = combined.drop_duplicates(subset=["ORDER_ID"], keep="last").drop(
            columns=["_txn_priority", "_source"]
        )
        total_count = len(deduped)
        print(f"Unique orders (one per ORDER_ID, real-TXN preferred): {total_count}")

        # Identify test orders: IS_TEST == "True" or BILL_EMAIL contains 'test'
        test_mask = deduped["IS_TEST"].astype(str).str.lower().eq("true") | \
                    deduped["BILL_EMAIL"].astype(str).str.contains("test", case=False, na=False)
        test_count = int(test_mask.sum())
        non_test_count = int(total_count - test_count)

        print(f"Test orders: {test_count}")
        print(f"Non-test orders: {non_test_count}")

        # 4th return is the non-test subset (used by verify_data Step 4 to
        # match blob line items to DB rows by order_id, since Sticky shifts
        # date_of_sale on later raw_updated re-fetches).
        return total_count, non_test_count, test_count, deduped[~test_mask]

    except Exception as e:
        print(f"Error fetching blob data: {e}")
        return None, None, None, None


def get_db_order_count(client_id, date_str):
    """
    Fetch the count of orders from the database for a specific date.
    Uses data.orders_{client_id} table with end_date = '9999-12-31' filter.
    """
    return _db_count(
        f"""
        SELECT COUNT(1) FROM data.orders_{client_id}
        WHERE date_of_sale = %s AND end_date = '9999-12-31'
        """,
        (datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d"),),
    )


def _db_count(sql, params):
    """Run a single COUNT(*) SQL and return the integer (or None on failure)."""
    conn = cur = None
    try:
        conn = psycopg2.connect(
            user=PSG_USER,
            password=PSG_PASSWORD,
            host=PSG_HOST,
            port=PSG_PORT,
            database=PSG_DATABASE,
        )
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else 0
    except Exception as e:
        print(f"Error fetching count from database: {e}")
        return None
    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def get_db_order_count_by_ids(client_id, order_ids):
    """Count active rows in data.orders_{client_id} whose order_id is in `order_ids`,
    regardless of date_of_sale. Used by Sticky verification Step 4 because Sticky's
    raw_updated pipeline can shift date_of_sale on later re-fetches, so a strict
    date match misses rows that are still present (just under the next day's date)."""
    if not order_ids:
        return 0
    ids = sorted({str(o) for o in order_ids if o is not None and str(o).strip() not in ("", "nan", "None")})
    if not ids:
        return 0
    return _db_count(
        f"""
        SELECT COUNT(1) FROM data.orders_{client_id}
        WHERE order_id::text = ANY(%s)
          AND end_date = '9999-12-31'
        """,
        (ids,),
    )


def get_konnektive_db_count(client_id, date_str):
    """Count of SALE/AUTHORIZE rows in data.konnektive_transactions_{client_id}
    for the given date (matches the API /transactions/query/ unit)."""
    sql_date = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    return _db_count(
        f"""
        SELECT COUNT(1) FROM data.konnektive_transactions_{client_id}
        WHERE txn_type IN ('SALE', 'AUTHORIZE')
          AND date_created::date = %s
          AND end_date = '9999-12-31'
        """,
        (sql_date,),
    )


def get_vrio_db_count(client_id, date_str):
    """Non-test rows in data.orders_{client_id} for the date — matches vrio.py's filter."""
    sql_date = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    return _db_count(
        f"""
        SELECT COUNT(1) FROM data.orders_{client_id}
        WHERE date_of_sale = %s
          AND end_date = '9999-12-31'
          AND is_test_actual != 'Yes'
        """,
        (sql_date,),
    )


def get_paysight_raw_db_count(client_id, date_str):
    """Count of rows in the raw staging table data.paysight_{client_id} for the date.
    Compares against API total (unfiltered)."""
    sql_date = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    return _db_count(
        f"""
        SELECT COUNT(1) FROM data.paysight_{client_id}
        WHERE date_of_sale::date = %s
          AND end_date = '9999-12-31'
        """,
        (sql_date,),
    )


def get_paysight_orders_db_count(client_id, date_str):
    """Count of rows in data.orders_{client_id} (orders table). Compares against
    API filtered count (orders only — excludes alerts/Visa-Fraud/refund-children/etc)."""
    sql_date = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    return _db_count(
        f"""
        SELECT COUNT(1) FROM data.orders_{client_id}
        WHERE date_of_sale::date = %s
          AND end_date = '9999-12-31'
        """,
        (sql_date,),
    )


def get_pg_orders_nontest_count(client_id, date_str):
    """Count of distinct active, non-test, non-excluded order_ids in Postgres
    data.orders_{client_id} for the date. Uses COUNT(DISTINCT order_id) so duplicate
    rows don't skew the Postgres-vs-ClickHouse comparison. Excludes test orders
    (is_test IS NOT TRUE) and excluded orders (is_exclude IS NOT TRUE) to match the
    orders_enriched grain, which only contains non-test, non-excluded orders."""
    return _db_count(
        f"""
        SELECT COUNT(DISTINCT order_id) FROM data.orders_{client_id}
        WHERE date_of_sale = %s
          AND end_date = '9999-12-31'
          AND is_test IS NOT TRUE
          AND is_exclude IS NOT TRUE
        """,
        (datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d"),),
    )


def _ch_scalar(sql):
    """Run a single-value query against the ClickHouse HTTP interface.
    Returns the integer result, or None on any error (missing table, network, etc.)."""
    if not CLICKHOUSE_HOST:
        print("Error: [clickhouse] CLICKHOUSE_HOST is not set in config.ini — required for ClickHouse checks")
        return None
    headers = {
        "X-ClickHouse-User": CLICKHOUSE_USER,
        "X-ClickHouse-Key": CLICKHOUSE_PASSWORD,
    }
    try:
        resp = requests.post(CLICKHOUSE_HOST, data=sql.encode(), headers=headers, timeout=60)
        if resp.status_code != 200:
            print(f"ClickHouse query failed ({resp.status_code}): {resp.text.strip()[:200]}")
            return None
        text = resp.text.strip()
        return int(text) if text else 0
    except Exception as e:
        print(f"Error querying ClickHouse: {e}")
        return None


def get_ch_orders_count(client_id, date_str):
    """Count of distinct active, non-test, non-excluded order_ids in ClickHouse
    data.orders_{client_id} for the date. Uses uniqExact(order_id) so duplicate rows
    (e.g. un-merged ReplacingMergeTree parts) don't skew the comparison. Excludes test
    orders (is_test = 0) and excluded orders (is_exclude = 0) so it matches both the
    Postgres distinct count (Step 5) and the orders_enriched grain (Step 6)."""
    sql_date = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    return _ch_scalar(
        f"SELECT uniqExact(order_id) FROM data.orders_{client_id} "
        f"WHERE date_of_sale = '{sql_date}' AND end_date = '9999-12-31' "
        f"AND (is_test = 0 OR is_test IS NULL) "
        f"AND (is_exclude = 0 OR is_exclude IS NULL) FORMAT TabSeparated"
    )


def get_ch_enriched_count(client_id, date_str):
    """Distinct non-test, non-excluded order_ids in ClickHouse reporting.orders_enriched_{client_id}
    for the date. The enriched table fans out rows per order (alert/refund/cb/void/tc40 events and,
    for some CRMs, multiple rows per order_id), so we restrict to the sale/order grain
    (kind='sale' AND row_kind='order'), exclude test and excluded orders, and count distinct
    order_id to match the data.orders_ distinct-order_id grain (get_ch_orders_count). Counting
    order_id rather than unique_order_key keeps both sides on the same grain for CRMs where an
    order_id legitimately spans multiple enriched rows (e.g. Konnektive)."""
    sql_date = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    return _ch_scalar(
        f"SELECT uniqExact(order_id) FROM reporting.orders_enriched_{client_id} "
        f"WHERE kind = 'sale' AND row_kind = 'order' AND date_of_sale = '{sql_date}' "
        f"AND (is_test = 0 OR is_test IS NULL) "
        f"AND (is_exclude = 0 OR is_exclude IS NULL) FORMAT TabSeparated"
    )


def _match_pct(a, b):
    """Symmetric match percentage between two counts. None if either is missing."""
    if a is None or b is None:
        return None
    if max(a, b) == 0:
        return 100.0
    return min(a, b) / max(a, b) * 100


def run_clickhouse_checks(client_id, date_str):
    """
    Step 5: Postgres data.orders_{id}  vs  ClickHouse data.orders_{id}  (sync check)
    Step 6: ClickHouse data.orders_{id}  vs  reporting.orders_enriched_{id}  (enrichment check)

    All counts exclude test orders, since orders_enriched only contains non-test orders.
    A missing table (None count) is treated as a failure. Returns a dict of counts,
    percentages, and per-step pass flags plus an overall ch_pass.
    """
    pg_orders = get_pg_orders_nontest_count(client_id, date_str)
    ch_orders = get_ch_orders_count(client_id, date_str)
    enriched = get_ch_enriched_count(client_id, date_str)

    sync_pct = _match_pct(pg_orders, ch_orders)
    enriched_pct = _match_pct(ch_orders, enriched)

    sync_pass = sync_pct is not None and sync_pct >= (CH_SYNC_THRESHOLD * 100)
    enriched_pass = enriched_pct is not None and enriched_pct >= (CH_ENRICHED_THRESHOLD * 100)

    print(f"\n--- Step 5: Postgres vs ClickHouse (data.orders sync) ---")
    print(f"Postgres data.orders:    {pg_orders if pg_orders is not None else 'N/A (missing)'}")
    print(f"ClickHouse data.orders:  {ch_orders if ch_orders is not None else 'N/A (missing)'}")
    print(f"Match:                   {f'{sync_pct:.2f}%' if sync_pct is not None else 'N/A'}   "
          f"Threshold: {CH_SYNC_THRESHOLD * 100}%")
    print(f"Status:                  {'PASS' if sync_pass else 'FAIL'}")

    print(f"\n--- Step 6: ClickHouse data.orders vs orders_enriched ---")
    print(f"ClickHouse data.orders:  {ch_orders if ch_orders is not None else 'N/A (missing)'}")
    print(f"orders_enriched (orders):{enriched if enriched is not None else 'N/A (missing)'}")
    print(f"Match:                   {f'{enriched_pct:.2f}%' if enriched_pct is not None else 'N/A'}   "
          f"Threshold: {CH_ENRICHED_THRESHOLD * 100}%")
    print(f"Status:                  {'PASS' if enriched_pass else 'FAIL'}")

    return {
        "ch_pg_orders": pg_orders,
        "ch_orders": ch_orders,
        "ch_enriched": enriched,
        "ch_sync_pct": round(sync_pct, 2) if sync_pct is not None else None,
        "ch_enriched_pct": round(enriched_pct, 2) if enriched_pct is not None else None,
        "ch_sync_pass": sync_pass,
        "ch_enriched_pass": enriched_pass,
        "ch_pass": sync_pass and enriched_pass,
    }


def update_data_verified_status(client_id, is_verified):
    """
    Update the data_verified column in beast_insights_v2.clients_pipeline_status table.
    """
    try:
        conn = psycopg2.connect(
            user=PSG_USER,
            password=PSG_PASSWORD,
            host=PSG_HOST,
            port=PSG_PORT,
            database=PSG_DATABASE,
        )
        cur = conn.cursor()

        sql = """
            UPDATE beast_insights_v2.clients_pipeline_status
            SET data_verified = %s
            WHERE client_id = %s
        """
        cur.execute(sql, (is_verified, client_id))
        conn.commit()

        rows_updated = cur.rowcount
        if rows_updated > 0:
            print(f"Updated data_verified to {is_verified} for client_id {client_id}")
        else:
            print(f"No rows updated for client_id {client_id} in clients_pipeline_status")

        return rows_updated > 0

    except Exception as e:
        print(f"Error updating data_verified status: {e}")
        return False
    finally:
        cur.close()
        conn.close()


PAYSIGHT_THRESHOLD = 0.90  # paysight.py passes at 90% on both sub-checks


def _verify_paysight(crm, target_date, date_str, dry_run, update_status=True):
    """Mirror paysight.py: dual API↔DB check (raw paysight table + filtered orders table)
    with 90% threshold; PASS only if both checks pass."""
    print(f"\n--- Paysight: fetching transactions for {target_date.strftime('%Y-%m-%d')} ---")
    crm_total, crm_orders = _paysight_counts(crm, date_str)
    if crm_total is None:
        print("FAILED: Could not fetch CRM transaction count")
        if update_status and not dry_run:
            update_data_verified_status(crm.CLIENT_ID, False)
        return {"status": "ERROR", "client_id": crm.CLIENT_ID, "client_name": crm.CLIENT_NAME, "date": date_str, "error": "Failed to fetch CRM transaction count"}

    print(f"CRM Total:  {crm_total}   CRM Orders (filtered): {crm_orders}")

    paysight_count = get_paysight_raw_db_count(crm.CLIENT_ID, date_str)
    orders_count = get_paysight_orders_db_count(crm.CLIENT_ID, date_str)
    if paysight_count is None or orders_count is None:
        print("FAILED: Could not fetch database counts")
        if update_status and not dry_run:
            update_data_verified_status(crm.CLIENT_ID, False)
        return {"status": "ERROR", "client_id": crm.CLIENT_ID, "client_name": crm.CLIENT_NAME, "date": date_str, "error": "Failed to fetch database counts"}

    paysight_pct = (paysight_count / crm_total * 100) if crm_total > 0 else (100.0 if paysight_count == 0 else 0.0)
    orders_pct = (orders_count / crm_orders * 100) if crm_orders > 0 else (100.0 if orders_count == 0 else 0.0)
    threshold_pct = PAYSIGHT_THRESHOLD * 100
    paysight_pass = paysight_pct >= threshold_pct
    orders_pass = orders_pct >= threshold_pct
    is_verified = paysight_pass and orders_pass
    status = "PASS" if is_verified else "FAIL"

    print(f"\n[paysight {crm.CLIENT_ID}] {date_str}:")
    print(f"  data.paysight_{crm.CLIENT_ID}: {paysight_count}/{crm_total} ({paysight_pct:.2f}%)  {'PASS' if paysight_pass else 'FAIL'}")
    print(f"  data.orders_{crm.CLIENT_ID}:   {orders_count}/{crm_orders} ({orders_pct:.2f}%)  {'PASS' if orders_pass else 'FAIL'}")
    print(f"  Threshold: {threshold_pct}%   Status: {status}")

    if update_status and not dry_run:
        update_data_verified_status(crm.CLIENT_ID, bool(is_verified))

    # api_db_pct uses the orders comparison (the more meaningful one) so the existing column doesn't break
    return {
        "status": status,
        "client_id": crm.CLIENT_ID,
        "client_name": crm.CLIENT_NAME,
        "date": date_str,
        "crm_count": crm_orders,            # treat filtered orders as the headline API number
        "crm_total": crm_total,             # also expose unfiltered total
        "blob_total": None,
        "blob_non_test": None,
        "blob_test": None,
        "db_count": orders_count,
        "paysight_db_count": paysight_count,
        "paysight_pct": round(paysight_pct, 2),
        "orders_pct": round(orders_pct, 2),
        "api_db_pct": round(orders_pct, 2),
        "is_verified": is_verified,
    }


def _verify_core(crm, target_date=None, dry_run=False, update_status=True):
    """
    Verify data through the CRM-specific pipeline (API / Blob / DB).
    Step 1: API count vs Blob count (deduplicated)
    Step 2: Blob count (minus test orders) vs DB count

    When update_status is False, this does not write data_verified — the caller
    (verify_data) does so once, after the ClickHouse steps, using the combined result.
    """
    if target_date is None:
        target_date = datetime.now() - timedelta(days=1)

    date_str = target_date.strftime("%m/%d/%Y")

    print(f"\n{'='*60}")
    print(f"Data Verification for {crm.CLIENT_NAME} (Client ID: {crm.CLIENT_ID})")
    print(f"Date: {date_str}")
    print(f"{'='*60}")

    crm_name_early = (getattr(crm, "CRM_NAME", "") or "").strip().lower()
    if crm_name_early == "paysight":
        return _verify_paysight(crm, target_date, date_str, dry_run, update_status)

    # Step 1: Get CRM API order count
    print(f"\n--- Step 1: CRM API Order Count ---")
    crm_count, _ = get_crm_order_count(crm, date_str)
    if crm_count is None:
        print("FAILED: Could not fetch CRM order count")
        if update_status and not dry_run:
            update_data_verified_status(crm.CLIENT_ID, False)
        return {"status": "ERROR", "client_id": crm.CLIENT_ID, "client_name": crm.CLIENT_NAME, "date": date_str, "error": "Failed to fetch CRM order count"}

    print(f"CRM API Order Count: {crm_count}")

    # Non-Sticky CRMs: 2-way API vs DB check (no processed-blob layer to inspect)
    crm_name = (getattr(crm, "CRM_NAME", "") or "").strip().lower()
    if crm_name != "sticky":
        print(f"\n--- Step 2: API vs DB Comparison ({crm.CRM_NAME}) ---")
        if crm_name == "konnektive":
            db_count = get_konnektive_db_count(crm.CLIENT_ID, date_str)
        elif crm_name == "vrio":
            db_count = get_vrio_db_count(crm.CLIENT_ID, date_str)
        else:
            db_count = get_db_order_count(crm.CLIENT_ID, date_str)
        if db_count is None:
            print("FAILED: Could not fetch database order count")
            if update_status and not dry_run:
                update_data_verified_status(crm.CLIENT_ID, False)
            return {"status": "ERROR", "client_id": crm.CLIENT_ID, "client_name": crm.CLIENT_NAME, "date": date_str, "error": "Failed to fetch database order count"}

        if max(crm_count, db_count) > 0:
            api_db_pct = min(crm_count, db_count) / max(crm_count, db_count) * 100
        else:
            api_db_pct = 100.0

        api_db_pass = api_db_pct >= (BLOB_DB_THRESHOLD * 100)
        print(f"CRM API Count:   {crm_count}")
        print(f"DB Order Count:  {db_count}")
        print(f"Match:           {api_db_pct:.2f}%   Threshold: {BLOB_DB_THRESHOLD * 100}%")
        print(f"Status:          {'PASS' if api_db_pass else 'FAIL'}")
        if not api_db_pass:
            print(f"Difference:      {abs(crm_count - db_count)}")

        status = "PASS" if api_db_pass else "FAIL"
        print(f"\n{'='*60}\nVerification Status:     {status}\n{'='*60}\n")
        if update_status and not dry_run:
            update_data_verified_status(crm.CLIENT_ID, bool(api_db_pass))
        return {
            "status": status,
            "client_id": crm.CLIENT_ID,
            "client_name": crm.CLIENT_NAME,
            "date": date_str,
            "crm_count": crm_count,
            "blob_total": None,
            "blob_non_test": None,
            "blob_test": None,
            "db_count": db_count,
            "api_db_pct": round(api_db_pct, 2),
            "is_verified": api_db_pass,
        }

    # Step 2: Get Blob order count (deduplicated)
    print(f"\n--- Step 2: Azure Blob Order Count ---")
    blob_total, blob_non_test, blob_test, blob_non_test_df = get_blob_order_data(crm.CLIENT_ID, target_date)
    if blob_total is None:
        print("FAILED: Could not fetch blob data")
        if update_status and not dry_run:
            update_data_verified_status(crm.CLIENT_ID, False)
        return {"status": "ERROR", "client_id": crm.CLIENT_ID, "client_name": crm.CLIENT_NAME, "date": date_str, "error": "Failed to fetch blob data"}

    # Step 3: Compare API count with Blob count (99% threshold)
    print(f"\n--- Step 3: API vs Blob Comparison ---")
    print(f"CRM API Count:           {crm_count}")
    print(f"Blob Total (deduped):    {blob_total}")

    # Blob >= API is acceptable (extra line items / products per order)
    # Only fail if blob is missing data compared to API
    if crm_count > 0:
        api_blob_pct = (blob_total / crm_count) * 100
    else:
        api_blob_pct = 100.0 if blob_total == 0 else 0.0

    api_blob_pass = api_blob_pct >= (API_BLOB_THRESHOLD * 100)
    print(f"Blob Coverage:           {api_blob_pct:.2f}%")
    print(f"Threshold:               {API_BLOB_THRESHOLD * 100}%")
    print(f"Status:                  {'PASS' if api_blob_pass else 'FAIL'}")

    if not api_blob_pass:
        diff = crm_count - blob_total
        print(f"Missing in Blob:         {diff}")
        print("FAILED: Blob coverage below threshold")
        if update_status and not dry_run:
            update_data_verified_status(crm.CLIENT_ID, False)
        return {
            "status": "FAIL",
            "client_id": crm.CLIENT_ID,
            "client_name": crm.CLIENT_NAME,
            "date": date_str,
            "crm_count": crm_count,
            "blob_total": blob_total,
            "api_blob_pct": api_blob_pct,
            "step_failed": "API vs Blob",
            "is_verified": False,
        }

    # Step 4: Compare Blob non-test count with DB count.
    # Match by order_id set (not date_of_sale) so rows whose date_of_sale was
    # shifted by a later raw_updated re-fetch still match.
    print(f"\n--- Step 4: Blob (non-test) vs DB Comparison ---")
    blob_order_ids = (
        blob_non_test_df["ORDER_ID"].astype(str).tolist()
        if blob_non_test_df is not None and "ORDER_ID" in blob_non_test_df.columns
        else []
    )
    db_count = get_db_order_count_by_ids(crm.CLIENT_ID, blob_order_ids)
    if db_count is None:
        print("FAILED: Could not fetch database order count")
        if update_status and not dry_run:
            update_data_verified_status(crm.CLIENT_ID, False)
        return {"status": "ERROR", "client_id": crm.CLIENT_ID, "client_name": crm.CLIENT_NAME, "date": date_str, "error": "Failed to fetch database order count"}

    print(f"Blob Non-Test Count:     {blob_non_test}")
    print(f"Blob Test Orders:        {blob_test}")
    print(f"DB Order Count:          {db_count}")

    if blob_non_test > 0:
        blob_db_pct = min(blob_non_test, db_count) / max(blob_non_test, db_count) * 100
    else:
        blob_db_pct = 100.0 if db_count == 0 else 0.0

    blob_db_pass = blob_db_pct >= (BLOB_DB_THRESHOLD * 100)
    print(f"Match Percentage:        {blob_db_pct:.2f}%")
    print(f"Threshold:               {BLOB_DB_THRESHOLD * 100}%")
    print(f"Status:                  {'PASS' if blob_db_pass else 'FAIL'}")

    if not blob_db_pass:
        diff = abs(blob_non_test - db_count)
        print(f"Difference:              {diff}")

    is_verified = blob_db_pass
    status = "PASS" if is_verified else "FAIL"

    print(f"\n{'='*60}")
    print(f"Verification Status:     {status}")
    print(f"{'='*60}\n")

    if update_status and not dry_run:
        update_data_verified_status(crm.CLIENT_ID, bool(is_verified))

    api_db_pct = (
        round(min(crm_count, db_count) / max(crm_count, db_count) * 100, 2)
        if max(crm_count, db_count) > 0 else 100.0
    )

    return {
        "status": status,
        "client_id": crm.CLIENT_ID,
        "client_name": crm.CLIENT_NAME,
        "date": date_str,
        "crm_count": crm_count,
        "blob_total": blob_total,
        "blob_non_test": blob_non_test,
        "blob_test": blob_test,
        "db_count": db_count,
        "api_db_pct": api_db_pct,
        "is_verified": is_verified,
    }


def verify_data(crm, target_date=None, dry_run=False):
    """
    Full verification for one (client, date):
      1-4. CRM-specific pipeline (API / Blob / DB)  — via _verify_core
      5.   Postgres data.orders  vs  ClickHouse data.orders   (sync check)
      6.   ClickHouse data.orders  vs  orders_enriched         (enrichment check)

    The overall status is PASS only if the core pipeline AND both ClickHouse steps
    pass. data_verified is written once here, based on the combined result.
    """
    if target_date is None:
        target_date = datetime.now() - timedelta(days=1)
    date_str = target_date.strftime("%m/%d/%Y")

    # Core CRM pipeline — defer the data_verified write to us (after CH steps)
    core = _verify_core(crm, target_date, dry_run=dry_run, update_status=False)

    # If the core pipeline couldn't fetch its inputs, there's nothing to reconcile
    # against ClickHouse — persist the failure and return.
    if core.get("status") == "ERROR":
        if not dry_run:
            update_data_verified_status(crm.CLIENT_ID, False)
        return core

    core_pass = core.get("status") == "PASS"

    # Steps 5 & 6: ClickHouse sync + enrichment reconciliation
    ch = run_clickhouse_checks(crm.CLIENT_ID, date_str)

    final_verified = core_pass and ch["ch_pass"]
    status = "PASS" if final_verified else "FAIL"

    print(f"\n{'='*60}")
    print(f"Overall Verification (incl. ClickHouse):  {status}")
    print(f"{'='*60}\n")

    if not dry_run:
        update_data_verified_status(crm.CLIENT_ID, bool(final_verified))

    # Merge ClickHouse results in and override the headline status/flag
    core.update(ch)
    core["status"] = status
    core["is_verified"] = final_verified
    core["core_status"] = "PASS" if core_pass else "FAIL"
    return core


def main():
    parser = argparse.ArgumentParser(
        description="Verify order data between CRM API, Azure Blob, and database."
    )
    parser.add_argument(
        "--client_id", type=str, help="Client ID for verification (default: all active clients)"
    )
    parser.add_argument(
        "--crm", type=str, default="Sticky", help="CRM name (default: Sticky)"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Date to verify in MM/DD/YYYY format (default: yesterday)",
    )
    parser.add_argument(
        "--start_date",
        type=str,
        help="Start date for range verification in MM/DD/YYYY format",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        help="End date for range verification in MM/DD/YYYY format",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run verification without updating the database",
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export results to an xlsx report (default: no file written)",
    )
    args = parser.parse_args()

    # Build list of dates to verify
    dates_to_verify = []
    if args.start_date and args.end_date:
        try:
            start = datetime.strptime(args.start_date, "%m/%d/%Y")
            end = datetime.strptime(args.end_date, "%m/%d/%Y")
        except ValueError:
            print("Invalid date format. Please use MM/DD/YYYY format.")
            return
        current = start
        while current <= end:
            dates_to_verify.append(current)
            current += timedelta(days=1)
    elif args.date:
        try:
            dates_to_verify.append(datetime.strptime(args.date, "%m/%d/%Y"))
        except ValueError:
            print(f"Invalid date format: {args.date}. Please use MM/DD/YYYY format.")
            return
    else:
        dates_to_verify.append(datetime.now() - timedelta(days=1))

    crm_list = get_crm_credentials(args.crm, args.client_id)

    if not crm_list:
        print(f"No CRM credentials found" + (f" for client_id: {args.client_id}" if args.client_id else f" for CRM: {args.crm}"))
        return

    # Deduplicate by CLIENT_ID (in case multiple credentials per client)
    seen_clients = set()
    unique_crm_list = []
    for crm in crm_list:
        if crm.CLIENT_ID not in seen_clients:
            seen_clients.add(crm.CLIENT_ID)
            unique_crm_list.append(crm)
    crm_list = unique_crm_list

    print(f"Verifying {len(crm_list)} client(s) for {len(dates_to_verify)} date(s)...\n")

    filename = f"sticky_verification_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx" if args.export else None
    all_results = []

    for crm in crm_list:
        print(f"\n{'#'*60}")
        print(f"# Client: {crm.CLIENT_NAME} (ID: {crm.CLIENT_ID})")
        print(f"{'#'*60}")

        # Run all dates for this client in parallel
        client_results = []
        if len(dates_to_verify) > 1:
            max_workers = min(len(dates_to_verify), 3)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(verify_data, crm, d, dry_run=args.dry_run): d for d in dates_to_verify}
                for future in as_completed(futures):
                    client_results.append(future.result())
            client_results.sort(key=lambda r: r.get("date", ""))
        else:
            for d in dates_to_verify:
                client_results.append(verify_data(crm, d, dry_run=args.dry_run))

        all_results.extend(client_results)

        # Print client summary
        passed = sum(1 for r in client_results if r.get("status") == "PASS")
        failed = sum(1 for r in client_results if r.get("status") == "FAIL")
        errors = sum(1 for r in client_results if r.get("status") == "ERROR")
        print(f"\n--- {crm.CLIENT_NAME}: Passed={passed} | Failed={failed} | Errors={errors} ---")

        # Update Excel after each client (only if --export)
        if args.export:
            excel_data = []
            for r in all_results:
                crm_count = r.get("crm_count")
                blob_total = r.get("blob_total")
                blob_non_test = r.get("blob_non_test")
                db_count = r.get("db_count")
                excel_data.append({
                    "Client ID": r.get("client_id"),
                    "Client Name": r.get("client_name"),
                    "Date": r.get("date"),
                    "API Count": crm_count,
                    "Blob Count": blob_total,
                    "Test Orders": r.get("blob_test"),
                    "Non-Test Orders": blob_non_test,
                    "DB Count": db_count,
                    "API→Blob %": round((blob_total / crm_count) * 100, 2) if isinstance(crm_count, int) and isinstance(blob_total, int) and crm_count > 0 else None,
                    "Blob→DB %": round(min(blob_non_test, db_count) / max(blob_non_test, db_count) * 100, 2) if isinstance(blob_non_test, int) and isinstance(db_count, int) and max(blob_non_test, db_count) > 0 else None,
                    "PG Orders": r.get("ch_pg_orders"),
                    "CH Orders": r.get("ch_orders"),
                    "PG→CH %": r.get("ch_sync_pct"),
                    "Enriched": r.get("ch_enriched"),
                    "CH→Enriched %": r.get("ch_enriched_pct"),
                    "Status": r.get("status"),
                })
            df = pd.DataFrame(excel_data)
            df.to_excel(filename, index=False)
            print(f"Excel updated: {filename} ({len(all_results)} rows)")

    # Final Summary
    print("\n" + "=" * 110)
    print("FINAL VERIFICATION SUMMARY")
    print("=" * 110)

    passed = sum(1 for r in all_results if r.get("status") == "PASS")
    failed = sum(1 for r in all_results if r.get("status") == "FAIL")
    errors = sum(1 for r in all_results if r.get("status") == "ERROR")

    print(f"Clients: {len(crm_list)} | Days: {len(dates_to_verify)} | Total: {len(all_results)} | Passed: {passed} | Failed: {failed} | Errors: {errors}")

    print(f"\n{'Client':<10} {'Name':<22} {'Date':<14} {'API':>8} {'Blob':>8} {'Non-Test':>10} {'DB':>8} {'API→Blob':>10} {'Blob→DB':>10} {'PG.ord':>8} {'CH.ord':>8} {'PG→CH':>8} {'Enrich':>8} {'CH→Enr':>8} {'Status':<6}")
    print("-" * 150)
    for r in all_results:
        client_id = r.get("client_id", "-")
        client_name = str(r.get("client_name", "-"))[:20]
        date = r.get("date", "N/A")
        status = r.get("status", "N/A")
        crm_count = r.get("crm_count", "-")
        blob_total = r.get("blob_total", "-")
        blob_non_test = r.get("blob_non_test", "-")
        db_count = r.get("db_count", "-")

        if isinstance(crm_count, int) and isinstance(blob_total, int) and crm_count > 0:
            api_blob_pct = f"{(blob_total / crm_count) * 100:.1f}%"
        else:
            api_blob_pct = "-"

        if isinstance(blob_non_test, int) and isinstance(db_count, int) and max(blob_non_test, db_count) > 0:
            blob_db_pct = f"{min(blob_non_test, db_count) / max(blob_non_test, db_count) * 100:.1f}%"
        else:
            blob_db_pct = "-"

        pg_ord = r.get("ch_pg_orders")
        ch_ord = r.get("ch_orders")
        enriched = r.get("ch_enriched")
        sync_pct = r.get("ch_sync_pct")
        enr_pct = r.get("ch_enriched_pct")
        pg_ord_s = str(pg_ord) if pg_ord is not None else "-"
        ch_ord_s = str(ch_ord) if ch_ord is not None else "-"
        enriched_s = str(enriched) if enriched is not None else "-"
        sync_pct_s = f"{sync_pct:.1f}%" if isinstance(sync_pct, (int, float)) else "-"
        enr_pct_s = f"{enr_pct:.1f}%" if isinstance(enr_pct, (int, float)) else "-"

        print(f"{str(client_id):<10} {client_name:<22} {date:<14} {str(crm_count):>8} {str(blob_total):>8} {str(blob_non_test):>10} {str(db_count):>8} {api_blob_pct:>10} {blob_db_pct:>10} {pg_ord_s:>8} {ch_ord_s:>8} {sync_pct_s:>8} {enriched_s:>8} {enr_pct_s:>8} {status:<6}")

    print("=" * 150)
    if args.export:
        print(f"\nExcel report: {filename}")

    if failed > 0 or errors > 0:
        exit(1)


if __name__ == "__main__":
    main()
