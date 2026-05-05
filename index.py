"""
Data Verification Script
Verifies order data through a 3-step pipeline:
1. Fetch order count from CRM API
2. Fetch deduplicated order count from Azure Blob (parquet files)
3. Compare: API count == Blob count, then (Blob count - test orders) == DB count
"""

import argparse
import configparser
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

API_BLOB_THRESHOLD = 0.99  # 99% match between API and Blob
BLOB_DB_THRESHOLD = 0.99   # 99% match between Blob (non-test) and DB


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


def _paysight_order_count(crm, date_str):
    """Paysight POST /api/transactions/search: paginate (limit=1000) until moreResults=false."""
    url = f"{crm.CRM_HOST.rstrip('/')}/api/transactions/search" if "://" in (crm.CRM_HOST or "") else f"https://{crm.CRM_HOST}/api/transactions/search"
    headers = {
        "Authorization": crm.CRM_PASSWORD,
        "ClientId": crm.CRM_API_KEY,
        "UserEmail": crm.CRM_USERNAME,
        "Content-Type": "application/json",
    }
    sql_date = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    page = 1
    total = 0
    while True:
        body = {"pageNumber": page, "limit": 1000, "dateFrom": sql_date, "dateTo": sql_date, "dateCompleted": True}
        try:
            r = requests.post(url, json=body, headers=headers, timeout=60)
        except requests.RequestException as e:
            print(f"Error calling Paysight API (page {page}): {e}")
            return None, []
        if r.status_code != 200:
            print(f"Paysight API request failed (page {page}): {r.status_code}")
            return None, []
        data = r.json()
        txs = data.get("transactions") or []
        total += len(txs)
        if not data.get("moreResults"):
            return total, []
        page += 1
        if page > 100:
            print(f"Paysight API: aborting after 100 pages on {date_str} — possible runaway")
            return None, []


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


def get_blob_order_data(client_id, target_date):
    """
    Fetch and deduplicate order data from Azure Blob parquet files.
    Path: processed/sticky/{client_id}/{year}/{month}/{day}/*.parquet
    Deduplication key: (transaction_id, date_of_sale, order_id, product_id, client_id)
    Returns: (total_deduplicated_count, non_test_count, test_count, DataFrame)
    """
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")
    prefix = f"sticky/{client_id}/{year}/{month}/{day}/"

    if not BLOB_CONTAINER_URL:
        print("Error: [azure] BLOB_CONTAINER_URL is not set in config.ini — required for Sticky verification")
        return None, None, None, None

    try:
        container_client = ContainerClient.from_container_url(BLOB_CONTAINER_URL)

        # List all parquet files for the day
        blobs = list(container_client.list_blobs(name_starts_with=prefix))
        if not blobs:
            print(f"No blob files found at: {prefix}")
            return None, None, None, None

        print(f"Found {len(blobs)} parquet file(s) at: {prefix}")

        # Read and concatenate all parquet files
        dfs = []
        for blob in blobs:
            blob_client = container_client.get_blob_client(blob.name)
            data = blob_client.download_blob().readall()
            df = pd.read_parquet(pd.io.common.BytesIO(data))
            dfs.append(df)

        combined = pd.concat(dfs, ignore_index=True)
        print(f"Total rows across all files: {len(combined)}")

        # Filter to only the target date
        target_date_str = target_date.strftime("%Y-%m-%d")
        combined = combined[combined["DATE_OF_SALE"].astype(str) == target_date_str]
        print(f"Rows for {target_date_str}: {len(combined)}")

        # Normalize TRANSACTION_ID: NaN and empty string are treated as the same
        combined["TRANSACTION_ID"] = combined["TRANSACTION_ID"].fillna("")

        # Deduplicate based on unique key
        dedup_cols = ["TRANSACTION_ID", "DATE_OF_SALE", "ORDER_ID", "PRODUCT_ID", "CLIENT_ID"]
        deduped = combined.drop_duplicates(subset=dedup_cols)
        total_count = len(deduped)
        print(f"Deduplicated rows: {total_count}")

        # Identify test orders: IS_TEST == "True" or BILL_EMAIL contains 'test'
        test_mask = deduped["IS_TEST"].astype(str).str.lower().eq("true") | deduped["BILL_EMAIL"].str.contains("test", case=False, na=False)
        test_count = int(test_mask.sum())
        non_test_count = int(total_count - test_count)

        print(f"Test orders: {test_count}")
        print(f"Non-test orders: {non_test_count}")

        return total_count, non_test_count, test_count, deduped

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


def verify_data(crm, target_date=None, dry_run=False):
    """
    Verify data through 3-step pipeline:
    Step 1: API count vs Blob count (deduplicated)
    Step 2: Blob count (minus test orders) vs DB count
    """
    if target_date is None:
        target_date = datetime.now() - timedelta(days=1)

    date_str = target_date.strftime("%m/%d/%Y")

    print(f"\n{'='*60}")
    print(f"Data Verification for {crm.CLIENT_NAME} (Client ID: {crm.CLIENT_ID})")
    print(f"Date: {date_str}")
    print(f"{'='*60}")

    # Step 1: Get CRM API order count
    print(f"\n--- Step 1: CRM API Order Count ---")
    crm_count, _ = get_crm_order_count(crm, date_str)
    if crm_count is None:
        print("FAILED: Could not fetch CRM order count")
        if not dry_run:
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
            if not dry_run:
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
        if not dry_run:
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
    blob_total, blob_non_test, blob_test, _ = get_blob_order_data(crm.CLIENT_ID, target_date)
    if blob_total is None:
        print("FAILED: Could not fetch blob data")
        if not dry_run:
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
        if not dry_run:
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

    # Step 4: Compare Blob non-test count with DB count
    print(f"\n--- Step 4: Blob (non-test) vs DB Comparison ---")
    db_count = get_db_order_count(crm.CLIENT_ID, date_str)
    if db_count is None:
        print("FAILED: Could not fetch database order count")
        if not dry_run:
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

    if not dry_run:
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

    print(f"\n{'Client':<10} {'Name':<22} {'Date':<14} {'API':>8} {'Blob':>8} {'Test':>6} {'Non-Test':>10} {'DB':>8} {'API→Blob':>10} {'Blob→DB':>10} {'Status':<6}")
    print("-" * 110)
    for r in all_results:
        client_id = r.get("client_id", "-")
        client_name = str(r.get("client_name", "-"))[:20]
        date = r.get("date", "N/A")
        status = r.get("status", "N/A")
        crm_count = r.get("crm_count", "-")
        blob_total = r.get("blob_total", "-")
        blob_test = r.get("blob_test", "-")
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

        print(f"{str(client_id):<10} {client_name:<22} {date:<14} {str(crm_count):>8} {str(blob_total):>8} {str(blob_test):>6} {str(blob_non_test):>10} {str(db_count):>8} {api_blob_pct:>10} {blob_db_pct:>10} {status:<6}")

    print("=" * 110)
    if args.export:
        print(f"\nExcel report: {filename}")

    if failed > 0 or errors > 0:
        exit(1)


if __name__ == "__main__":
    main()
