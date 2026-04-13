"""
Data Verification Script
Verifies order data through a 3-step pipeline:
1. Fetch order count from CRM API
2. Fetch deduplicated order count from Azure Blob (parquet files)
3. Compare: API count == Blob count, then (Blob count - test orders) == DB count
"""

import argparse
import configparser
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

BLOB_CONTAINER_URL = config.get("azure", "BLOB_CONTAINER_URL")

API_BLOB_THRESHOLD = 0.99  # 99% match between API and Blob
BLOB_DB_THRESHOLD = 0.99   # 99% match between Blob (non-test) and DB


def get_crm_credentials(crm_name, client_id):
    """Fetch CRM credentials from the database."""
    try:
        conn = psycopg2.connect(
            user=PSG_USER,
            password=PSG_PASSWORD,
            host=PSG_HOST,
            port=PSG_PORT,
            database=PSG_DATABASE,
        )
        cur = conn.cursor()

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


def get_crm_order_count(crm, date_str):
    """
    Fetch the count of orders from CRM API for a specific date.
    """
    order_find_url = f"https://{crm.CRM_HOST}/api/v1/order_find"

    order_find_payload = {
        "campaign_id": "all",
        "start_date": date_str,
        "start_time": "00:00:00",
        "end_date": date_str,
        "end_time": "23:59:59",
        "date_type": "create",
        "criteria": "all",
    }

    try:
        response = requests.post(
            order_find_url,
            json=order_find_payload,
            auth=(crm.CRM_USERNAME, crm.CRM_PASSWORD),
            timeout=60,
        )

        if response.status_code == 200:
            data = response.json()
            order_ids = data.get("order_id", [])
            total_count = len(order_ids)
            return total_count, order_ids
        else:
            print(f"CRM API request failed with status code: {response.status_code}")
            return None, []

    except requests.RequestException as e:
        print(f"Error calling CRM API: {e}")
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

        # Deduplicate based on unique key
        dedup_cols = ["TRANSACTION_ID", "DATE_OF_SALE", "ORDER_ID", "PRODUCT_ID", "CLIENT_ID"]
        deduped = combined.drop_duplicates(subset=dedup_cols)
        total_count = len(deduped)
        print(f"Deduplicated rows: {total_count}")

        # Identify test orders: IS_TEST == "True" or BILL_EMAIL contains 'test'
        test_mask = deduped["IS_TEST"].astype(str).str.lower().eq("true") | deduped["BILL_EMAIL"].str.contains("test", case=False, na=False)
        test_count = test_mask.sum()
        non_test_count = total_count - test_count

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
    try:
        conn = psycopg2.connect(
            user=PSG_USER,
            password=PSG_PASSWORD,
            host=PSG_HOST,
            port=PSG_PORT,
            database=PSG_DATABASE,
        )
        cur = conn.cursor()

        # Convert date format from MM/DD/YYYY to YYYY-MM-DD for SQL
        date_obj = datetime.strptime(date_str, "%m/%d/%Y")
        sql_date = date_obj.strftime("%Y-%m-%d")

        table_name = f"data.orders_{client_id}"
        sql = f"""
            SELECT COUNT(1)
            FROM {table_name}
            WHERE date_of_sale = %s
              AND end_date = '9999-12-31'
        """
        cur.execute(sql, (sql_date,))
        result = cur.fetchone()
        count = result[0] if result else 0

        return count

    except Exception as e:
        print(f"Error fetching order count from database: {e}")
        return None
    finally:
        cur.close()
        conn.close()


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


def verify_data(crm, target_date=None):
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
        update_data_verified_status(crm.CLIENT_ID, False)
        return {"status": "ERROR", "client_id": crm.CLIENT_ID, "client_name": crm.CLIENT_NAME, "date": date_str, "error": "Failed to fetch CRM order count"}

    print(f"CRM API Order Count: {crm_count}")

    # Step 2: Get Blob order count (deduplicated)
    print(f"\n--- Step 2: Azure Blob Order Count ---")
    blob_total, blob_non_test, blob_test, _ = get_blob_order_data(crm.CLIENT_ID, target_date)
    if blob_total is None:
        print("FAILED: Could not fetch blob data")
        update_data_verified_status(crm.CLIENT_ID, False)
        return {"status": "ERROR", "client_id": crm.CLIENT_ID, "client_name": crm.CLIENT_NAME, "date": date_str, "error": "Failed to fetch blob data"}

    # Step 3: Compare API count with Blob count (99% threshold)
    print(f"\n--- Step 3: API vs Blob Comparison ---")
    print(f"CRM API Count:           {crm_count}")
    print(f"Blob Total (deduped):    {blob_total}")

    if crm_count > 0:
        api_blob_pct = min(crm_count, blob_total) / max(crm_count, blob_total) * 100
    else:
        api_blob_pct = 100.0 if blob_total == 0 else 0.0

    api_blob_pass = api_blob_pct >= (API_BLOB_THRESHOLD * 100)
    print(f"Match Percentage:        {api_blob_pct:.2f}%")
    print(f"Threshold:               {API_BLOB_THRESHOLD * 100}%")
    print(f"Status:                  {'PASS' if api_blob_pass else 'FAIL'}")

    if not api_blob_pass:
        diff = abs(crm_count - blob_total)
        print(f"Difference:              {diff}")
        print("FAILED: API vs Blob match below threshold")
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

    update_data_verified_status(crm.CLIENT_ID, bool(is_verified))

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
        "is_verified": is_verified,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Verify order data between CRM API, Azure Blob, and database."
    )
    parser.add_argument(
        "--client_id", type=str, required=True, help="Client ID for verification"
    )
    parser.add_argument(
        "--crm", type=str, default="Sticky", help="CRM name (default: Sticky)"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Date to verify in MM/DD/YYYY format (default: yesterday)",
    )
    args = parser.parse_args()

    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%m/%d/%Y")
        except ValueError:
            print(f"Invalid date format: {args.date}. Please use MM/DD/YYYY format.")
            return

    crm_list = get_crm_credentials(args.crm, args.client_id)

    if not crm_list:
        print(f"No CRM credentials found for client_id: {args.client_id}")
        return

    results = []
    for crm in crm_list:
        result = verify_data(crm, target_date)
        results.append(result)

    # Summary
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)

    passed = sum(1 for r in results if r.get("status") == "PASS")
    failed = sum(1 for r in results if r.get("status") == "FAIL")
    errors = sum(1 for r in results if r.get("status") == "ERROR")

    print(f"Total Verified: {len(results)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Errors: {errors}")

    if failed > 0 or errors > 0:
        exit(1)


if __name__ == "__main__":
    main()
