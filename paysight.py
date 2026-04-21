"""
Data Verification Script - Paysight CRM
Compares the number of transactions from Paysight CRM API with the database for yesterday.
If the database count is at least 90% of the CRM count, verification passes.
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


# Configuration
config = configparser.ConfigParser()
config.read("config.ini")

PSG_USER = config.get("database", "PSG_USER")
PSG_PASSWORD = config.get("database", "PSG_PASSWORD")
PSG_HOST = config.get("database", "PSG_HOST")
PSG_PORT = int(config.get("database", "PSG_PORT"))
PSG_DATABASE = config.get("database", "PSG_DATABASE")

VERIFICATION_THRESHOLD = 0.90  # 90% threshold
PAGE_LIMIT = 1000
MAX_RETRIES = 5
RETRY_BACKOFF = [5, 15, 30, 60, 120]


def get_crm_credentials(client_id=None):
    """Fetch Paysight CRM credentials from the database. If client_id is None, fetch all active clients."""
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
            sql = """
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
                WHERE crm.name = 'Paysight' AND cl.id = %s;
            """
            cur.execute(sql, (client_id,))
        else:
            sql = """
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
                WHERE crm.name = 'Paysight'
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


def fetch_transactions_page(crm, date_from, date_to, page_number):
    """
    Fetch a single page of transactions from Paysight API with retry and backoff.
    Returns (transactions_list, more_results) tuple.
    """
    url = f"{crm.CRM_HOST}/api/transactions/search"

    headers = {
        "Authorization": crm.CRM_PASSWORD,
        "ClientId": str(crm.CRM_API_KEY),
        "Content-Type": "application/json",
        "UserEmail": crm.CRM_USERNAME,
    }

    payload = {
        "pageNumber": page_number,
        "limit": PAGE_LIMIT,
        "dateFrom": date_from,
        "dateTo": date_to,
        "dateCompleted": True,
    }

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=120)

            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    return data.get("transactions", []), data.get("moreResults", False)
                else:
                    print(f"API returned success=false: {data.get('message')}")
                    return [], False

            if response.status_code == 429 or response.status_code >= 500:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                print(f"Paysight API {response.status_code} (page {page_number}), retrying in {wait}s (attempt {attempt + 1}/{MAX_RETRIES})...")
                time.sleep(wait)
                continue

            print(f"Paysight API {response.status_code}: {response.text[:200]}")
            return [], False

        except requests.exceptions.Timeout:
            wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
            print(f"Paysight API timeout (page {page_number}), retrying in {wait}s (attempt {attempt + 1}/{MAX_RETRIES})...")
            time.sleep(wait)
            continue
        except Exception as e:
            print(f"Paysight API error (page {page_number}): {e}")
            return [], False

    print(f"Paysight API failed after {MAX_RETRIES} retries (page {page_number})")
    return [], False


def is_order_transaction(txn):
    """
    Check if a transaction should be counted as an order.
    Applies the same exclusion filters as sync_paysight_to_orders.py.

    API field mapping:
      alertSource: "Unassigned" = no alert (DB NULL), anything else = alert
      refundSource: "Unassigned" = no refund (DB NULL), anything else = refund
      application: maps to DB application_name
      success: maps to DB is_approved
      originalTransactionId: maps to DB ancestor_order_id
      chargedBack: maps to DB is_chargeback
      refunded: maps to DB is_refund
    """
    alert_source = txn.get("alertSource")
    refund_source = txn.get("refundSource")
    application = txn.get("application", "")

    # Normalize "Unassigned" to None (matches DB NULL)
    if alert_source == "Unassigned":
        alert_source = None
    if refund_source == "Unassigned":
        refund_source = None

    # Exclude alerts (alertsource IS NOT NULL)
    if alert_source is not None:
        return False

    # Exclude Visa Fraud (TC40)
    if application == "Visa Fraud (TC40)":
        return False

    ancestor = txn.get("originalTransactionId")
    is_approved = txn.get("success", False)
    is_refund = txn.get("refunded", False)
    is_chargeback = txn.get("chargedBack", False)

    # Exclude refund child rows with ancestor
    # Note: API returns success=True for completed refunds, but DB stores
    # is_approved=False. So we exclude all refund child rows with ancestor
    # when they have a refund source or refunded flag, regardless of success.
    if ancestor and (refund_source is not None or is_refund):
        return False

    # Exclude chargeback rows with ancestor (not approved)
    if ancestor and is_chargeback and not is_approved:
        return False

    # Exclude orphan chargebacks (no ancestor, not approved)
    if not ancestor and is_chargeback and not is_approved:
        return False

    return True


def get_crm_order_count(crm, date_str):
    """
    Fetch the count of transactions from Paysight CRM API for a specific date.
    Uses pagination to fetch all transactions.
    Returns (total_count, filtered_order_count).
    """
    total_count = 0
    order_count = 0
    page = 1

    while True:
        transactions, more_results = fetch_transactions_page(crm, date_str, date_str, page)

        if page == 1 and not transactions and not more_results:
            return None, None

        total_count += len(transactions)
        order_count += sum(1 for txn in transactions if is_order_transaction(txn))
        print(f"  Page {page}: {len(transactions)} records (total so far: {total_count})")

        if not more_results:
            break

        page += 1
        time.sleep(1)  # pace requests to avoid rate limit

    return total_count, order_count


def get_db_paysight_count(client_id, date_str):
    """
    Fetch the count of records from data.paysight_{client_id} for a specific date.
    Uses end_date = '9999-12-31' filter for current records.
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

        date_obj = datetime.strptime(date_str, "%m/%d/%Y")
        sql_date = date_obj.strftime("%Y-%m-%d")

        table_name = f"data.paysight_{client_id}"
        sql = f"""
            SELECT COUNT(1)
            FROM {table_name}
            WHERE date_of_sale::date = %s
              AND end_date = '9999-12-31'
        """
        cur.execute(sql, (sql_date,))
        result = cur.fetchone()
        count = result[0] if result else 0

        return count

    except Exception as e:
        print(f"Error fetching paysight count from database: {e}")
        return None
    finally:
        cur.close()
        conn.close()


def get_db_orders_count(client_id, date_str):
    """
    Fetch the count of records from data.orders_{client_id} for a specific date.
    Uses end_date = '9999-12-31' filter for current records.
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

        date_obj = datetime.strptime(date_str, "%m/%d/%Y")
        sql_date = date_obj.strftime("%Y-%m-%d")

        table_name = f"data.orders_{client_id}"
        sql = f"""
            SELECT COUNT(1)
            FROM {table_name}
            WHERE date_of_sale::date = %s
              AND end_date = '9999-12-31'
        """
        cur.execute(sql, (sql_date,))
        result = cur.fetchone()
        count = result[0] if result else 0

        return count

    except Exception as e:
        print(f"Error fetching orders count from database: {e}")
        return None
    finally:
        cur.close()
        conn.close()


def update_data_verified_status(client_id, is_verified):
    """
    Update the data_verified column in beast_insights_v2.clients_pipeline_status table.
    Sets to True if match >= 90%, else False.
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
    Verify the data by comparing Paysight CRM transaction count with database count.
    Returns verification result with details.
    """
    # Use yesterday's date if no date provided
    if target_date is None:
        target_date = datetime.now() - timedelta(days=1)

    date_str = target_date.strftime("%m/%d/%Y")
    api_date_str = target_date.strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print(f"Data Verification for {crm.CLIENT_NAME} (Client ID: {crm.CLIENT_ID})")
    print(f"CRM: Paysight")
    print(f"Date: {date_str}")
    print(f"{'='*60}")

    # Get CRM transaction count
    print(f"\nFetching transactions from Paysight API...")
    crm_total, crm_orders = get_crm_order_count(crm, api_date_str)
    if crm_total is None:
        print("FAILED: Could not fetch CRM transaction count")
        return {
            "status": "ERROR",
            "client_id": crm.CLIENT_ID,
            "client_name": crm.CLIENT_NAME,
            "date": date_str,
            "error": "Failed to fetch CRM transaction count",
        }

    # Get database counts from both tables
    paysight_count = get_db_paysight_count(crm.CLIENT_ID, date_str)
    if paysight_count is None:
        print("FAILED: Could not fetch paysight table count")
        return {
            "status": "ERROR",
            "client_id": crm.CLIENT_ID,
            "client_name": crm.CLIENT_NAME,
            "date": date_str,
            "error": "Failed to fetch paysight table count",
        }

    orders_count = get_db_orders_count(crm.CLIENT_ID, date_str)
    if orders_count is None:
        print("FAILED: Could not fetch orders table count")
        return {
            "status": "ERROR",
            "client_id": crm.CLIENT_ID,
            "client_name": crm.CLIENT_NAME,
            "date": date_str,
            "error": "Failed to fetch orders table count",
        }

    # Calculate match percentages
    if crm_total > 0:
        paysight_match = (paysight_count / crm_total) * 100
    else:
        paysight_match = 100.0 if paysight_count == 0 else 0.0

    if crm_orders > 0:
        orders_match = (orders_count / crm_orders) * 100
    else:
        orders_match = 100.0 if orders_count == 0 else 0.0

    # Verification passes if both tables meet the threshold
    paysight_pass = paysight_match >= (VERIFICATION_THRESHOLD * 100)
    orders_pass = orders_match >= (VERIFICATION_THRESHOLD * 100)
    is_verified = paysight_pass and orders_pass
    status = "PASS" if is_verified else "FAIL"

    # Print results
    print(f"\nCRM Total Transactions:      {crm_total}")
    print(f"CRM Orders (filtered):       {crm_orders}")
    print(f"CRM Excluded (alerts/refunds/chargebacks): {crm_total - crm_orders}")
    print(f"\n--- data.paysight_{crm.CLIENT_ID} (API Total vs Raw Table) ---")
    print(f"DB Paysight Count:           {paysight_count}")
    print(f"Match Percentage:            {paysight_match:.2f}%  {'PASS' if paysight_pass else 'FAIL'}")
    print(f"\n--- data.orders_{crm.CLIENT_ID} (API Filtered vs Orders Table) ---")
    print(f"DB Orders Count:             {orders_count}")
    print(f"Match Percentage:            {orders_match:.2f}%  {'PASS' if orders_pass else 'FAIL'}")
    print(f"\nThreshold:                   {VERIFICATION_THRESHOLD * 100}%")
    print(f"Verification Status:         {status}")
    print(f"{'='*60}\n")

    # Update data_verified status in clients_pipeline_status table
    update_data_verified_status(crm.CLIENT_ID, is_verified)

    return {
        "status": status,
        "client_id": crm.CLIENT_ID,
        "client_name": crm.CLIENT_NAME,
        "date": date_str,
        "crm_total": crm_total,
        "crm_orders": crm_orders,
        "paysight_count": paysight_count,
        "orders_count": orders_count,
        "paysight_match": paysight_match,
        "orders_match": orders_match,
        "threshold": VERIFICATION_THRESHOLD * 100,
        "is_verified": is_verified,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Verify transaction data between Paysight CRM and database."
    )
    parser.add_argument(
        "--client_id", type=str, help="Client ID for verification (default: all active Paysight clients)"
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

    # Get CRM credentials for Paysight
    crm_list = get_crm_credentials(args.client_id)

    if not crm_list:
        print(f"No Paysight CRM credentials found" + (f" for client_id: {args.client_id}" if args.client_id else ""))
        return

    print(f"Verifying {len(crm_list)} client(s) for {len(dates_to_verify)} date(s)...\n")

    # Build all (crm, date) tasks (Paysight uses max 3 workers to avoid rate limits)
    tasks = [(crm, d) for d in dates_to_verify for crm in crm_list]

    results = []
    if len(tasks) > 1:
        max_workers = min(len(tasks), 3)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(verify_data, crm, d): (crm, d) for crm, d in tasks}
            for future in as_completed(futures):
                results.append(future.result())
        results.sort(key=lambda r: (str(r.get("client_id", "")), r.get("date", "")))
    else:
        for crm, d in tasks:
            results.append(verify_data(crm, d))

    # Summary
    print("\n" + "=" * 120)
    print("VERIFICATION SUMMARY")
    print("=" * 120)

    passed = sum(1 for r in results if r.get("status") == "PASS")
    failed = sum(1 for r in results if r.get("status") == "FAIL")
    errors = sum(1 for r in results if r.get("status") == "ERROR")

    print(f"Clients: {len(crm_list)} | Days: {len(dates_to_verify)} | Passed: {passed} | Failed: {failed} | Errors: {errors}")

    # Date-wise breakdown
    print(f"\n{'Client':<10} {'Name':<22} {'Date':<14} {'API Total':>10} {'API Orders':>11} {'Paysight DB':>12} {'Orders DB':>10} {'Pay%':>8} {'Ord%':>8} {'Status':<6}")
    print("-" * 120)
    for r in results:
        client_id = r.get("client_id", "-")
        client_name = str(r.get("client_name", "-"))[:20]
        date = r.get("date", "N/A")
        status = r.get("status", "N/A")
        crm_total = r.get("crm_total", "-")
        crm_orders = r.get("crm_orders", "-")
        paysight_count = r.get("paysight_count", "-")
        orders_count = r.get("orders_count", "-")
        pay_pct = r.get("paysight_match")
        ord_pct = r.get("orders_match")
        pay_str = f"{pay_pct:.1f}%" if pay_pct is not None else "-"
        ord_str = f"{ord_pct:.1f}%" if ord_pct is not None else "-"
        print(f"{str(client_id):<10} {client_name:<22} {date:<14} {str(crm_total):>10} {str(crm_orders):>11} {str(paysight_count):>12} {str(orders_count):>10} {pay_str:>8} {ord_str:>8} {status:<6}")
    print("=" * 120)

    # Export to Excel (only if --export)
    if args.export:
        excel_data = []
        for r in results:
            excel_data.append({
                "Client ID": r.get("client_id"),
                "Client Name": r.get("client_name"),
                "Date": r.get("date"),
                "API Total": r.get("crm_total"),
                "API Orders": r.get("crm_orders"),
                "Paysight DB": r.get("paysight_count"),
                "Orders DB": r.get("orders_count"),
                "Paysight Match %": r.get("paysight_match"),
                "Orders Match %": r.get("orders_match"),
                "Status": r.get("status"),
            })
        df = pd.DataFrame(excel_data)
        filename = f"paysight_verification_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        df.to_excel(filename, index=False)
        print(f"\nExcel report saved: {filename}")

    if failed > 0 or errors > 0:
        exit(1)


if __name__ == "__main__":
    main()
