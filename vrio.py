"""
Data Verification Script - VRIO CRM
Compares the number of orders from VRIO CRM API with the database for yesterday.
If the database count is at least 90% of the CRM count, verification passes.
Test orders are excluded from the comparison.
"""

import argparse
import configparser
from datetime import datetime, timedelta
from types import SimpleNamespace

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

VRIO_API_URL = "https://api.vrio.app"
VERIFICATION_THRESHOLD = 0.90  # 90% threshold

# Transaction types that the ETL ingests as orders
ORDER_TRANSACTION_TYPES = [1, 6, 7, 8]  # Sale, Auth, Capture, COD


def get_crm_credentials(client_id):
    """Fetch VRIO CRM credentials from the database."""
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
            WHERE crm.id = 2 AND cl.id = %s;
        """
        cur.execute(sql, (client_id,))
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


def fetch_vrio_api(endpoint, params, offset, api_key):
    """Fetch data from VRIO API with offset-based pagination."""
    try:
        url = f"{VRIO_API_URL}{endpoint}"
        api_params = {
            "offset": offset,
            "limit": 200,
            **params,
        }
        headers = {
            "accept": "application/json",
            "hostname": "api.vrio.app",
            "X-Api-Key": api_key,
        }

        response = requests.get(url, params=api_params, headers=headers, timeout=60)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"VRIO API request failed with status code: {response.status_code}")
            return None

    except requests.RequestException as e:
        print(f"Error calling VRIO API: {e}")
        return None


def is_test_order(transaction):
    """
    Check if a transaction is a test order.
    A transaction is considered test if:
    - Any line item has is_test = true
    - Customer email contains 'test'
    """
    # Check line items for is_test flag
    line_items = transaction.get("line_items", [])
    for item in line_items:
        if item.get("is_test") in (True, "true", "True", 1, "1"):
            return True

    # Check customer email for 'test'
    customer = transaction.get("customer", {})
    email = customer.get("email", "") or ""
    if "test" in email.lower():
        return True

    return False


def get_crm_order_count(crm, date_str):
    """
    Fetch the count of orders from VRIO CRM API for a specific date.
    Fetches transaction types: Sale(1), Auth(6), Capture(7), COD(8).
    Excludes test orders.
    Returns count of non-test line items (matching how ETL stores one row per line item).
    """
    all_non_test_count = 0
    total_count = 0

    for txn_type in ORDER_TRANSACTION_TYPES:
        offset = 0

        while True:
            json_data = fetch_vrio_api(
                "/transactions/",
                {
                    "date_complete_from": date_str,
                    "date_complete_to": date_str,
                    "transaction_type_id": txn_type,
                    "transaction_status": 2,
                    "with": "customer,line_items",
                },
                offset,
                crm.CRM_API_KEY,
            )

            if not json_data or json_data.get("result") == "ERROR":
                if offset == 0 and txn_type == 1:
                    return None
                break

            transactions = json_data.get("transactions", [])
            api_total = json_data.get("total", 0)

            for txn in transactions:
                line_items = txn.get("line_items", [])
                line_item_count = max(len(line_items), 1)
                total_count += line_item_count

                if not is_test_order(txn):
                    all_non_test_count += line_item_count

            if offset + 200 >= api_total:
                break

            offset += 200

    print(f"CRM Total (all): {total_count}, Non-test: {all_non_test_count}")
    return all_non_test_count


def get_db_order_count(client_id, date_str):
    """
    Fetch the count of non-test orders from the database for a specific date.
    Uses data.orders_{client_id} table with end_date = '9999-12-31' filter.
    Excludes test orders (is_test_actual = 'Yes').
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
              AND is_test_actual != 'Yes'
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
    Verify the data by comparing VRIO CRM order count with database order count.
    Returns verification result with details.
    """
    # Use yesterday's date if no date provided
    if target_date is None:
        target_date = datetime.now() - timedelta(days=1)

    date_str = target_date.strftime("%m/%d/%Y")

    print(f"\n{'='*60}")
    print(f"Data Verification for {crm.CLIENT_NAME} (Client ID: {crm.CLIENT_ID})")
    print(f"CRM: VRIO")
    print(f"Date: {date_str}")
    print(f"{'='*60}")

    # Get CRM order count (excluding test orders)
    crm_count = get_crm_order_count(crm, date_str)
    if crm_count is None:
        print("FAILED: Could not fetch CRM order count")
        return {
            "status": "ERROR",
            "client_id": crm.CLIENT_ID,
            "client_name": crm.CLIENT_NAME,
            "date": date_str,
            "error": "Failed to fetch CRM order count",
        }

    # Get database order count (excluding test orders)
    db_count = get_db_order_count(crm.CLIENT_ID, date_str)
    if db_count is None:
        print("FAILED: Could not fetch database order count")
        return {
            "status": "ERROR",
            "client_id": crm.CLIENT_ID,
            "client_name": crm.CLIENT_NAME,
            "date": date_str,
            "error": "Failed to fetch database order count",
        }

    # Calculate match percentage
    if crm_count > 0:
        match_percentage = (db_count / crm_count) * 100
    else:
        match_percentage = 100.0 if db_count == 0 else 0.0

    # Determine verification status
    is_verified = match_percentage >= (VERIFICATION_THRESHOLD * 100)
    status = "PASS" if is_verified else "FAIL"

    # Print results
    print(f"\nCRM Order Count (Non-test):  {crm_count}")
    print(f"DB Order Count (Non-test):   {db_count}")
    print(f"Match Percentage:            {match_percentage:.2f}%")
    print(f"Threshold:                   {VERIFICATION_THRESHOLD * 100}%")
    print(f"\nVerification Status:         {status}")
    print(f"{'='*60}\n")

    # Update data_verified status in clients_pipeline_status table
    update_data_verified_status(crm.CLIENT_ID, is_verified)

    return {
        "status": status,
        "client_id": crm.CLIENT_ID,
        "client_name": crm.CLIENT_NAME,
        "date": date_str,
        "crm_count": crm_count,
        "db_count": db_count,
        "match_percentage": match_percentage,
        "threshold": VERIFICATION_THRESHOLD * 100,
        "is_verified": is_verified,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Verify order data between VRIO CRM and database."
    )
    parser.add_argument(
        "--client_id", type=str, required=True, help="Client ID for verification"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Date to verify in MM/DD/YYYY format (default: yesterday)",
    )
    args = parser.parse_args()

    # Parse custom date if provided
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%m/%d/%Y")
        except ValueError:
            print(f"Invalid date format: {args.date}. Please use MM/DD/YYYY format.")
            return

    # Get CRM credentials for VRIO
    crm_list = get_crm_credentials(args.client_id)

    if not crm_list:
        print(f"No VRIO CRM credentials found for client_id: {args.client_id}")
        return

    # Run verification for each CRM
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

    # Exit with error code if any verification failed
    if failed > 0 or errors > 0:
        exit(1)


if __name__ == "__main__":
    main()
