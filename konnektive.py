"""
Data Verification Script - Konnektive CRM
Compares the number of orders from Konnektive CRM API with the database for yesterday.
If the database count is at least 90% of the CRM count, verification passes.
"""

import argparse
import configparser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import psycopg2
import pytz
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

# Mapping of timezone abbreviations to pytz-compatible Olson timezone names
TIMEZONE_MAPPING = {
    "ICT": "Asia/Bangkok",      # Indochina Time (UTC+7)
    "EST": "America/New_York",  # Eastern Standard Time
    "PST": "America/Los_Angeles",  # Pacific Standard Time
    "CST": "America/Chicago",   # Central Standard Time
    "MST": "America/Denver",    # Mountain Standard Time
    "GMT": "Europe/London",     # Greenwich Mean Time
    "UTC": "UTC",
}


def get_konnektive_api_url(client_id):
    """Get Konnektive API URL based on client_id."""
    if str(client_id) in ["10057", "10058", "10062"]:
        return "https://api.checkoutchamp.com"
    else:
        return config.get("production", "KONNEKTIVE_API_URL")


def get_client_timezone(client_id):
    """Fetch the timezone for a client from clients_pipeline_status table."""
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
            SELECT timezone
            FROM beast_insights_v2.clients_pipeline_status
            WHERE client_id = %s
        """
        cur.execute(sql, (client_id,))
        result = cur.fetchone()

        if result and result[0]:
            return result[0]
        else:
            print(f"No timezone found for client_id {client_id}, using UTC")
            return "UTC"

    except Exception as e:
        print(f"Error fetching client timezone: {e}")
        return "UTC"
    finally:
        cur.close()
        conn.close()


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


def fetch_api_data(api_endpoint, params, crm, page=1):
    """Fetch JSON data from Konnektive API."""
    try:
        api_url = get_konnektive_api_url(crm.CLIENT_ID)
        url = f"{api_url}{api_endpoint}"
        api_params = {
            "page": page,
            "resultsPerPage": 200,
            "loginId": crm.CRM_USERNAME,
            "password": crm.CRM_PASSWORD,
            **params,
        }
        response = requests.post(url, params=api_params, timeout=60)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch JSON data from {api_endpoint}. Status: {response.status_code}")
            return None
    except Exception as e:
        print(f"API error: {e}")
        return None


def get_crm_order_count(crm, target_date, client_tz):
    """
    Fetch the count of orders from Konnektive CRM API for a specific date.
    Uses pagination to fetch all orders. Fetches both SALE and AUTHORIZE transactions.
    Converts client timezone to EST for API call.
    """
    all_orders = []

    # EST timezone for API
    est_tz = pytz.timezone("America/New_York")

    # Create start and end of day in client's timezone
    start_of_day_client = client_tz.localize(
        datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0)
    )
    end_of_day_client = client_tz.localize(
        datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59)
    )

    # Convert to EST
    start_in_est = start_of_day_client.astimezone(est_tz)
    end_in_est = end_of_day_client.astimezone(est_tz)

    # Extract date and time strings for API
    start_date_str = start_in_est.strftime("%m/%d/%Y")
    start_time_str = start_in_est.strftime("%H:%M:%S")
    end_date_str = end_in_est.strftime("%m/%d/%Y")
    end_time_str = end_in_est.strftime("%H:%M:%S")

    print(f"API Query: {start_date_str} {start_time_str} to {end_date_str} {end_time_str} (EST)")

    # Fetch both SALE and AUTHORIZE transaction types
    for txn_type in ["SALE", "AUTHORIZE"]:
        page = 1
        while True:
            json_data = fetch_api_data(
                "/transactions/query/",
                {
                    "startDate": start_date_str,
                    "endDate": end_date_str,
                    "startTime": start_time_str,
                    "endTime": end_time_str,
                    "txnType": txn_type,
                },
                crm,
                page,
            )

            if not json_data or json_data.get("result") == "ERROR":
                if page == 1 and txn_type == "SALE":
                    # First page of SALE failed, return None to indicate error
                    return None, []
                break

            if "message" in json_data and "data" in json_data["message"]:
                data = json_data["message"]["data"]
                all_orders.extend(data)

            total_results = json_data.get("message", {}).get("totalResults", 0)

            if page * 200 >= total_results:
                break

            page += 1

    return len(all_orders), all_orders


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

        # Count orders from data.orders_{client_id} table
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
    Sets to True if match > 90%, else False.
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
    Verify the data by comparing CRM order count with database order count.
    Returns verification result with details.
    """
    # Get client's timezone
    client_tz_str = get_client_timezone(crm.CLIENT_ID)
    # Map abbreviation to Olson timezone name if needed
    tz_name = TIMEZONE_MAPPING.get(client_tz_str, client_tz_str)
    try:
        client_tz = pytz.timezone(tz_name)
    except Exception:
        print(f"Invalid timezone '{client_tz_str}' (mapped to '{tz_name}'), using UTC")
        client_tz = pytz.UTC

    # Use yesterday's date in client's timezone if no date provided
    if target_date is None:
        now_in_client_tz = datetime.now(client_tz)
        target_date = now_in_client_tz - timedelta(days=1)

    date_str = target_date.strftime("%m/%d/%Y")

    print(f"\n{'='*60}")
    print(f"Data Verification for {crm.CLIENT_NAME} (Client ID: {crm.CLIENT_ID})")
    print(f"CRM: Konnektive")
    print(f"Date: {date_str} (Timezone: {client_tz_str} -> {tz_name})")
    print(f"{'='*60}")

    # Get CRM order count
    crm_count, _ = get_crm_order_count(crm, target_date, client_tz)
    if crm_count is None:
        print("FAILED: Could not fetch CRM order count")
        return {
            "status": "ERROR",
            "client_id": crm.CLIENT_ID,
            "client_name": crm.CLIENT_NAME,
            "date": date_str,
            "error": "Failed to fetch CRM order count",
        }

    # Get database order count
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
    print(f"\nCRM Order Count (Total):     {crm_count}")
    print(f"DB Order Count (Active):     {db_count}")
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
        description="Verify order data between Konnektive CRM and database."
    )
    parser.add_argument(
        "--client_id", type=str, help="Client ID for verification (default: all active Konnektive clients)"
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
        dates_to_verify.append(None)  # Will use yesterday in client's timezone

    # Get CRM credentials for Konnektive
    crm_list = get_crm_credentials("Konnektive", args.client_id)

    if not crm_list:
        print(f"No Konnektive CRM credentials found" + (f" for client_id: {args.client_id}" if args.client_id else ""))
        return

    print(f"Verifying {len(crm_list)} client(s) for {len(dates_to_verify)} date(s)...\n")

    # Build all (crm, date) tasks
    tasks = [(crm, d) for d in dates_to_verify for crm in crm_list]

    results = []
    if len(tasks) > 1:
        max_workers = min(len(tasks), 5)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(verify_data, crm, d): (crm, d) for crm, d in tasks}
            for future in as_completed(futures):
                results.append(future.result())
        results.sort(key=lambda r: (str(r.get("client_id", "")), r.get("date", "")))
    else:
        for crm, d in tasks:
            results.append(verify_data(crm, d))

    # Summary
    print("\n" + "=" * 90)
    print("VERIFICATION SUMMARY")
    print("=" * 90)

    passed = sum(1 for r in results if r.get("status") == "PASS")
    failed = sum(1 for r in results if r.get("status") == "FAIL")
    errors = sum(1 for r in results if r.get("status") == "ERROR")

    print(f"Clients: {len(crm_list)} | Days: {len(dates_to_verify)} | Passed: {passed} | Failed: {failed} | Errors: {errors}")

    # Date-wise breakdown
    print(f"\n{'Client':<10} {'Name':<22} {'Date':<14} {'CRM':>8} {'DB':>8} {'Match%':>10} {'Status':<6}")
    print("-" * 90)
    for r in results:
        client_id = r.get("client_id", "-")
        client_name = str(r.get("client_name", "-"))[:20]
        date = r.get("date", "N/A")
        status = r.get("status", "N/A")
        crm_count = r.get("crm_count", "-")
        db_count = r.get("db_count", "-")
        match_pct = r.get("match_percentage")
        match_str = f"{match_pct:.1f}%" if match_pct is not None else "-"
        print(f"{str(client_id):<10} {client_name:<22} {date:<14} {str(crm_count):>8} {str(db_count):>8} {match_str:>10} {status:<6}")
    print("=" * 90)

    # Export to Excel
    excel_data = []
    for r in results:
        excel_data.append({
            "Client ID": r.get("client_id"),
            "Client Name": r.get("client_name"),
            "Date": r.get("date"),
            "CRM Count": r.get("crm_count"),
            "DB Count": r.get("db_count"),
            "Match %": r.get("match_percentage"),
            "Status": r.get("status"),
        })
    df = pd.DataFrame(excel_data)
    filename = f"konnektive_verification_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    df.to_excel(filename, index=False)
    print(f"\nExcel report saved: {filename}")

    if failed > 0 or errors > 0:
        exit(1)


if __name__ == "__main__":
    main()
