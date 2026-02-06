"""
Client Revenue Report Script
Generates a CSV/Excel report with revenue data for all clients.
Includes last payment date, next payment date, and last 30 days revenue.
"""

import configparser
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import stripe
from pyairtable import Api

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Configuration
config = configparser.ConfigParser()
config.read("config.ini")

PSG_USER = config.get("database", "PSG_USER")
PSG_PASSWORD = config.get("database", "PSG_PASSWORD")
PSG_HOST = config.get("database", "PSG_HOST")
PSG_PORT = int(config.get("database", "PSG_PORT"))
PSG_DATABASE = config.get("database", "PSG_DATABASE")

STRIPE_API_KEY = config.get("stripe", "STRIPE_API_KEY")
stripe.api_key = STRIPE_API_KEY

AIRTABLE_API_KEY = config.get("airtable", "AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = config.get("airtable", "AIRTABLE_BASE_ID")
AIRTABLE_TABLE_ID = config.get("airtable", "AIRTABLE_TABLE_ID")
AIRTABLE_VIEW_ID = config.get("airtable", "AIRTABLE_VIEW_ID")


def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(
        user=PSG_USER,
        password=PSG_PASSWORD,
        host=PSG_HOST,
        port=PSG_PORT,
        database=PSG_DATABASE,
    )


def get_client_name(conn, client_id):
    """Fetch client name from database."""
    try:
        cur = conn.cursor()
        sql = """
            SELECT name
            FROM beast_insights_v2.clients
            WHERE id = %s
        """
        cur.execute(sql, (client_id,))
        result = cur.fetchone()
        cur.close()
        return result[0] if result else None
    except Exception:
        conn.rollback()
        return None


def get_stripe_client_data(conn):
    """Fetch Stripe client data for active, non-deleted clients."""
    try:
        cur = conn.cursor()
        sql = """
            SELECT s.client_id, s.stripe_customer_id, s.subscription_id
            FROM beast_insights_v2.stripe_client_data s
            JOIN beast_insights_v2.clients c ON s.client_id = c.id
            WHERE c.is_active = True
              AND c.is_deleted = False
            ORDER BY s.client_id
        """
        cur.execute(sql)
        results = cur.fetchall()
        cur.close()
        return results
    except Exception:
        conn.rollback()
        return []


def get_subscription_url(subscription_id):
    """Generate Stripe dashboard URL for subscription."""
    return f"https://dashboard.stripe.com/subscriptions/{subscription_id}"


def get_price_tier(revenue):
    """Calculate price tier based on last 30 days revenue."""
    if revenue <= 10000:
        return 99
    elif revenue <= 50000:
        return 499
    elif revenue <= 500000:
        return 1299
    elif revenue <= 3000000:
        return 2499
    else:  # $3M to $8M+
        return 3999


def get_stripe_payment_dates(subscription_id):
    """
    Fetch last payment date and next payment date from Stripe API.
    Returns (last_payment_date, next_payment_date) as formatted strings.
    """
    if not subscription_id:
        return None, None

    try:
        subscription = stripe.Subscription.retrieve(subscription_id)

        # Next payment date from current_period_end (in subscription items)
        next_payment_date = None
        items = subscription.get("items")
        if items and "data" in items and len(items["data"]) > 0:
            period_end = items["data"][0].get("current_period_end")
            if period_end:
                next_payment_date = datetime.fromtimestamp(period_end).strftime("%Y-%m-%d")

        # Last payment date from latest_invoice
        last_payment_date = None
        latest_invoice_id = subscription.get("latest_invoice")
        if latest_invoice_id:
            invoice = stripe.Invoice.retrieve(latest_invoice_id)
            status_transitions = invoice.get("status_transitions")
            if status_transitions:
                paid_at = status_transitions.get("paid_at")
                if paid_at:
                    last_payment_date = datetime.fromtimestamp(paid_at).strftime("%Y-%m-%d")

        return last_payment_date, next_payment_date
    except Exception as e:
        logger.error(f"Error fetching Stripe data for {subscription_id}: {e}")
        return None, None


def get_revenue(conn, client_id, start_date):
    """
    Fetch revenue based on client ID range.
    - 10005-10063: Uses reporting.order_summary_{client_id} table
    - 20001-20030: Uses order_details table with filters
    """
    try:
        cur = conn.cursor()

        if 10005 <= client_id <= 10063:
            # Query for clients 10005-10063
            table_name = f"reporting.order_summary_{client_id}"
            sql = f"""
                SELECT COALESCE(SUM(revenue), 0) as revenue
                FROM {table_name}
                WHERE date >= %s
            """
            cur.execute(sql, (start_date,))
        elif 20001 <= client_id <= 20030:
            # Query for clients 20001-20030
            sql = """
                SELECT COALESCE(SUM(order_total), 0) as revenue
                FROM order_details
                WHERE client_id = %s
                  AND is_test = 'No'
                  AND order_status = 'Approved'
                  AND bill_email NOT ILIKE '%%test%%'
                  AND date_of_sale >= %s
            """
            cur.execute(sql, (str(client_id), start_date))
        else:
            cur.close()
            return None

        result = cur.fetchone()
        cur.close()

        return float(result[0]) if result else 0.0
    except Exception:
        conn.rollback()
        return None


def process_single_client(client_data):
    """Process a single client - fetch Stripe data and revenue. Used for parallel processing."""
    client_id, _, subscription_id = client_data

    # Get Stripe payment dates (API call)
    last_payment_date, next_payment_date = get_stripe_payment_dates(subscription_id)

    # Generate subscription URL
    subscription_url = get_subscription_url(subscription_id)

    return {
        "client_id": client_id,
        "subscription_id": subscription_id,
        "subscription_url": subscription_url,
        "last_payment_date": last_payment_date,
        "next_payment_date": next_payment_date,
    }


def update_airtable_revenue(report_data):
    """Update Airtable records with last 30 days revenue and price tier."""
    api = Api(AIRTABLE_API_KEY)
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID)

    # Get all records from the specific view
    records = table.all(view=AIRTABLE_VIEW_ID)

    # Create a mapping of client_id to record_id
    client_to_record = {}
    for record in records:
        client_id = record["fields"].get("Client ID")
        if client_id:
            client_to_record[int(client_id)] = record["id"]

    # Update records with revenue and price tier data
    updated_count = 0
    for data in report_data:
        client_id = data["client_id"]
        revenue = data["last_30_days_revenue"]
        price_tier = get_price_tier(revenue)

        if client_id in client_to_record:
            record_id = client_to_record[client_id]
            try:
                table.update(record_id, {
                    "Revenue (last 30 days)": revenue,
                    "Price Tier": price_tier
                })
                logger.info(f"Updated client {client_id}: ${revenue:,.2f} -> Tier ${price_tier}")
                updated_count += 1
            except Exception as e:
                logger.error(f"Error updating client {client_id}: {e}")
        else:
            logger.warning(f"Client {client_id} not found in Airtable view")

    return updated_count


def generate_revenue_report(max_workers=10):
    """Generate revenue report for all clients with Stripe subscription data.

    Args:
        max_workers: Number of parallel threads for Stripe API calls (default: 10)
    """

    # Calculate date ranges
    today = datetime.now().date()
    last_30_days_start = today - timedelta(days=30)

    logger.info("=" * 70)
    logger.info("Client Revenue Report with Stripe Payment Data")
    logger.info("=" * 70)
    logger.info(f"Report Date: {today}")
    logger.info(f"Last 30 Days: {last_30_days_start} to {today}")
    logger.info(f"Parallel workers: {max_workers}")
    logger.info("=" * 70)

    # Connect to database
    logger.info("Connecting to database...")
    conn = get_db_connection()
    logger.info("Database connection established")

    # Get all Stripe client data
    logger.info("Fetching Stripe client data from database...")
    stripe_clients = get_stripe_client_data(conn)

    if not stripe_clients:
        logger.warning("No Stripe client data found.")
        conn.close()
        return None

    logger.info(f"Found {len(stripe_clients)} clients with Stripe subscriptions")
    logger.info("Fetching Stripe payment data in parallel...")

    # Fetch Stripe data in parallel
    stripe_results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_client = {
            executor.submit(process_single_client, client_data): client_data[0]
            for client_data in stripe_clients
        }

        for future in as_completed(future_to_client):
            client_id = future_to_client[future]
            try:
                result = future.result()
                stripe_results[client_id] = result
                logger.debug(f"Client {client_id}: Stripe data fetched")
            except Exception as e:
                logger.error(f"Client {client_id}: Error - {e}")

    logger.info(f"Stripe data fetched for {len(stripe_results)} clients")
    logger.info("Fetching revenue data from database...")

    # Now fetch database data (client names and revenue) sequentially
    report_data = []

    for client_id, _, _ in stripe_clients:
        if client_id not in stripe_results:
            continue

        client_name = get_client_name(conn, client_id)

        if not client_name:
            logger.warning(f"Client {client_id}: Name not found, skipping")
            continue

        # Get revenue for last 30 days
        revenue_30_days = get_revenue(conn, client_id, last_30_days_start)

        stripe_data = stripe_results[client_id]

        report_data.append({
            "client_id": client_id,
            "client_name": client_name,
            "subscription_url": stripe_data["subscription_url"],
            "last_payment_date": stripe_data["last_payment_date"] or "N/A",
            "next_payment_date": stripe_data["next_payment_date"] or "N/A",
            "last_30_days_revenue": revenue_30_days or 0.0,
        })

        logger.info(f"Client {client_id} ({client_name}): ${revenue_30_days or 0:,.2f}")

    conn.close()

    # Create DataFrame and export
    if report_data:
        df = pd.DataFrame(report_data)

        # Format column names
        df.columns = [
            "Client ID",
            "Client Name",
            "Subscription URL",
            "Last Payment Date",
            "Next Payment Date",
            f"Last 30 Days Revenue ({last_30_days_start} to {today})"
        ]

        # Static filename (overwrites previous report)
        filename = "client_revenue_report.xlsx"

        # Try Excel first, fall back to CSV
        logger.info("Exporting report to file...")
        try:
            df.to_excel(filename, index=False, sheet_name="Revenue Report", engine="openpyxl")
        except ImportError:
            filename = "client_revenue_report.csv"
            df.to_csv(filename, index=False)

        logger.info("=" * 70)
        logger.info(f"Report generated: {filename}")
        logger.info(f"Total clients processed: {len(report_data)}")
        logger.info("=" * 70)

        # Log summary
        total_30_days = sum(r["last_30_days_revenue"] for r in report_data)
        logger.info(f"Total Revenue (Last 30 Days): ${total_30_days:,.2f}")

        # Update Airtable
        logger.info("=" * 70)
        logger.info("Updating Airtable...")
        logger.info("=" * 70)
        updated_count = update_airtable_revenue(report_data)
        logger.info("=" * 70)
        logger.info(f"Airtable updated: {updated_count} records")
        logger.info("=" * 70)

        return filename
    else:
        logger.warning("No data found for any clients.")
        return None


def main():
    generate_revenue_report()


if __name__ == "__main__":
    main()
