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
import requests
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

TELEGRAM_BOT_TOKEN = config.get("telegram", "BOT_TOKEN")
TELEGRAM_CHAT_ID = config.get("telegram", "CHAT_ID")

CLICKHOUSE_HOST = config.get("clickhouse", "CLICKHOUSE_HOST")
CLICKHOUSE_USER = config.get("clickhouse", "CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = config.get("clickhouse", "CLICKHOUSE_PASSWORD")


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


# Revenue -> tier bands: first band whose upper bound (inclusive) is >= revenue
# wins; None is the unbounded top band. Ordered ascending.
DEFAULT_TIER_BANDS = [
    (10000, 99),
    (50000, 499),
    (500000, 1299),
    (3000000, 2499),
    (None, 3999),  # $3M to $8M+
]

# Per-client overrides of the tier bands (keyed by client_id).
CLIENT_TIER_BANDS = {
    # Sabre Brands (10039): the $1299 -> $2499 boundary is $1M instead of $500k.
    10039: [
        (10000, 99),
        (50000, 499),
        (1000000, 1299),
        (3000000, 2499),
        (None, 3999),
    ],
}


def get_price_tier(revenue, client_id=None):
    """Calculate price tier based on last 30 days revenue.

    Uses the client's custom tier bands when one is configured in
    CLIENT_TIER_BANDS, otherwise the default bands.
    """
    bands = CLIENT_TIER_BANDS.get(client_id, DEFAULT_TIER_BANDS)
    for upper, tier in bands:
        if upper is None or revenue <= upper:
            return tier
    return bands[-1][1]


# Recurring monthly Stripe Price IDs for each revenue tier (Beast Advanced product).
TIER_PRICE_IDS = {
    99: "price_1SnIbYHbglIIuFdher5A7XC7",    # Monthly Volume upto $10k
    499: "price_1SfuxbHbglIIuFdhOKKCsX8I",   # Monthly Volume upto $50k
    1299: "price_1RgTRwHbglIIuFdhZtJp5Y6J",  # Monthly Volume Up to $500k
    2499: "price_1SbDpFHbglIIuFdhlx9dSrpY",  # Monthly Volume upto $3M
    3999: "price_1SbDpFHbglIIuFdhQCHpEEza",  # Monthly Volume upto $8M
}


def get_stripe_subscription_info(subscription_id):
    """
    Fetch payment dates and current pricing for a subscription from Stripe.
    Returns a dict with last/next payment dates, the current price (dollars),
    the subscription item id, and the subscription status.
    """
    info = {
        "last_payment_date": None,
        "next_payment_date": None,
        "current_price": None,
        "price_id": None,
        "subscription_item_id": None,
        "status": None,
    }
    if not subscription_id:
        return info

    try:
        subscription = stripe.Subscription.retrieve(subscription_id)
        info["status"] = subscription.get("status")

        # Pricing + next payment date live on the subscription item
        items = subscription.get("items")
        if items and "data" in items and len(items["data"]) > 0:
            item = items["data"][0]
            info["subscription_item_id"] = item.get("id")

            period_end = item.get("current_period_end")
            if period_end:
                info["next_payment_date"] = datetime.fromtimestamp(period_end).strftime("%Y-%m-%d")

            price = item.get("price") or {}
            info["price_id"] = price.get("id")
            amount = price.get("unit_amount")
            if amount is None:
                amount = (item.get("plan") or {}).get("amount")
            if amount is not None:
                info["current_price"] = amount / 100.0

        # Last payment date from latest_invoice
        latest_invoice_id = subscription.get("latest_invoice")
        if latest_invoice_id:
            invoice = stripe.Invoice.retrieve(latest_invoice_id)
            status_transitions = invoice.get("status_transitions")
            if status_transitions:
                paid_at = status_transitions.get("paid_at")
                if paid_at:
                    info["last_payment_date"] = datetime.fromtimestamp(paid_at).strftime("%Y-%m-%d")

        return info
    except Exception as e:
        logger.error(f"Error fetching Stripe data for {subscription_id}: {e}")
        return info


def update_stripe_subscription_tier(subscription_id, item_id, new_price_id):
    """
    Switch a subscription's item to new_price_id, effective at the next renewal
    (proration_behavior='none' means no mid-cycle charge/credit).
    """
    stripe.Subscription.modify(
        subscription_id,
        items=[{"id": item_id, "price": new_price_id}],
        proration_behavior="none",
    )


def clickhouse_query(sql):
    """Run a query against the ClickHouse HTTP interface and return the raw response text."""
    headers = {
        "X-ClickHouse-User": CLICKHOUSE_USER,
        "X-ClickHouse-Key": CLICKHOUSE_PASSWORD,
    }
    resp = requests.post(CLICKHOUSE_HOST, data=sql.encode(), headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.text


def get_revenue(client_id, start_date, end_date):
    """
    Fetch revenue for a client from ClickHouse reporting.orders_enriched_{client_id}.

    Sums order_total over approved sale rows in [start_date, end_date], matching
    the canonical app revenue definition: the enriched table fans out rows per
    order (alert / chargeback / tc40 / refund events), so we keep only
    row_kind IN ('order', 'alert_orphan', 'alert_event'), drop refund-alert order
    rows, and total order_total where kind = 'sale' AND is_approved. Filtered on
    the `date` column.
    """
    try:
        sql = f"""
            SELECT COALESCE(SUM(CASE WHEN kind = 'sale' AND is_approved THEN order_total END), 0) AS revenue
            FROM reporting.orders_enriched_{client_id}
            WHERE row_kind IN ('order', 'alert_orphan', 'alert_event')
              AND NOT (row_kind = 'order' AND kind = 'refund' AND refund_type = 'Refund Alert')
              AND date >= toDate('{start_date}')
              AND date <= toDate('{end_date}')
            FORMAT TabSeparated
        """
        text = clickhouse_query(sql).strip()
        return float(text) if text else 0.0
    except Exception as e:
        logger.error(f"Error fetching revenue for {client_id}: {e}")
        return None


def process_single_client(client_data):
    """Process a single client - fetch Stripe data and revenue. Used for parallel processing."""
    client_id, _, subscription_id = client_data

    # Get Stripe payment dates + current pricing (API call)
    info = get_stripe_subscription_info(subscription_id)

    # Generate subscription URL
    subscription_url = get_subscription_url(subscription_id)

    return {
        "client_id": client_id,
        "subscription_id": subscription_id,
        "subscription_url": subscription_url,
        "last_payment_date": info["last_payment_date"],
        "next_payment_date": info["next_payment_date"],
        "current_price": info["current_price"],
        "price_id": info["price_id"],
        "subscription_item_id": info["subscription_item_id"],
        "status": info["status"],
    }


def fetch_airtable_records():
    """Fetch all records from Airtable view once for reuse."""
    api = Api(AIRTABLE_API_KEY)
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID)
    records = table.all(view=AIRTABLE_VIEW_ID)
    return table, records


def update_airtable_revenue(report_data, table, records):
    """Update Airtable records with last 30 days revenue and price tier using batch updates."""
    # Create a mapping of client_id to record_id
    client_to_record = {}
    for record in records:
        client_id = record["fields"].get("Client ID")
        if client_id:
            client_to_record[int(client_id)] = record["id"]

    # Build batch update list
    batch = []
    for data in report_data:
        client_id = data["client_id"]
        revenue = data["last_30_days_revenue"]
        price_tier = get_price_tier(revenue, client_id)

        if client_id in client_to_record:
            record_id = client_to_record[client_id]
            last_payment = data["last_payment_date"]
            fields_to_update = {
                "Revenue (last 30 days)": revenue,
                "Price Tier": price_tier,
            }
            if last_payment != "N/A":
                fields_to_update["Last Payment Date"] = last_payment
            batch.append({"id": record_id, "fields": fields_to_update})
            logger.info(f"Queued client {client_id}: ${revenue:,.2f} -> Tier ${price_tier}, Last Payment: {last_payment}")
        else:
            logger.warning(f"Client {client_id} not found in Airtable view")

    # Batch update (pyairtable handles chunking into groups of 10 internally)
    updated_count = 0
    if batch:
        try:
            table.batch_update(batch)
            updated_count = len(batch)
            logger.info(f"Batch updated {updated_count} records")
        except Exception as e:
            logger.error(f"Error in batch update: {e}")

    return updated_count


def send_telegram_message(message):
    """Send a message via Telegram bot."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        logger.info("Telegram notification sent successfully")
    except Exception as e:
        logger.error(f"Error sending Telegram notification: {e}")


def check_upcoming_payments_and_notify(report_data, records):
    """
    Check for clients with payments due in 3 days that have
    'New Pricing Model' checked in Airtable, and send Telegram notifications.
    """

    # Build set of client IDs with "New Pricing model" checked and their current price tier
    new_pricing_clients = {}
    for record in records:
        client_id = record["fields"].get("Client ID")
        new_pricing = record["fields"].get("New Pricing model", False)
        if client_id and new_pricing:
            current_tier = record["fields"].get("Price Tier")
            new_pricing_clients[int(client_id)] = current_tier

    # Find clients with payment due in 3 days
    target_date = (datetime.now().date() + timedelta(days=3)).strftime("%Y-%m-%d")
    clients_due = []

    for data in report_data:
        client_id = data["client_id"]
        next_payment = data["next_payment_date"]

        if (
            next_payment != "N/A"
            and next_payment == target_date
            and client_id in new_pricing_clients
        ):
            clients_due.append(data)

    if not clients_due:
        logger.info("No clients with upcoming payments in 3 days under New Pricing Model")
        return

    # Build and send Telegram message
    message = "<b>⚠️ Payment Due in 3 Days - New Pricing Model Clients</b>\n\n"
    for client in clients_due:
        current_tier = new_pricing_clients.get(client["client_id"])
        updated_tier = get_price_tier(client["last_30_days_revenue"], client["client_id"])
        message += (
            f"<b>{client['client_name']}</b> (ID: {client['client_id']})\n"
            f"  Next Payment: {client['next_payment_date']}\n"
            f"  Last 30 Days Revenue: ${client['last_30_days_revenue']:,.2f}\n"
            f"  Current Price Tier: ${current_tier}\n"
            f"  Updated Price Tier: ${updated_tier}\n"
            f"  Subscription: {client['subscription_url']}\n\n"
        )

    send_telegram_message(message)
    logger.info(f"Notified about {len(clients_due)} client(s) with payments due in 3 days")


def notify_tier_changes(changes, dry_run):
    """Send a Telegram summary of the detected tier changes."""
    header = "🧪 [DRY RUN] " if dry_run else ""
    message = f"<b>{header}📊 Tier Changes - New Pricing Model</b>\n\n"
    for change in changes:
        direction = "🔺 Upgrade" if change["new_tier"] > change["current_tier"] else "🔻 Downgrade"
        if change["dry_run"]:
            stripe_status = "DRY RUN — not applied"
        elif change["applied"]:
            stripe_status = "✅ updated (effective next renewal)"
        else:
            stripe_status = f"⚠️ FAILED: {change['error']}"
        message += (
            f"<b>{change['client_name']}</b> (ID: {change['client_id']})\n"
            f"  {direction}: ${change['current_tier']} → ${change['new_tier']}\n"
            f"  Last 30 Days Revenue: ${change['revenue']:,.2f}\n"
            f"  Next Payment: {change['next_payment_date']}\n"
            f"  Stripe: {stripe_status}\n"
            f"  {change['subscription_url']}\n\n"
        )
    send_telegram_message(message)
    logger.info(f"Notified about {len(changes)} tier change(s)")


def process_tier_changes(report_data, records, dry_run=False, days_ahead=3):
    """
    For 'New Pricing model' clients whose next Stripe payment is due within the
    next `days_ahead` days, compare the tier computed from last-30-day revenue
    against the client's current Stripe price. When they differ, switch the
    Stripe subscription to the new tier (effective next renewal) and send a
    Telegram notification summarizing the changes.

    With dry_run=True, no Stripe changes are made — only detection + notification.
    """
    # Clients flagged for the new pricing model in Airtable
    npm_clients = set()
    for record in records:
        client_id = record["fields"].get("Client ID")
        if client_id is not None and record["fields"].get("New Pricing model", False):
            npm_clients.add(int(client_id))

    # Only act on clients whose next payment falls within the upcoming window
    today = datetime.now().date()
    window_end = today + timedelta(days=days_ahead)

    changes = []
    clients_in_window = 0
    for data in report_data:
        client_id = data["client_id"]
        if client_id not in npm_clients:
            continue

        # Restrict to clients billed within the next `days_ahead` days
        next_payment = data.get("next_payment_date")
        if not next_payment or next_payment == "N/A":
            continue
        try:
            next_payment_date = datetime.strptime(next_payment, "%Y-%m-%d").date()
        except ValueError:
            logger.warning(f"Client {client_id}: unparseable next payment date '{next_payment}', skipping")
            continue
        if not (today < next_payment_date <= window_end):
            continue

        clients_in_window += 1

        revenue = data["last_30_days_revenue"]
        new_tier = get_price_tier(revenue, client_id)

        current_price = data.get("current_price")
        item_id = data.get("subscription_item_id")
        subscription_id = data.get("subscription_id")

        if current_price is None or not item_id:
            logger.warning(f"Client {client_id}: missing Stripe pricing info, skipping tier check")
            continue

        current_tier = int(round(current_price))
        if current_tier == new_tier:
            continue

        new_price_id = TIER_PRICE_IDS.get(new_tier)
        if not new_price_id:
            logger.error(f"Client {client_id}: no Stripe price configured for tier ${new_tier}")
            continue

        applied = False
        error = None
        if dry_run:
            logger.info(
                f"[DRY RUN] Client {client_id}: tier ${current_tier} -> ${new_tier} "
                f"(would update Stripe, effective next renewal)"
            )
        else:
            try:
                update_stripe_subscription_tier(subscription_id, item_id, new_price_id)
                applied = True
                logger.info(
                    f"Client {client_id}: Stripe tier updated ${current_tier} -> ${new_tier} "
                    f"(effective next renewal)"
                )
            except Exception as e:
                error = str(e)
                logger.error(f"Client {client_id}: failed to update Stripe tier: {e}")

        changes.append({
            "client_id": client_id,
            "client_name": data["client_name"],
            "revenue": revenue,
            "current_tier": current_tier,
            "new_tier": new_tier,
            "next_payment_date": next_payment,
            "subscription_url": data["subscription_url"],
            "applied": applied,
            "dry_run": dry_run,
            "error": error,
        })

    if changes:
        notify_tier_changes(changes, dry_run)
    elif clients_in_window == 0:
        logger.info(f"No New Pricing model clients with payments due in the next {days_ahead} days")
        header = "🧪 [DRY RUN] " if dry_run else ""
        send_telegram_message(
            f"<b>{header}📊 Tier Changes - New Pricing Model</b>\n\n"
            f"No clients with payments due in the next {days_ahead} days."
        )
    else:
        logger.info(
            f"{clients_in_window} New Pricing model client(s) due in the next {days_ahead} days, "
            f"no tier changes needed"
        )
        header = "🧪 [DRY RUN] " if dry_run else ""
        send_telegram_message(
            f"<b>{header}📊 Tier Changes - New Pricing Model</b>\n\n"
            f"{clients_in_window} client(s) with payments due in the next {days_ahead} days — "
            f"all already on the correct tier, no changes needed."
        )

    return changes


def generate_revenue_report(max_workers=10, dry_run=False):
    """Generate revenue report for all clients with Stripe subscription data.

    Args:
        max_workers: Number of parallel threads for Stripe API calls (default: 10)
        dry_run: When True, detect tier changes and notify on Telegram but do NOT
                 modify any Stripe subscriptions (default: False).
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
        revenue_30_days = get_revenue(client_id, last_30_days_start, today)

        stripe_data = stripe_results[client_id]

        report_data.append({
            "client_id": client_id,
            "client_name": client_name,
            "subscription_id": stripe_data["subscription_id"],
            "subscription_url": stripe_data["subscription_url"],
            "last_payment_date": stripe_data["last_payment_date"] or "N/A",
            "next_payment_date": stripe_data["next_payment_date"] or "N/A",
            "last_30_days_revenue": revenue_30_days or 0.0,
            "current_price": stripe_data["current_price"],
            "subscription_item_id": stripe_data["subscription_item_id"],
        })

        logger.info(f"Client {client_id} ({client_name}): ${revenue_30_days or 0:,.2f}")

    conn.close()

    # Create DataFrame and export
    if report_data:
        # Project only the columns shown in the report (report_data carries extra
        # internal fields used for tier processing that we don't export)
        df = pd.DataFrame(report_data)[[
            "client_id",
            "client_name",
            "subscription_url",
            "last_payment_date",
            "next_payment_date",
            "last_30_days_revenue",
        ]]

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

        # Fetch Airtable records once
        logger.info("=" * 70)
        logger.info("Fetching Airtable records...")
        table, airtable_records = fetch_airtable_records()
        logger.info(f"Fetched {len(airtable_records)} records from Airtable")

        # Update Airtable
        logger.info("Updating Airtable...")
        logger.info("=" * 70)
        updated_count = update_airtable_revenue(report_data, table, airtable_records)
        logger.info("=" * 70)
        logger.info(f"Airtable updated: {updated_count} records")
        logger.info("=" * 70)

        # Check for upcoming payments and send Telegram notifications
        logger.info("Checking for upcoming payments (due in 3 days)...")
        check_upcoming_payments_and_notify(report_data, airtable_records)

        # Detect tier changes for New Pricing model clients, update Stripe and notify
        logger.info("=" * 70)
        logger.info(f"Processing tier changes (dry_run={dry_run})...")
        process_tier_changes(report_data, airtable_records, dry_run=dry_run)
        logger.info("=" * 70)

        return filename
    else:
        logger.warning("No data found for any clients.")
        return None


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate client revenue report and sync tiers.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect tier changes and notify on Telegram but do NOT modify Stripe subscriptions.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Number of parallel threads for Stripe API calls (default: 10).",
    )
    args = parser.parse_args()

    generate_revenue_report(max_workers=args.max_workers, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
