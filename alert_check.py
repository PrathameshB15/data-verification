"""
Alert Data Check Script
Checks if all active clients have alert data for yesterday in the order_alerts table.
Shows distinct source values per client.
"""

import argparse
import configparser
from datetime import datetime, timedelta

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

TELEGRAM_BOT_TOKEN = config.get("telegram", "BOT_TOKEN")
TELEGRAM_CHAT_ID = config.get("telegram", "CHAT_ID")


def get_db_connection():
    return psycopg2.connect(
        user=PSG_USER,
        password=PSG_PASSWORD,
        host=PSG_HOST,
        port=PSG_PORT,
        database=PSG_DATABASE,
    )


def get_active_clients(conn):
    """Fetch all active clients from the database."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name
        FROM beast_insights_v2.clients
        WHERE is_deleted = false AND is_active = true
        ORDER BY id
    """)
    clients = cur.fetchall()
    cur.close()
    return clients


def get_alert_data(conn, start_date, end_date):
    """Fetch alert counts and distinct sources per client per date for the date range."""
    cur = conn.cursor()
    cur.execute("""
        SELECT client_id,
               alert_date,
               COUNT(*) as alert_count,
               array_agg(DISTINCT source ORDER BY source) FILTER (WHERE source IS NOT NULL) as sources
        FROM public.order_alerts
        WHERE alert_date BETWEEN %s AND %s
        GROUP BY client_id, alert_date
        ORDER BY client_id, alert_date
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()

    # {client_id: {date: {count, sources}}}
    alert_map = {}
    for client_id, alert_date, count, sources in rows:
        if client_id not in alert_map:
            alert_map[client_id] = {}
        alert_map[client_id][alert_date] = {
            "count": count,
            "sources": sources or [],
        }
    return alert_map


def print_report(active_clients, alert_data, dates, label):
    """Print alert report for a given date range."""
    date_headers = [d.strftime("%m/%d") for d in dates]
    num_days = len(dates)
    start_date = dates[0]
    end_date = dates[-1]

    print(f"\n{'='*100}")
    print(f"Alert Data Check ({label}) - {start_date.strftime('%m/%d/%Y')} to {end_date.strftime('%m/%d/%Y')}")
    print(f"{'='*100}")
    print(f"Active clients: {len(active_clients)}")
    print(f"{'='*100}\n")

    # Build header
    header = f"{'Client ID':<12} {'Client Name':<25}"
    for dh in date_headers:
        header += f" {dh:<8}"
    header += f" {'Sources'}"
    print(header)
    print("-" * 100)

    clients_with_data = []
    clients_missing = []

    for client_id, client_name in active_clients:
        client_alerts = alert_data.get(client_id, {})
        has_any_data = any(d in client_alerts for d in dates)

        all_sources = set()
        row = f"{client_id:<12} {client_name:<25}"
        for d in dates:
            day_data = client_alerts.get(d)
            if day_data:
                row += f" {day_data['count']:<8}"
                all_sources.update(day_data["sources"])
            else:
                row += f" {'—':<8}"

        sources_str = ", ".join(sorted(all_sources)) if all_sources else "None" if has_any_data else "NO DATA"
        row += f" {sources_str}"
        print(row)

        if has_any_data:
            clients_with_data.append(client_id)
        else:
            clients_missing.append((client_id, client_name))

    # Summary
    print(f"\n{'='*100}")
    print(f"SUMMARY ({label})")
    print(f"{'='*100}")
    print(f"Clients with data:    {len(clients_with_data)}")
    print(f"Clients missing data (all {num_days} days): {len(clients_missing)}")

    if clients_missing:
        print(f"\nMissing clients:")
        for client_id, client_name in clients_missing:
            print(f"  - {client_id}: {client_name}")

    print(f"{'='*100}")

    return clients_with_data, clients_missing


def main():
    parser = argparse.ArgumentParser(
        description="Check if all active clients have alert data for the last 3 and 7 days."
    )
    parser.add_argument(
        "--date",
        type=str,
        help="End date in MM/DD/YYYY format (default: yesterday)",
    )
    args = parser.parse_args()

    today = datetime.now().date()
    if args.date:
        try:
            end_date = datetime.strptime(args.date, "%m/%d/%Y").date()
        except ValueError:
            print(f"Invalid date format: {args.date}. Please use MM/DD/YYYY format.")
            return
    else:
        end_date = today - timedelta(days=1)

    start_date_7d = end_date - timedelta(days=6)
    start_date_3d = end_date - timedelta(days=2)

    dates_3d = [start_date_3d + timedelta(days=i) for i in range(3)]
    dates_7d = [start_date_7d + timedelta(days=i) for i in range(7)]

    conn = get_db_connection()
    active_clients = get_active_clients(conn)
    alert_data = get_alert_data(conn, start_date_7d, end_date)
    conn.close()

    # Print both reports
    clients_with_3d, missing_3d = print_report(active_clients, alert_data, dates_3d, "Last 3 Days")
    clients_with_7d, missing_7d = print_report(active_clients, alert_data, dates_7d, "Last 7 Days")

    # Send Telegram summary
    total_alerts_3d = sum(
        day["count"]
        for cid, client_days in alert_data.items()
        for d, day in client_days.items()
        if d >= start_date_3d
    )
    total_alerts_7d = sum(
        day["count"]
        for client_days in alert_data.values()
        for day in client_days.values()
    )

    msg = f"<b>Alert Data Check</b>\n\n"

    msg += f"<b>Last 3 Days ({start_date_3d.strftime('%m/%d')} - {end_date.strftime('%m/%d')})</b>\n"
    msg += f"Clients with Data: {len(clients_with_3d)}/{len(active_clients)}\n"
    msg += f"Missing: {len(missing_3d)} | Alerts: {total_alerts_3d}\n\n"

    msg += f"<b>Last 7 Days ({start_date_7d.strftime('%m/%d')} - {end_date.strftime('%m/%d')})</b>\n"
    msg += f"Clients with Data: {len(clients_with_7d)}/{len(active_clients)}\n"
    msg += f"Missing: {len(missing_7d)} | Alerts: {total_alerts_7d}\n"

    if missing_7d:
        msg += f"\n<b>Missing (all 7 days):</b>\n"
        for client_id, client_name in missing_7d:
            msg += f"  - {client_id}: {client_name}\n"

    send_telegram_message(msg)

    if missing_3d:
        exit(1)


def send_telegram_message(message):
    """Send a message via Telegram bot."""
    url = f"https://api.telegram.org/{TELEGRAM_BOT_TOKEN}/sendMessage" if TELEGRAM_BOT_TOKEN.startswith("bot") else f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        print("Telegram notification sent successfully")
    except Exception as e:
        print(f"Error sending Telegram notification: {e}")


if __name__ == "__main__":
    main()
