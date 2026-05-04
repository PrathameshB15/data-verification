"""
Weekly data verification job.

Verifies the last N days (default 30) of order data for every active client of a
given CRM. Designed to run on Jenkins on a weekly cadence.

- Always runs in dry-run mode: this is a reporting job, not a flag-update job.
  The data_verified column is owned by the daily verification cron.
- Retries each (client, date) with exponential backoff when verify_data returns
  an ERROR status (e.g. transient network/API failures).
- Exits 1 if any (client, date) ends in FAIL or ERROR after all retries, so
  Jenkins can mark the build unstable/failed.

Usage:
    python weekly_verification.py [--crm Sticky] [--days 30] [--retries 2]
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd

from index import get_crm_credentials, verify_data


DEMO_CLIENT_IDS = {10000, 10027}


def verify_with_retry(crm, target_date, max_retries, backoff_base=5):
    """Run verify_data with retry on ERROR. PASS/FAIL are returned immediately."""
    result = None
    for attempt in range(max_retries + 1):
        result = verify_data(crm, target_date, dry_run=True)
        if result.get("status") != "ERROR":
            return result
        if attempt < max_retries:
            wait = backoff_base * (3 ** attempt)
            print(
                f"[retry] {crm.CLIENT_NAME} (id={crm.CLIENT_ID}) "
                f"{target_date.strftime('%m/%d/%Y')}: ERROR — "
                f"retrying in {wait}s (attempt {attempt + 2}/{max_retries + 1})"
            )
            time.sleep(wait)
    return result


def build_excel_rows(results):
    rows = []
    for r in results:
        crm_count = r.get("crm_count")
        blob_total = r.get("blob_total")
        blob_non_test = r.get("blob_non_test")
        db_count = r.get("db_count")
        rows.append({
            "Client ID": r.get("client_id"),
            "Client Name": r.get("client_name"),
            "Date": r.get("date"),
            "API Count": crm_count,
            "Blob Count": blob_total,
            "Test Orders": r.get("blob_test"),
            "Non-Test Orders": blob_non_test,
            "DB Count": db_count,
            "API→Blob %": (
                round((blob_total / crm_count) * 100, 2)
                if isinstance(crm_count, int) and isinstance(blob_total, int) and crm_count > 0
                else None
            ),
            "Blob→DB %": (
                round(min(blob_non_test, db_count) / max(blob_non_test, db_count) * 100, 2)
                if isinstance(blob_non_test, int) and isinstance(db_count, int) and max(blob_non_test, db_count) > 0
                else None
            ),
            "API→DB %": r.get("api_db_pct"),
            "Status": r.get("status"),
            "Error": r.get("error"),
        })
    return rows


def main():
    parser = argparse.ArgumentParser(description="Weekly verification for last N days, all clients of a CRM.")
    parser.add_argument("--crm", default="Sticky", help="CRM name (default: Sticky)")
    parser.add_argument("--client-id", type=int, default=None, help="Run for a single client ID instead of all active clients")
    parser.add_argument("--days", type=int, default=30, help="Number of trailing days (default: 30, ignored if --start-date/--end-date given)")
    parser.add_argument("--start-date", type=str, default=None, help="Start date YYYY-MM-DD (inclusive). Use with --end-date to override --days.")
    parser.add_argument("--end-date", type=str, default=None, help="End date YYYY-MM-DD (inclusive). Use with --start-date to override --days.")
    parser.add_argument("--retries", type=int, default=2, help="Retries on ERROR per (client, date) (default: 2)")
    parser.add_argument("--max-date-workers", type=int, default=3, help="Parallel dates per client (default: 3)")
    parser.add_argument("--no-export", action="store_true", help="Skip xlsx export")
    parser.add_argument(
        "--exclude-clients",
        type=str,
        default=",".join(str(i) for i in sorted(DEMO_CLIENT_IDS)),
        help=f"Comma-separated client IDs to skip (default: {sorted(DEMO_CLIENT_IDS)} — demo accounts)",
    )
    args = parser.parse_args()

    excluded = {int(x) for x in args.exclude_clients.split(",") if x.strip()}

    if bool(args.start_date) ^ bool(args.end_date):
        print("--start-date and --end-date must be used together")
        sys.exit(2)
    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
        if end_date < start_date:
            print("--end-date must be on or after --start-date")
            sys.exit(2)
        num_days = (end_date - start_date).days + 1
    else:
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=args.days - 1)
        num_days = args.days
    dates = [start_date + timedelta(days=i) for i in range(num_days)]

    print(f"Weekly verification — CRM: {args.crm}")
    print(f"Range: {start_date.strftime('%m/%d/%Y')} → {end_date.strftime('%m/%d/%Y')} ({num_days} days)")
    print(f"Retries on ERROR: {args.retries}")

    crm_list = get_crm_credentials(args.crm, client_id=args.client_id)
    if not crm_list:
        print(f"No CRM credentials found for CRM: {args.crm}")
        sys.exit(2)

    seen = set()
    unique = []
    skipped = []
    for c in crm_list:
        if c.CLIENT_ID in seen:
            continue
        seen.add(c.CLIENT_ID)
        if c.CLIENT_ID in excluded:
            skipped.append(c)
            continue
        unique.append(c)
    crm_list = unique

    if skipped:
        print(f"Skipping {len(skipped)} excluded client(s): " +
              ", ".join(f"{c.CLIENT_NAME} (id={c.CLIENT_ID})" for c in skipped))
    print(f"Verifying {len(crm_list)} client(s) over {len(dates)} day(s)...\n")

    filename = (
        None if args.no_export
        else f"{args.crm.lower()}_weekly_verification_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    )
    all_results = []

    for crm in crm_list:
        print(f"\n{'#' * 60}")
        print(f"# Client: {crm.CLIENT_NAME} (ID: {crm.CLIENT_ID})")
        print(f"{'#' * 60}")

        client_results = []
        max_workers = min(len(dates), max(1, args.max_date_workers))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(verify_with_retry, crm, d, args.retries): d
                for d in dates
            }
            for fut in as_completed(futures):
                client_results.append(fut.result())
        client_results.sort(key=lambda r: r.get("date", ""))
        all_results.extend(client_results)

        passed = sum(1 for r in client_results if r.get("status") == "PASS")
        failed = sum(1 for r in client_results if r.get("status") == "FAIL")
        errors = sum(1 for r in client_results if r.get("status") == "ERROR")
        print(f"\n--- {crm.CLIENT_NAME}: Passed={passed} | Failed={failed} | Errors={errors} ---")

        if filename:
            pd.DataFrame(build_excel_rows(all_results)).to_excel(filename, index=False)
            print(f"Excel updated: {filename} ({len(all_results)} rows)")

    print("\n" + "=" * 80)
    print("WEEKLY VERIFICATION SUMMARY")
    print("=" * 80)
    passed = sum(1 for r in all_results if r.get("status") == "PASS")
    failed = sum(1 for r in all_results if r.get("status") == "FAIL")
    errors = sum(1 for r in all_results if r.get("status") == "ERROR")
    print(
        f"CRM: {args.crm} | Clients: {len(crm_list)} | Days: {len(dates)} | "
        f"Total: {len(all_results)} | Passed: {passed} | Failed: {failed} | "
        f"Errors (after retries): {errors}"
    )
    if filename:
        print(f"Report: {filename}")

    if failed > 0 or errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
