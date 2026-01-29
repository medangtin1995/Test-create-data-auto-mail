"""
Main script to run all data processing scripts for Automail Analytics.
Supports CLI arguments for automation via GitHub Actions.
"""
import subprocess
import sys
import argparse
from calendar import monthrange
from datetime import datetime, timedelta
from pathlib import Path


def get_sheet_id(year: str, month: str, dry_run: bool = False) -> str:
    """Get or create sheet ID for the given month."""
    try:
        from auto_create_sheet import get_or_create_monthly_sheet
        return get_or_create_monthly_sheet(year, month, dry_run)
    except ImportError:
        # Fallback: use SHEET_ID from env
        import os
        from dotenv import load_dotenv
        load_dotenv()
        return os.getenv("SHEET_ID", "")


def run_script(script_name: str, year: str, month: str, day: str, sheet_id: str) -> bool:
    """Run a child script with the specified arguments."""
    result = subprocess.run(
        [sys.executable, script_name, year, month, day, sheet_id],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"Error running {script_name}: {result.stderr}")
        return False
    print(f"Output of {script_name}: {result.stdout}")
    return True


def process_date(year: str, month: str, day: str, sheet_id: str, dry_run: bool = False) -> bool:
    """Process data for a single date."""
    scripts = [
        "0.download_item.py",
        "1.download_parquet.py",
        "2.beautify.py",
        "3.pivot.py"
    ]
    
    print(f"\n=== Processing {year}-{month}-{day} ===")
    
    if dry_run:
        print(f"[DRY-RUN] Would run scripts: {scripts}")
        print(f"[DRY-RUN] Sheet ID: {sheet_id}")
        return True
    
    for script in scripts:
        if not run_script(script, year, month, day, sheet_id):
            print(f"Stopping execution due to error in {script}")
            return False
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Automail Analytics Data Processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_all_scripts.py --yesterday              # Process yesterday's data
  python run_all_scripts.py --date 2026-01-15        # Process specific date
  python run_all_scripts.py --year 2026 --month 01   # Process entire month
  python run_all_scripts.py --month-to-date          # Process from 1st to today
  python run_all_scripts.py --yesterday --dry-run    # Preview without executing
        """
    )
    
    # Date options (mutually exclusive)
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--yesterday",
        action="store_true",
        help="Process yesterday's data (for daily automation)"
    )
    date_group.add_argument(
        "--date",
        type=str,
        help="Process specific date (YYYY-MM-DD format)"
    )
    date_group.add_argument(
        "--year",
        type=str,
        help="Year to process (requires --month)"
    )
    date_group.add_argument(
        "--month-to-date",
        action="store_true",
        help="Process from 1st of current month to today"
    )
    
    parser.add_argument(
        "--month",
        type=str,
        help="Month to process (requires --year, processes all days)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without executing"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.year and not args.month:
        parser.error("--year requires --month")
    if args.month and not args.year:
        parser.error("--month requires --year")
    
    # Determine dates to process
    dates_to_process = []
    
    if args.month_to_date:
        today = datetime.now()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        for day in range(1, today.day + 1):
            dates_to_process.append((year, month, f"{day:02d}"))
    elif args.yesterday:
        target = datetime.now() - timedelta(days=1)
        dates_to_process.append((
            target.strftime("%Y"),
            target.strftime("%m"),
            target.strftime("%d")
        ))
    elif args.date:
        try:
            target = datetime.strptime(args.date, "%Y-%m-%d")
            dates_to_process.append((
                target.strftime("%Y"),
                target.strftime("%m"),
                target.strftime("%d")
            ))
        except ValueError:
            parser.error("--date must be in YYYY-MM-DD format")
    elif args.year and args.month:
        year = args.year
        month = args.month.zfill(2)
        num_days = monthrange(int(year), int(month))[1]
        for day in range(1, num_days + 1):
            dates_to_process.append((year, month, f"{day:02d}"))
    else:
        # Interactive mode (fallback for backward compatibility)
        print("No arguments provided. Running in interactive mode.")
        year = input("Nhập năm (YYYY): ")
        month = input("Nhập tháng (MM): ").zfill(2)
        sheet_id = input("Nhập SHEET_ID: ")
        num_days = monthrange(int(year), int(month))[1]
        for day in range(1, num_days + 1):
            dates_to_process.append((year, month, f"{day:02d}"))
        
        # Process with manually entered sheet_id
        print(f"\n{'='*50}")
        print(f"Processing {len(dates_to_process)} date(s)")
        print(f"Sheet ID: {sheet_id}")
        print(f"{'='*50}\n")
        
        success_count = 0
        for y, m, d in dates_to_process:
            if process_date(y, m, d, sheet_id, False):
                success_count += 1
        
        print(f"\n{'='*50}")
        print(f"Completed: {success_count}/{len(dates_to_process)} dates processed successfully")
        print(f"{'='*50}")
        return 0 if success_count == len(dates_to_process) else 1
    
    if not dates_to_process:
        parser.error("No dates to process. Use --yesterday, --date, or --year/--month")
    
    # Get sheet ID for the month (auto-lookup)
    first_year, first_month, _ = dates_to_process[0]
    sheet_id = get_sheet_id(first_year, first_month, args.dry_run)
    
    print(f"\n{'='*50}")
    print(f"Processing {len(dates_to_process)} date(s)")
    print(f"Sheet ID: {sheet_id}")
    print(f"Dry run: {args.dry_run}")
    print(f"{'='*50}\n")
    
    # Process each date
    success_count = 0
    for year, month, day in dates_to_process:
        if process_date(year, month, day, sheet_id, args.dry_run):
            success_count += 1
    
    print(f"\n{'='*50}")
    print(f"Completed: {success_count}/{len(dates_to_process)} dates processed successfully")
    print(f"{'='*50}")
    
    return 0 if success_count == len(dates_to_process) else 1


if __name__ == "__main__":
    sys.exit(main())
