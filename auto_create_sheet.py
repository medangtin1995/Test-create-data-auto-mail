"""
Auto-create Google Sheet for each month by cloning from template.
Uses 30-day or 31-day template based on the month.
Stores mapping in a Config Google Sheet for persistence across CI runs.
"""
import os
import calendar
from pathlib import Path
import gspread
from google.oauth2.service_account import Credentials
from google_sheet_utils import clone_template_sheet
from dotenv import load_dotenv

load_dotenv()

CREDS_FILE = Path(__file__).parent / "service_account.json"

# Config Sheet ID - stores templates and monthly sheet mappings
# Format: Row 1 = headers, Row 2+ = data
# Sheet "templates": type, sheet_id
# Sheet "sheets": month_key, sheet_id
CONFIG_SHEET_ID = os.getenv("CONFIG_SHEET_ID", "")

# Validate credentials file exists
if not CREDS_FILE.exists():
    raise FileNotFoundError(
        f"Credentials file not found: {CREDS_FILE}\n"
        "For local: Place service_account.json in project root.\n"
        "For GitHub Actions: Set GOOGLE_SERVICE_ACCOUNT secret (base64 encoded)."
    )


def get_gspread_client():
    """Get authenticated gspread client."""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(str(CREDS_FILE), scopes=scope)
    return gspread.authorize(creds)


def load_mapping_from_sheet() -> dict:
    """Load sheets mapping from Google Sheet."""
    try:
        client = get_gspread_client()
        config_sheet = client.open_by_key(CONFIG_SHEET_ID)
        
        # Load templates from "templates" worksheet
        templates_ws = config_sheet.worksheet("templates")
        templates_data = templates_ws.get_all_records()
        templates = {row["type"]: row["sheet_id"] for row in templates_data}
        
        # Load sheets from "sheets" worksheet
        sheets_ws = config_sheet.worksheet("sheets")
        sheets_data = sheets_ws.get_all_records()
        sheets = {row["month_key"]: row["sheet_id"] for row in sheets_data}
        
        return {"templates": templates, "sheets": sheets}
    except Exception as e:
        print(f"[WARNING] Failed to load mapping from sheet: {e}")
        return {"templates": {}, "sheets": {}}


def save_sheet_mapping(month_key: str, sheet_id: str) -> None:
    """Save a single sheet mapping to Google Sheet."""
    try:
        client = get_gspread_client()
        config_sheet = client.open_by_key(CONFIG_SHEET_ID)
        sheets_ws = config_sheet.worksheet("sheets")
        
        # Append new row
        sheets_ws.append_row([month_key, sheet_id])
        print(f"[INFO] Saved mapping: {month_key} -> {sheet_id}")
    except Exception as e:
        print(f"[ERROR] Failed to save mapping: {e}")


def get_template_id(year: int, month: int, templates: dict) -> str:
    """Get appropriate template ID based on number of days in month."""
    days_in_month = calendar.monthrange(year, month)[1]
    
    if days_in_month <= 30:
        return templates.get("30_days", "")
    else:
        return templates.get("31_days", "")


def get_or_create_monthly_sheet(year: str, month: str, dry_run: bool = False) -> str:
    """
    Get existing sheet ID or create new one by cloning template.
    
    Args:
        year: Year string (e.g., "2026")
        month: Month string (e.g., "01")
        dry_run: If True, only print what would happen
        
    Returns:
        Sheet ID for the specified month
    """
    mapping = load_mapping_from_sheet()
    key = f"{year}{month}"
    
    # Check if sheet already exists
    if key in mapping.get("sheets", {}):
        sheet_id = mapping["sheets"][key]
        print(f"[INFO] Sheet for {year}-{month} already exists: {sheet_id}")
        return sheet_id
    
    # Get appropriate template
    template_id = get_template_id(int(year), int(month), mapping.get("templates", {}))
    days_in_month = calendar.monthrange(int(year), int(month))[1]
    new_name = f"Automail {year}{month}"
    
    if not template_id:
        print(f"[WARNING] No template ID found. Check 'templates' sheet in config.")
        import os
        from dotenv import load_dotenv
        load_dotenv()
        return os.getenv("SHEET_ID", "")
    
    if dry_run:
        print(f"[DRY-RUN] Would clone template ({days_in_month} days) for: {new_name}")
        print(f"[DRY-RUN] Template ID: {template_id}")
        return f"dry-run-sheet-{key}"
    
    # Clone template
    print(f"[INFO] Creating new sheet for {year}-{month} ({days_in_month} days)...")
    new_sheet_id = clone_template_sheet(
        template_id=template_id,
        new_name=new_name,
        creds_file=str(CREDS_FILE)
    )
    
    # Save to Google Sheet
    save_sheet_mapping(key, new_sheet_id)
    
    print(f"[INFO] Created new sheet: {new_sheet_id}")
    return new_sheet_id


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create monthly Google Sheet")
    parser.add_argument("--year", required=True, help="Year (YYYY)")
    parser.add_argument("--month", required=True, help="Month (MM)")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing")
    
    args = parser.parse_args()
    
    sheet_id = get_or_create_monthly_sheet(args.year, args.month, args.dry_run)
    print(f"Sheet ID: {sheet_id}")
