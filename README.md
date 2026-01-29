# Automail Analytics

Automatically download and process Automail data from DynamoDB/S3, and write to Google Sheets daily.

## 🚀 Quick Start

### Run Locally (Interactive)
```bash
python run_all_scripts.py
# Enter year, month, sheet ID when prompted
```

### Run Locally (CLI)
```bash
# Process yesterday's data
python run_all_scripts.py --yesterday

# Process specific date
python run_all_scripts.py --date 2026-01-15

# Process entire month
python run_all_scripts.py --year 2026 --month 01

# Preview without executing
python run_all_scripts.py --yesterday --dry-run
```

---

## ⚙️ Setup

### 1. Clone and Install Dependencies
```bash
git clone <repo-url>
cd Test-create-data-auto-mail
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Local Configuration
```bash
# Copy and edit .env file
cp .env.example .env
```

Contents of `.env`:
```env
BUCKET_NAME=auto-mail-sendgrid-tracking/production
TABLE_NAME=prod-auto-mail-main-pricing-request-db
REGION=ap-northeast-1
AWS_PROFILE=your-aws-profile
CONFIG_SHEET_ID=your-config-sheet-id
```

### 3. Add Google Service Account
- Place `service_account.json` in project root
- Share Editor access to service account email on all Google Sheets

---

## 🤖 GitHub Actions Automation

Workflow runs automatically at **8:00 AM JST** daily.

### Workflow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  GitHub Actions (cron: 0 23 * * * UTC)                      │
│                                                             │
│  1. Checkout code                                           │
│  2. Setup Python + install dependencies                     │
│  3. Configure AWS credentials (from secrets)                │
│  4. Setup Google credentials (from secrets)                 │
│  5. Create .env file                                        │
│  6. Run: python run_all_scripts.py --yesterday              │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  run_all_scripts.py                                         │
│  └── auto_create_sheet.py                                   │
│      ├── Read Config from Google Sheet                      │
│      ├── Check if monthly sheet exists                      │
│      └── Clone template if needed → Save to Config          │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Processing Pipeline                                         │
│  ├── 0.download_item.py   → Download from DynamoDB          │
│  ├── 1.download_parquet.py → Download from S3               │
│  ├── 2.beautify.py        → Convert timestamps to JST       │
│  └── 3.pivot.py           → Merge events → Google Sheets    │
└─────────────────────────────────────────────────────────────┘
```

### Setup Secrets

Go to **Settings → Secrets and variables → Actions**, add:

| Secret | Description |
|--------|-------------|
| `AWS_ACCESS_KEY_ID` | From AWS SSO Portal |
| `AWS_SECRET_ACCESS_KEY` | From AWS SSO Portal |
| `AWS_SESSION_TOKEN` | From AWS SSO Portal |
| `GOOGLE_SERVICE_ACCOUNT` | `base64 -i service_account.json` |
| `BUCKET_NAME` | `auto-mail-sendgrid-tracking/production` |
| `TABLE_NAME` | `prod-auto-mail-main-pricing-request-db` |
| `CONFIG_SHEET_ID` | Config Google Sheet ID |

### Manual Run

1. Go to **Actions** tab
2. Select **"Daily Automail Report"**
3. Click **"Run workflow"**
4. Options:
   - `date`: Enter specific date (YYYY-MM-DD)
   - `dry_run`: Check to preview only

---

## 📊 Config Google Sheet

Create a Google Sheet with 2 worksheets:

### Worksheet `templates`
| type | sheet_id |
|------|----------|
| 30_days | `<30-day template ID>` |
| 31_days | `<31-day template ID>` |

### Worksheet `sheets`
| month_key | sheet_id |
|-----------|----------|
| *(auto-added when creating new sheets)* | |

---

## 📁 Project Structure

```
├── 0.download_item.py      # Download from DynamoDB
├── 1.download_parquet.py   # Download from S3
├── 2.beautify.py           # Convert timestamps
├── 3.pivot.py              # Merge events, write to Google Sheets
├── run_all_scripts.py      # Main orchestrator
├── auto_create_sheet.py    # Auto-create monthly sheets
├── google_sheet_utils.py   # Google Sheets utilities
└── .github/workflows/
    └── daily_report.yml    # GitHub Actions workflow
```
