import csv
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timezone, timedelta
from google_sheet_utils import update_google_sheet
import numpy as np
import sys

# Define a function to convert timestamp to Japan datetime format
def convert_to_japan_time(timestamp):
    if pd.isna(timestamp):
        return None
    dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
    japan_time = dt.astimezone(timezone(timedelta(hours=9)))
    return japan_time.strftime("%Y-%m-%d %H:%M:%S")


def save_to_csv(data, file_path):
    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)


# Select request from items.
def select_request_from_items(csv_filepath, year, month, day):
    requests = []
    df = pd.read_csv(csv_filepath)
    for _, row in df.iterrows():
        flow_assessment_jp = row.get("flow_assessment_jp")
        if isinstance(flow_assessment_jp, str) and flow_assessment_jp.startswith(
            f"{year}-{month}-{day}"
        ):
            request_id = row.get("request_id")
            lambda_email_status = row.get("request_status")
            lambda_sent_at = row.get("sent_at_jp")
            answer = row.get("answer")
            submitted_at = row.get("submitted_at_jp")
            lambda_sms_status = row.get("sms_status")
            total_price = row.get("total_price")
            cancel_reason = row.get("reason_cancel")
            requests.append(
                {
                    "request_id": request_id,
                    "lambda_email_status": lambda_email_status,
                    "lambda_sent_at": lambda_sent_at,
                    "answer": answer,
                    "answered_at": submitted_at,
                    "lambda_sms_status": lambda_sms_status,
                    "total_price": total_price,
                    "cancel_reason": cancel_reason
                }
            )
    return requests


# Merge all parquet files into one CSV file
def merge_parquet_files_to_csv(parquet_dir, csv_filepath):
    # Check if directory exists
    if not os.path.exists(parquet_dir):
        print(f"[WARNING] Directory not found: {parquet_dir}")
        os.makedirs(os.path.dirname(csv_filepath), exist_ok=True)
        pd.DataFrame().to_csv(csv_filepath, index=False)
        return
    
    all_files = [
        f"{parquet_dir}/{f}" for f in os.listdir(parquet_dir) if f.endswith(".parquet")
    ]
    
    # Handle empty directory
    if not all_files:
        print(f"[WARNING] No parquet files found in: {parquet_dir}")
        os.makedirs(os.path.dirname(csv_filepath), exist_ok=True)
        pd.DataFrame().to_csv(csv_filepath, index=False)
        return
    
    df = pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)

    # Ensure the directory exists
    os.makedirs(os.path.dirname(csv_filepath), exist_ok=True)
    df.to_csv(csv_filepath, index=False)


# Select request has request_id and event exists in merged events.
def select_request_with_event(requests, event_filepath, event_name):
    try:
        df = pd.read_csv(event_filepath)
    except Exception as e:
        print(f"[WARNING] Failed to read events file: {e}")
        for request in requests:
            request[f"{event_name}_at"] = None
        return requests
    
    if df.empty:
        for request in requests:
            request[f"{event_name}_at"] = None
        return requests
    
    events = df[df["event"] == event_name]
    request_ids = events["request_id"].unique()
    event_dict = events.set_index("request_id")["timestamp"].to_dict()

    for request in requests:
        if request["request_id"] in request_ids:
            timestamp = event_dict[request["request_id"]]
            request[f"{event_name}_at"] = convert_to_japan_time(timestamp)

        else:
            request[f"{event_name}_at"] = None

    return requests


def select_request_with_sg(requests, event_filepath):
    try:
        df = pd.read_csv(event_filepath)
    except Exception as e:
        print(f"[WARNING] Failed to read events file for sg: {e}")
        for request in requests:
            request["sg_template_name"] = None
        return requests
    
    if df.empty:
        for request in requests:
            request["sg_template_name"] = None
        return requests
    
    events = df[df["event"] == "processed"]
    request_ids = events["request_id"].unique()
    template_dict = events.set_index("request_id")["sg_template_name"].to_dict()

    for request in requests:
        if request["request_id"] in request_ids:
            request["sg_template_name"] = template_dict[request["request_id"]]

        else:
            request["sg_template_name"] = None

    return requests


os.environ.pop("FULL_MONTH", None)
os.environ.pop("FULL_YEAR", None)
os.environ.pop("FULL_DAY", None)
os.environ.pop("SHEET_ID", None)
load_dotenv()
# month = os.getenv("FULL_MONTH")
# year = os.getenv("FULL_YEAR")
# day = os.getenv("FULL_DAY")

if len(sys.argv) >= 5:
    year = sys.argv[1]
    month = sys.argv[2]
    day = sys.argv[3]
    sheet_id = sys.argv[4]
else:
    year = os.getenv('FULL_YEAR')
    month = os.getenv('FULL_MONTH')
    day = os.getenv('FULL_DAY')
    sheet_id = os.getenv('SHEET_ID')

csv_filepath = f"data/{year}{month}/items_with_japan_time_{year}{month}{day}.csv"
requests = select_request_from_items(csv_filepath, year, month, day)

email_event_dir = f"email-events/year={year}/month={month}/day={day}"
event_filepath = f"events/merged_events_{year}{month}{day}.csv"
merge_parquet_files_to_csv(email_event_dir, event_filepath)

print(f"Merged events saved to {event_filepath}")

requests_with_sg = select_request_with_sg(requests, event_filepath)

requests_with_processed = select_request_with_event(
    requests, event_filepath, "processed"
)
requests_with_dropped = select_request_with_event(requests, event_filepath, "dropped")
requests_with_deferred = select_request_with_event(requests, event_filepath, "deferred")
requests_with_bounce = select_request_with_event(requests, event_filepath, "bounce")
requests_with_delivered = select_request_with_event(
    requests, event_filepath, "delivered"
)
requests_with_open = select_request_with_event(requests, event_filepath, "open")
requests_with_click = select_request_with_event(requests, event_filepath, "click")
requests_with_spamreport = select_request_with_event(
    requests, event_filepath, "spamreport"
)

# Save final requests
output_filepath = f"requests/{year}{month}/requests_{year}{month}{day}.csv"
save_to_csv(requests, output_filepath)

print(f"Writing requests to {output_filepath}")

# Convert list to a DataFrame
requests_df = pd.DataFrame(requests)
requests_df = requests_df.replace({np.nan: None})

# Update Google Sheets (sheet_id from command line or env)
sheet_name = day
range_names = [
    f"A2:A{len(requests) + 1}",
    f"B2:B{len(requests) + 1}",
    f"C2:C{len(requests) + 1}",
    f"D2:D{len(requests) + 1}",
    f"E2:E{len(requests) + 1}",
    f"F2:F{len(requests) + 1}",
    f"G2:G{len(requests) + 1}",
    f"H2:H{len(requests) + 1}",
    f"I2:I{len(requests) + 1}",
    f"J2:J{len(requests) + 1}",
    f"K2:K{len(requests) + 1}",
    f"L2:L{len(requests) + 1}",
    f"M2:M{len(requests) + 1}",
    f"N2:N{len(requests) + 1}",
    f"O2:O{len(requests) + 1}",
    f"P2:P{len(requests) + 1}",
    f"Q2:Q{len(requests) + 1}",
]
values = [
    requests_df[["request_id"]].values.tolist(),
    requests_df[["lambda_email_status"]].values.tolist(),
    requests_df[["lambda_sent_at"]].values.tolist(),
    requests_df[["answer"]].values.tolist(),
    requests_df[["answered_at"]].values.tolist(),
    requests_df[["lambda_sms_status"]].values.tolist(),
    requests_df[["total_price"]].values.tolist(),
    requests_df[["sg_template_name"]].values.tolist(),
    requests_df[["processed_at"]].values.tolist(),
    requests_df[["dropped_at"]].values.tolist(),
    requests_df[["deferred_at"]].values.tolist(),
    requests_df[["bounce_at"]].values.tolist(),
    requests_df[["delivered_at"]].values.tolist(),
    requests_df[["open_at"]].values.tolist(),
    requests_df[["click_at"]].values.tolist(),
    requests_df[["spamreport_at"]].values.tolist(),
    requests_df[["cancel_reason"]].values.tolist(),
]
creds_file = "service_account.json"
update_google_sheet(sheet_id, sheet_name, range_names, values, creds_file)

print(f"Updated Google Sheets with data for {year}-{month}-{day} in sheet '{sheet_name}' - {sheet_id}")
