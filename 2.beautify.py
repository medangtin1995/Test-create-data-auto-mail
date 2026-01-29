import pandas as pd
from datetime import datetime, timezone, timedelta
import os
from dotenv import load_dotenv
import sys

# Load environment variables
os.environ.pop('FULL_MONTH', None)
os.environ.pop('FULL_YEAR', None)
os.environ.pop('FULL_DAY', None)
load_dotenv()

if len(sys.argv) >= 4:
    year = sys.argv[1]
    month = sys.argv[2]
    day = sys.argv[3]
else:
    year = os.getenv('FULL_YEAR')
    month = os.getenv('FULL_MONTH')
    day = os.getenv('FULL_DAY')

# Define a function to convert timestamp to Japan datetime format
def convert_to_japan_time(timestamp):
    if pd.isna(timestamp):
        return None
    dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
    japan_time = dt.astimezone(timezone(timedelta(hours=9)))
    return japan_time.strftime('%Y-%m-%d %H:%M:%S')

# Load the CSV file
try:
    df = pd.read_csv('data/items.csv')
except Exception as e:
    print(f"[WARNING] Failed to read data/items.csv: {e}")
    df = pd.DataFrame()

# Check if DataFrame is empty
if df.empty:
    print(f"[WARNING] No items to process for {year}-{month}-{day}")
    output_filename = f'data/{year}{month}/items_with_japan_time_{year}{month}{day}.csv'
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    pd.DataFrame().to_csv(output_filename, index=False)
    sys.exit(0)

# Apply the function to the timestamp columns
df['created_at_jp'] = df['created_at'].apply(convert_to_japan_time)
df['expired_at_jp'] = df['expired_at'].apply(convert_to_japan_time)
df['flow_assessment_jp'] = df['flow_assessment'].apply(convert_to_japan_time)
df['processing_at_jp'] = df['processing_at'].apply(convert_to_japan_time)
df['sent_at_jp'] = df['sent_at'].apply(convert_to_japan_time)
df['updated_at_jp'] = df['updated_at'].apply(convert_to_japan_time)
df['submitted_at_jp'] = df['submitted_at'].apply(convert_to_japan_time)

# Filter the DataFrame based on the created_at_jp date
df_1 = df[df['created_at_jp'].str.startswith(f"{year}-{month}-{day}")]

# Update answer column: if answer is "no_answer", set it to None
df_1 = df_1.copy()
df_1['answer'] = df_1['answer'].replace('no_answer', None)

# Save the updated DataFrame back to a CSV file with a timestamp in the filename
output_filename = f'data/{year}{month}/items_with_japan_time_{year}{month}{day}.csv'
os.makedirs(os.path.dirname(output_filename), exist_ok=True)
df_1.to_csv(output_filename, index=False)

