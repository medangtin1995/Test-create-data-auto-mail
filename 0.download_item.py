import boto3
from boto3.dynamodb.conditions import Attr
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
import sys

os.environ.pop('TABLE_NAME', None)
os.environ.pop('REGION', None)
# os.environ.pop('FULL_YEAR', None)
# os.environ.pop('FULL_MONTH', None)
# os.environ.pop('FULL_DAY', None)
load_dotenv()
table_name = os.getenv('TABLE_NAME')
region_name = os.getenv('REGION')


if len(sys.argv) >= 4:
    year = sys.argv[1]
    month = sys.argv[2]
    day = sys.argv[3]
else:
    year = os.getenv('FULL_YEAR')
    month = os.getenv('FULL_MONTH')
    day = os.getenv('FULL_DAY')

def download_items_from_dynamodb(table_name, region_name, start_date=None, end_date=None):
    """
    Download items from a DynamoDB table between two dates.

    :param table_name: Name of the DynamoDB table
    :param region_name: AWS region where the table is located
    :param start_date: Start date string in 'YYYY-MM-DD' format
    :param end_date: End date string in 'YYYY-MM-DD' format
    :return: List of items from the table
    """
    dynamodb = boto3.resource('dynamodb', region_name=region_name)
    table = dynamodb.Table(table_name)

    try:
        scan_kwargs = {}
        if start_date and end_date:
            start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
            end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
            scan_kwargs['FilterExpression'] = Attr('created_at').between(start_ts, end_ts)
        elif start_date:
            start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
            scan_kwargs['FilterExpression'] = Attr('created_at').gte(start_ts)
        elif end_date:
            end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
            scan_kwargs['FilterExpression'] = Attr('created_at').lte(end_ts)

        response = table.scan(**scan_kwargs)
        items = response.get('Items', [])

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **scan_kwargs)
            items.extend(response.get('Items', []))

        return items
    except Exception as e:
        print(f"Error downloading items from DynamoDB: {e}")
        return []

# Example usage
if __name__ == "__main__":
    date_filter = (datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    to_date = (datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    items = download_items_from_dynamodb(table_name, region_name, date_filter, to_date)
    # items = download_items_from_dynamodb(table_name, region_name, date_filter, None)

    # Save items to a CSV file
    df = pd.DataFrame(items)
    output_filepath = "data/items.csv"
    df.to_csv(output_filepath, index=False)

    print(f"Downloaded {len(items)} items from DynamoDB table '{table_name}' with date filter '{date_filter}'. {sys.argv}")
