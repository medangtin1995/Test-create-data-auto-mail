import subprocess
import os
import shutil
from dotenv import load_dotenv
import sys

def download_s3_files(bucket_name, folder_path, local_path, profile_name):
    """
    Download files from an S3 bucket using AWS CLI.

    :param bucket_name: The name of the S3 bucket.
    :param folder_path: The folder path in the S3 bucket to download files from.
    :param local_path: The local directory to download files to.
    :param profile_name: The AWS CLI profile name to use for authentication.
    """
    try:

        # Delete folders v1 and v2 if they exist
        if os.path.exists(local_path):
            shutil.rmtree(local_path)
            print(f"Deleted folder: {local_path}")

        command = [
            "aws", "s3", "cp",
            f"s3://{bucket_name}/{folder_path}",
            folder_path,
            "--recursive",
            "--profile", profile_name
        ]

        print(f"Executing command: {' '.join(command)}")
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        print("Download successful.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error during download:")
        print(e.stderr)

# Example usage:
os.environ.pop('FULL_DAY', None)
os.environ.pop('FULL_MONTH', None)
os.environ.pop('FULL_YEAR', None)
load_dotenv()
bucket_name = os.getenv('BUCKET_NAME')
# day = os.getenv('FULL_DAY')
# month = os.getenv('FULL_MONTH')
# year = os.getenv('FULL_YEAR')

if len(sys.argv) >= 4:
    year = sys.argv[1]
    month = sys.argv[2]
    day = sys.argv[3]
else:
    year = os.getenv('FULL_YEAR')
    month = os.getenv('FULL_MONTH')
    day = os.getenv('FULL_DAY')

profile_name = os.getenv('AWS_PROFILE')

folder_path = f'email-events/year={year}/month={month}/day={day}/'
local_path = f'email-events/year={year}/month={month}/day={day}'

download_s3_files(bucket_name, folder_path, local_path, profile_name)
