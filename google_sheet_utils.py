import logging
import gspread
from google.oauth2.service_account import Credentials

def update_google_sheet(sheet_id, sheet_name, range_names, values, creds_file):
    """
    Updates a Google Sheet with the specified values.

    Args:
        sheet_id (str): The ID of the Google Sheet.
        range_name (str): The range to update (e.g., "Sheet1!A1:C3").
        values (list of list): The values to insert (e.g., [["A", "B", "C"], [1, 2, 3]]).
        creds_file (str): Path to the service account JSON credentials file.

    Returns:
        str: Confirmation message.
    """
    try:
        # Authenticate using the service account
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(creds_file, scopes=scope)
        client = gspread.authorize(creds)

        # Open the Google Sheet by ID
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.worksheet(sheet_name)
        
        # Update the specified ranges
        for range_name, value in zip(range_names, values):
            worksheet.update(range_name=range_name, values=value)

        logging.info(f"Successfully updated range {range_name} in Google Sheet.")
    except Exception as e:
        logging.error(f"Error updating Google Sheet: {str(e)}")