import gspread
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime

def write_to_google_sheets(gc_keys, spreadsheet, worksheet, dictionary):
    # Current date
    current_date = datetime.now().strftime("%d-%m-%Y")
    
    # Determine the scope
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets",'https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

    # Setup Google Sheets API credentials
    credentials = service_account.Credentials.from_service_account_info(gc_keys, scopes=SCOPES)
    client = gspread.authorize(credentials)

    # Open Google Sheet
    spreadsheet = client.open(f"{spreadsheet}")
    worksheet = spreadsheet.worksheet(f"{worksheet}")

    # Determine starting point
    row_index = len(worksheet.col_values(1)) + 1

    # Write current datetime
    worksheet.update_cell(row_index, 1, current_date)

    # Write box values
    col_index = 2 
    for value in dictionary.values():
        worksheet.update_cell(row_index, col_index, value)  # Write the value to the cells
        col_index += 1