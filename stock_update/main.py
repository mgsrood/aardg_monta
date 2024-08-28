from dotenv import load_dotenv
import os
import json
from modules.write import write_to_google_sheets
from modules.get_request import get_request

if __name__ == "__main__":

    load_dotenv()

    # Laad variabelen
    api_url = os.getenv("MONTA_API_URL")
    username = os.getenv("MONTA_USERNAME")
    password = os.getenv("MONTA_PASSWORD")
    gc_keys_path = os.getenv("AARDG_GOOGLE_CREDENTIALS")
    with open(gc_keys_path) as f:
        gc_keys = json.load(f)

    # Define the spreadsheet and worksheets
    spreadsheet = "5. Voorraad"
    worksheet_1 = "Doos Voorraad"
    worksheet_2 = "Unit Voorraad"

    # Run Get Request
    try:
        box_values, unit_values = get_request(api_url, username, password)
        print("Data successfully retrieved")
    except Exception as e:
        print(f"An error occurred: {e}")

    # Write to Google Sheets
    try:
        write_to_google_sheets(gc_keys, spreadsheet, worksheet_1, box_values)
        print("Data successfully written to Google Sheets")
    except Exception as e:
        print(f"An error occurred: {e}")

    try:
        write_to_google_sheets(gc_keys, spreadsheet, worksheet_2, unit_values)
        print("Data successfully written to Google Sheets")
    except Exception as e:
        print(f"An error occurred: {e}")
