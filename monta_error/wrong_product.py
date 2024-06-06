import os
import pandas as pd
from dotenv import load_dotenv
import json
import requests
from requests.auth import HTTPBasicAuth
import io
import xlsxwriter
from datetime import datetime, timedelta
from google.cloud import storage
from google.oauth2 import service_account

# Create weeknumber
today = datetime.now()
date_today = today.strftime("%Y-%m-%d")
other_day = today - timedelta(days=6)
date_other_day = other_day.strftime("%Y-%m-%d")

# Create updated since date
updated_since_date_time = today - timedelta(days=6)
updated_since = updated_since_date_time.strftime("%Y-%m-%dT%H:%M:%S")

# Import keys.env
load_dotenv()

# Define variables
api_url = os.getenv("MONTA_API_URL")
username = os.getenv("MONTA_USERNAME")
password = os.getenv("MONTA_PASSWORD")
order_list_endpoint = f"order/updated_since/{updated_since}"
order_list_url = api_url + order_list_endpoint

# Create response and turn into a DataFrame
order_list_response = requests.get(order_list_url, auth=HTTPBasicAuth(username, password))
order_list_response_data = order_list_response.json()

if not order_list_response_data:
    print("Geen gegevens beschikbaar om DataFrame te maken")
else:
    # Maak de DataFrame met de gegevens
    df = pd.DataFrame(order_list_response_data)

# Extract internal webshop order id's
internal_ids = [order['Order']['WebshopOrderId'] for order in df['Orders'] if order['Order'] is not None]

# Wrong product orders
wrong_orders = [order for order in internal_ids if "(Verkeerd verzonden)" in order or "(verkeerd verzonden)" in order]

# Define the DataFrame
result_df = pd.DataFrame(columns=['Originele Order', 'Verzenddatum', 'Herstel Order ID'])

for wrong_order in wrong_orders:
    # Define new variables
    wrong_order_endpoint = f"order/{wrong_order}"
    wrong_order_url = api_url + wrong_order_endpoint

    # Create response and turn into a DataFrame
    wrong_order_response = requests.get(wrong_order_url, auth=HTTPBasicAuth(username, password))
    wrong_order_response_data = wrong_order_response.json()
    df = pd.DataFrame([wrong_order_response_data])

    # Add Original Order column
    pattern = r'(\d{5,6})'
    df['OriginalOrder'] = df['WebshopOrderId'].str.extract(pattern)

    # Rename the columns
    df.rename(columns={
        'OriginalOrder': 'Originele Order',
        'PlannedShipmentDate': 'Verzenddatum',
        'WebshopOrderId': 'Herstel Order ID'
    }, inplace=True)

    # Select desired columns
    df = df[['Originele Order', 'Verzenddatum', 'Herstel Order ID']]

    # Append DataFrame to result_df
    result_df = result_df._append(df, ignore_index=True)

# Define the Excel buffer
excel_buffer = io.BytesIO()

# Create the layout using Xlsxwriter
workbook = xlsxwriter.Workbook(excel_buffer)
worksheet = workbook.add_worksheet(f'Van {str(date_other_day)} tot {str(date_today)}')

# Define header_format
header_format = workbook.add_format({'bold': True, 'font_color': 'black', 'font_name': 'Arial', 'font_size': 14})
header_format.set_align('left')

# Define basic_format
basic_format = workbook.add_format({'font_color': 'black', 'font_name': 'Arial', 'font_size': 12})
basic_format.set_align('left')

# Set the width of all columns to 15
for col_num, col_name in enumerate(result_df.columns, 1):
    worksheet.set_column(col_num, col_num, 30)

# Write the header of the DataFrame into the Excel-file
for col_num, value in enumerate(result_df.columns.values):
    worksheet.write(0, col_num, value, header_format)

# Write the data from the DataFrame into the rest of the Excel-file
for row_num, row in enumerate(result_df.values):
    for col_num, cell in enumerate(row):
        worksheet.write(row_num + 1, col_num, cell, basic_format)

# Save workbook
workbook.close()

# Save the DataFrame as an Excel-file
excel_buffer.seek(0)

# Filename for the Excel-file
excel_filename = f'Verkeerd Verzonden Orders van {str(date_other_day)} tot {str(date_today)}.xlsx'

# Load Google Cloud credentials from environment variables
credentials_info = {
    "type": os.getenv('GOOGLE_APPLICATION_CREDENTIALS_TYPE'),
    "project_id": os.getenv('GOOGLE_APPLICATION_CREDENTIALS_PROJECT_ID'),
    "private_key_id": os.getenv('GOOGLE_APPLICATION_CREDENTIALS_PRIVATE_KEY_ID'),
    "private_key": os.getenv('GOOGLE_APPLICATION_CREDENTIALS_PRIVATE_KEY').replace('\\n', '\n'),
    "client_email": os.getenv('GOOGLE_APPLICATION_CREDENTIALS_CLIENT_EMAIL'),
    "client_id": os.getenv('GOOGLE_APPLICATION_CREDENTIALS_CLIENT_ID'),
    "auth_uri": os.getenv('GOOGLE_APPLICATION_CREDENTIALS_AUTH_URI'),
    "token_uri": os.getenv('GOOGLE_APPLICATION_CREDENTIALS_TOKEN_URI'),
    "auth_provider_x509_cert_url": os.getenv('GOOGLE_APPLICATION_CREDENTIALS_AUTH_PROVIDER_X509_CERT_URL'),
    "client_x509_cert_url": os.getenv('GOOGLE_APPLICATION_CREDENTIALS_CLIENT_X509_CERT_URL')
}

credentials = service_account.Credentials.from_service_account_info(credentials_info)

# Initialize the Storage client
client = storage.Client(credentials=credentials, project=credentials_info["project_id"])

# Define and select GCP-Bucket
bucket_name = "wrong_order_bucket"
gcs_object_name = excel_filename
bucket = client.bucket(bucket_name)

# Upload the file to the bucket
blob = bucket.blob(gcs_object_name)
blob.upload_from_string(excel_buffer.getvalue(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# Confirmation
print(f"Excel-bestand is geüpload naar GCS-bucket met objectnaam: '{gcs_object_name}'.")