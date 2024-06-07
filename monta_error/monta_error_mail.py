import os
from google.cloud import bigquery
import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
import requests
from requests.auth import HTTPBasicAuth
import gspread
import math
import io
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import smtplib
import xlsxwriter
from datetime import datetime, timedelta
import calendar
import time
import re
from google.cloud import storage

# Email
recipient_mail = 'Henk.denBoer@monta.nl'

# Create month-variable
today = datetime.now()
first_day_current_month = today.replace(day=1)
last_day_last_month = first_day_current_month - timedelta(days=1)
last_month = calendar.month_name[last_day_last_month.month]
last_month_number = last_day_last_month.month
first_day_last_month = last_day_last_month.replace(day=1)
year_of_last_month = first_day_last_month.year

# Fromat dates for SQL
start_date = first_day_last_month.strftime('%Y-%m-%d')
end_date = last_day_last_month.strftime('%Y-%m-%d')

# Import keys.env
load_dotenv()

# Get the GCP keys
gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

credentials = service_account.Credentials.from_service_account_file(gc_keys)
project_id = credentials.project_id

client = bigquery.Client(credentials=credentials, project=project_id)

# Define the SQL query
order_sql_query = f"""
SELECT
    order_id
    FROM `aardg-data.woocommerce_data.orders`
    WHERE DATE(date_created) BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date_created ASC
"""

# Execute the query and fetch the result
order_query_job = client.query(order_sql_query)
order_results = order_query_job.result()

# Extract the column names from the schema
order_column_names = [field.name for field in order_results.schema]

# Create a Pandas DataFrame from the query result
df_p = pd.DataFrame(data=[list(row.values()) for row in order_results], columns=order_column_names)

# Add two columns for package and mailbox
df_p['p_package'] = 0
df_p['p_mailbox'] = 0

# Define variables
api_url = os.getenv("MONTA_API_URL")
username = os.getenv("MONTA_USERNAME")
password = os.getenv("MONTA_PASSWORD")

# Create the function to define the number of shipments
def shipments(order_id):
    endpoint = f"order/{order_id}/colli"
    url = api_url + endpoint
    p_package, p_mailbox = 0, 0
    try:
        response = requests.get(url, auth=HTTPBasicAuth(username, password))
        if response.status_code == 200:
            response_data = response.json()
            for item in response_data:
                if 'Brievenbusdoos wit A5' in item['PackageDescription'] or 'Brievenbusdoos wit A4' in item['PackageDescription']:
                    p_mailbox += 1
                else:
                    p_package += 1
            return p_package, p_mailbox
    except Exception as e:
        print(f"Fout bij het verwerken van order {order_id}: {str(e)}")
    return 0, 0

# Divide the proces in batches
batch_size = 200
order_ids = df_p['order_id'].tolist()
batches = [order_ids[i:i+batch_size] for i in range(0, len(order_ids), batch_size)]

# Run through the batches and fill the df_order DataFrame
batch_nummer = 1
for batch in batches:
    for order_id in batch:
        p, m = shipments(order_id)
        df_p.loc[df_p['order_id'] == order_id, 'p_package'] += p
        df_p.loc[df_p['order_id'] == order_id, 'p_mailbox'] += m
    print(f"Batch {batch_nummer} verwerkt.")

    batch_nummer += 1
    time.sleep(60)  # Wacht 1 minuut voordat je het volgende batchverzoek verzend

# Extract unnested order data from BigQuery

# Define your SQL query
theoretical_sql_query = f"""
SELECT 
    *
    FROM `aardg-data.order_data.unnested_order_data`
    WHERE DATE(date_created) BETWEEN '{start_date}' AND '{end_date}'
"""

# Execute the query and fetch the result
theoretical_query_job = client.query(theoretical_sql_query)
theoretical_results = theoretical_query_job.result()

# Extract the column names from the schema
theoretical_column_names = [field.name for field in theoretical_results.schema]

# Create a Pandas DataFrame from the query result
df_theoretical = pd.DataFrame(data=[list(row.values()) for row in theoretical_results], columns=theoretical_column_names)

# Select the wanted columns and add
df_theory = df_theoretical[['order_id', 'date_created', 'product_id', 'quantity']]

# Define the scope
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

# Get the Google Sheets credentials through the gc_keys.json
creds = service_account.Credentials.from_service_account_file(gc_keys, scopes=scope)

# Authorize the Google Sheet
client = gspread.authorize(creds)

# Open the product_catalogue spreadsheet and extract the data
sheet = client.open('product_catalogue').sheet1
data = sheet.get_all_records()

# Convert the data to a DataFrame
df_sheet = pd.DataFrame(data)

# Select the desired columns
df_product_catalogue = df_sheet[['product_id', 'package_shipments', 'mailbox_shipments']]

# Merge the theoretical DataFrame with the product catalogue DataFrame
df_merged = pd.merge(df_theory, df_product_catalogue, on='product_id', how='left')

# Calculate and create two new columns: i_package and i_mailbox
df_merged['i_package'] = df_merged['quantity'] * df_merged['package_shipments']
df_merged['i_mailbox'] = df_merged['quantity'] * df_merged['mailbox_shipments']

# Group and aggregate based on order_id
df_result = df_merged.groupby('order_id').agg({
    'date_created': 'first',
    'i_package': 'sum',
    'i_mailbox': 'sum'
}).reset_index()

# Define a function to calculate the amount of packages
def calculate_packages(row):
    packages = row['i_package']
    mailboxes = row['i_mailbox']
    if packages >= 1:
        return math.ceil(packages / 3.0)
    if mailboxes >= 3:
        return 1
    else:
        return 0

# Apply the function to define the theoretical packages
df_result['t_package'] = df_result.apply(calculate_packages, axis=1)

# Define a function to calculate the amount of mailboxes
def calculate_mailboxes(row):
    packages = row['i_package']
    mailboxes = row['i_mailbox']
    if packages >= 1:
        return 0
    if mailboxes >= 3:
        return 0
    else:
        return 1

# Apply the function to define the theoretical mailboxes
df_result['t_mailbox'] = df_result.apply(calculate_mailboxes, axis=1)

# Cleanup
df_t = df_result.drop(columns= ['i_package', 'i_mailbox'])

# Defining the unnecessary packages
df = pd.merge(df_p, df_t, on='order_id', suffixes=('_practice', '_theory'))
df['u_package'] = df['p_package'] - df['t_package']
df['u_mailbox'] = df['p_mailbox'] - df['t_mailbox']

# Only select order with wrong packaging
df_excel = df[((df['u_package'] > 0) | (df['u_mailbox'] > 0)) & ((df['u_package'] >= 0) & (df['u_mailbox'] >= 0))]

# Define comment function
def create_comment(row):
    comment = ""
    if row['u_package'] > 0 and row['u_mailbox'] > 0:
        comment = "Uitzonderlijk geval"
    elif row['u_mailbox'] > 0:
        comment = f"Aantal onnodige brievenbuspakketten is {row['u_mailbox']}"
    elif row['u_package'] > 0:
        comment = f"Aantal onnodige pakketten is {row['u_package']}"
    return comment

# Create a comment column
df_excel['comment'] = df_excel.apply(create_comment, axis=1)

# Reformat date_created column
df_excel['date_created'] = pd.to_datetime(df_excel['date_created'])
df_excel['Datum'] = df_excel['date_created'].dt.strftime('%d-%m-%Y')
df_excel.drop(columns=['date_created'], inplace=True)

# Rename the columns
df_excel.rename(columns={
    'order_id': 'Order ID',
    'p_package': 'Verzonden Pakketten',
    'p_mailbox': 'Verzonden Brievenbuspakketten',
    't_package': 'Benodigde Pakketten',
    't_mailbox': 'Benodigde Brievenbuspakketten',
    'u_package': 'Onnodige Pakketten',
    'u_mailbox': 'Onnodige Brievenbuspakketten',
    'comment': 'Opmerking'
}, inplace=True)

# Re-order the columns
df_excel = df_excel[['Datum', 'Order ID', 'Verzonden Pakketten', 'Verzonden Brievenbuspakketten',
                       'Benodigde Pakketten', 'Benodigde Brievenbuspakketten',
                       'Onnodige Pakketten', 'Onnodige Brievenbuspakketten', 'Opmerking']]

# Define the Excel buffer
excel_buffer_1 = io.BytesIO()

# Create the layout using Xlsxwriter
workbook = xlsxwriter.Workbook(excel_buffer_1)
worksheet = workbook.add_worksheet(last_month)

# Define header_format
header_format = workbook.add_format({'bold': True, 'font_color': 'black', 'font_name': 'Arial', 'font_size': 14})
header_format.set_align('left')

# Define basic_format
basic_format = workbook.add_format({'font_color': 'black', 'font_name': 'Arial', 'font_size': 12})
basic_format.set_align('left')

# Set the width of all columns to 15
for col_num, col_name in enumerate(df_excel.columns, 1):
    worksheet.set_column(col_num, col_num, 30)

# Write the header of the DataFrame into the Excel-file
for col_num, value in enumerate(df_excel.columns.values):
    worksheet.write(0, col_num, value, header_format)

# Write the data from the DataFrame into the rest of the Excel-file
for row_num, row in enumerate(df_excel.values):
    for col_num, cell in enumerate(row):
        worksheet.write(row_num + 1, col_num, cell, basic_format)

# Save workbook
workbook.close()

# Save the DataFrame as an Excel-file
excel_buffer_1.seek(0)

# Initialize the Storage client
storage_client = storage.Client(credentials=credentials, project=project_id)

# Define and select GCP-Bucket
bucket_name = "wrong_order_bucket"

# Get all the blobs (files) in the bucket with the specified prefix
blobs = storage_client.list_blobs(bucket_name)

# File names
file_names = []
for blob in blobs:
    file_names.append(blob.name)

# Create variables voor the weeknumbers
lowest_date = datetime.max
highest_date = datetime.min

# Itereer over the file names to find the lowest and highest week numbers
for filename in file_names:
    # Find the date in the file name
    match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
    if match:
        date_str = match.group()
        file_date = datetime.strptime(date_str, "%Y-%m-%d")
        # Update the lowest and highest week numbers
        if lowest_date is None or file_date < lowest_date:
            lowest_date = file_date
        if highest_date is None or file_date > highest_date:
            highest_date = file_date

lowest_date_str = lowest_date.strftime("%Y-%m-%d")
highest_date_str = highest_date.strftime("%Y-%m-%d")

# New Excel filename
new_file_name = f"Verkeerd Verzonden Orders week {lowest_date_str} tot {highest_date_str}.xlsx"

# Create an empty DataFrame
merged_df = pd.DataFrame()

# Iterate over the files
for file_name in file_names:
    # Download the file's content
    blob = storage_client.bucket(bucket_name).blob(file_name)
    file_content = blob.download_as_string()
    # Read the file content into a DataFrame
    df = pd.read_excel(io.BytesIO(file_content))
    # Append it to the DataFrame
    merged_df = pd.concat([merged_df, df], ignore_index=True)
    # Delete the blob after processing
    blob.delete()

# Delete duplicates
merged_df.drop_duplicates(inplace=True)

# Define the Excel buffer
excel_buffer_2 = io.BytesIO()

# Create the layout using Xlsxwriter
workbook = xlsxwriter.Workbook(excel_buffer_2)
worksheet = workbook.add_worksheet(f'Week {lowest_date_str} tot {highest_date_str}')

# Define header_format
header_format = workbook.add_format({'bold': True, 'font_color': 'black', 'font_name': 'Arial', 'font_size': 14})
header_format.set_align('left')

# Define basic_format
basic_format = workbook.add_format({'font_color': 'black', 'font_name': 'Arial', 'font_size': 12})
basic_format.set_align('left')

# Set the width of all columns to 15
for col_num, col_name in enumerate(merged_df.columns, 1):
    worksheet.set_column(col_num, col_num, 30)

# Write the header of the DataFrame into the Excel-file
for col_num, value in enumerate(merged_df.columns.values):
    worksheet.write(0, col_num, value, header_format)

# Write the data from the DataFrame into the rest of the Excel-file
for row_num, row in enumerate(merged_df.values):
    for col_num, cell in enumerate(row):
        worksheet.write(row_num + 1, col_num, cell, basic_format)

# Save workbook
workbook.close()

# Save the DataFrame as an Excel-file
excel_buffer_2.seek(0)

# Verstuur een e-mail met het Excel-bestand als bijlage
def send_email_with_attachment(excel_buffer_1, excel_buffer_2, recipient_mail):
    # Configure the email
    smtp_server = os.getenv("MAIL_SMTP_SERVER")
    smtp_port = os.getenv("MAIL_SMTP_PORT")
    sender_email = os.getenv("MAIL_SENDER_EMAIL")
    sender_password = os.getenv("MAIL_SENDER_PASSWORD")
    recipient_email = recipient_mail

    # Define the subject
    email_subject = f"""
    Maand overzicht {last_month} {year_of_last_month} van verkeerd verpakte orders Aard'g
    """

    # Define the text
    email_text = f"""
    Hi Henk,

    Zie bijgevoegd het maand overzicht van {last_month} {year_of_last_month} van de verkeerd verpakte orders.
    """

    # Add the section about wrong orders if there are files in the bucket
    if file_names:
        email_text += f"En tevens het overzicht van verkeerd verzonden producten van week {lowest_date_str} tot en met {highest_date_str}."

    email_text += """
    Groet,
    Max
    """

    # Bericht samenstellen
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = recipient_email
    message['Subject'] = email_subject
    message.attach(MIMEText(email_text, 'plain'))

    # Add the first Excel-file as attachment
    part_1 = MIMEBase('application', 'octet-stream')
    part_1.set_payload(excel_buffer_1.read())
    encoders.encode_base64(part_1)
    part_1.add_header('Content-Disposition', f'attachment; filename={year_of_last_month}_{last_month_number}.xlsx')
    message.attach(part_1)

    # Add the second Excel-file as attachment if there are files in the bucket
    if file_names:
        part_2 = MIMEBase('application', 'octet-stream')
        part_2.set_payload(excel_buffer_2.read())
        encoders.encode_base64(part_2)
        part_2.add_header('Content-Disposition', f'attachment; filename={new_file_name}')
        message.attach(part_2)

    # Verbinding maken met de SMTP-server en e-mail verzenden
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient_email, message.as_string())

    print('E-mail met bijlage verzonden')

# Send the email with the Excel-file as attachment
send_email_with_attachment(excel_buffer_1, excel_buffer_2, recipient_mail)