import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq as pd_gbq
from tabulate import tabulate

# Import keys.env
load_dotenv()

# Define today and yesterday
today = datetime.now().date()
yesterday = today - timedelta(days=1)

# Get the GCP keys
gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

credentials = service_account.Credentials.from_service_account_file(gc_keys)
project_id = credentials.project_id

# Define variables
api_url: str = os.getenv("MONTA_API_URL", "")
username: str = os.getenv("MONTA_USERNAME", "")
password: str = os.getenv("MONTA_PASSWORD", "")
dataset_id = os.environ.get('DATASET_ID')
table_id = os.environ.get('INBOUND_TABLE_ID')

if api_url is None or username is None or password is None:
    raise ValueError("API URL, username, or password environment variables are not set.")

def retrieve_inbound(id):
    endpoint = f'inbounds?sinceid={id}'
    full_url = api_url + endpoint
    response = requests.get(full_url, auth=HTTPBasicAuth(username, password))
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve inbound information: {response.status_code} - {response.text}")
        return None

# ID Query
id_query = f"""
SELECT MAX(Id)
FROM `{project_id}.{dataset_id}.{table_id}`
"""

# Execute the ID query
df = pd_gbq.read_gbq(id_query, project_id=project_id, credentials=credentials)

if df.empty:
    last_id = 1
else:
    last_id = df['f0_'].iloc[0]

# Retrieve inbound data
inbound_data = retrieve_inbound(last_id)
print(inbound_data)


# Maak een lege lijst om de opgeschoonde data in op te slaan
cleaned_data = []

# Itereer over de inbound_data om de relevante data op te schonen
for entry in inbound_data:
    cleaned_entry = {
        "Id": entry["Id"],
        "Sku": entry["Sku"],
        "Aantal": entry["Quantity"],
        "Created": entry["Created"],
        "Type": entry["Type"],
        "Batch": entry["Batch"]["Reference"],
        "Quarantaine": entry["Quarantaine"]
    }
    cleaned_data.append(cleaned_entry)

# Maak een dataframe van de cleaned_data
df = pd.DataFrame(cleaned_data)

# Create the product column
sku_to_product_name = {
    '8719326399355': 'Citroen',
    '8719326399362': 'Bloem',
    '8719326399379': 'Gember',
    '8719326399386': 'Kombucha',
    '8719326399393': 'Waterkefir',
    '8719327215111': 'Starter Box',
    '8719327215128': 'Frisdrank Mix',
    '8719327215135': 'Mix Originals',
    '8719327215159': 'Kalender',
    '8719327215180': 'Probiotica',
    '265220495mm': 'Verzenddoos 2',
    '634266242mm': 'Verzenddoos 3'
}

df['Product'] = df['Sku'].map(sku_to_product_name)

# Custom parsing function
def parse_datetime(dt_str):
    for fmt in ('%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S'):
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            pass
    return pd.NaT

# Create Date column
df['Created'] = df['Created'].apply(parse_datetime)
df['Datum'] = df['Created'].dt.date

# Select the relevant columns
relevant_columns = ['Id', 'Product', 'Aantal', 'Datum', 'Type', 'Batch', 'Quarantaine']
df = df[relevant_columns]

# Limit to the necessary dates
df = df[df['Datum'] == today]

# Tabulate the data
table = tabulate(df, headers='keys', tablefmt='psql')

try:
# Voer de query uit en laad de resultaten in een DataFrame
    pd_gbq.to_gbq(df, destination_table=f'{project_id}.{dataset_id}.{table_id}', project_id=f'{project_id}', credentials=credentials, if_exists='append')
    print("Upload naar BigQuery succesvol voltooid!")
except Exception as e:
    error_message = f"Fout bij het uploaden naar BigQuery: {str(e)}"
    print(error_message)