from dotenv import load_dotenv
import os
from google.oauth2 import service_account
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd 
import pandas_gbq as pd_gbq

# Load environment variables
load_dotenv()

# Get the GCP keys
gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

credentials = service_account.Credentials.from_service_account_file(gc_keys)
project_id = credentials.project_id

# Define variables
api_url = os.getenv("MONTA_API_URL")
username = os.getenv("MONTA_USERNAME")
password = os.getenv("MONTA_PASSWORD")
dataset_id = os.environ.get('STOCK_DATASET_ID')
table_id = os.environ.get('STOCK_TABLE_ID')

# Create timestamp
today = datetime.now()

# Create the function to retrieve stock numbers
def stock_batch(sku):
    endpoint = f"product/{sku}/stock"
    url = api_url + endpoint
    
    response = requests.get(url, auth=HTTPBasicAuth(username, password))
    if response.status_code == 200:
        response_data = response.json()

    # Retrieve SKU, Description, Batch Reference and Quantity
    sku_list = [response_data['Sku']] * len(response_data['Stock']['Batches'])
    desc_list = [response_data['Description']] * len(response_data['Stock']['Batches'])
    reference_list = [batch['Reference'] for batch in response_data['Stock']['Batches']]
    quantity_list = [batch['Quantity'] for batch in response_data['Stock']['Batches']]
    timestamp = today

    # Turn the response into a Pandas DataFrame
    df = pd.DataFrame({
    'SKU': sku_list,
    'Description': desc_list,
    'Batch': reference_list,
    'Quantity': quantity_list,
    'Timestamp': timestamp
})
    return df

def stock_non_batch(sku):
    endpoint = f"product/{sku}/stock"
    url = api_url + endpoint
    
    response = requests.get(url, auth=HTTPBasicAuth(username, password))
    if response.status_code == 200:
        response_data = response.json()

    # Retrieve SKU, Description, Batch Reference and Quantity
    sku_list = [response_data['Sku']]
    desc_list = [response_data['Description']]
    reference_list = [None]
    quantity_list = response_data['Stock']['StockAll']
    timestamp = today

    # Turn the response into a Pandas DataFrame
    df = pd.DataFrame({
    'SKU': sku_list,
    'Description': desc_list,
    'Batch': reference_list,
    'Quantity': quantity_list,
    'Timestamp': timestamp
})
    return df

if __name__ == "__main__":

    stock_df = pd.DataFrame(columns=['SKU', 'Description', 'Batch', 'Quantity'])

    batch_skus = {
        '8719326399355': 'Citroen Kombucha 12x 250ml',
        '8719326399362': 'Bloem Kombucha 12x 250ml',
        '8719326399379': 'Gember Limonade 12x 250ml',
        '8719326399386': 'Kombucha Original 4x 1L',
        '8719326399393': 'Waterkefir Original 4x 1L',
        '8719327215111': 'Starter Box',
        '8719327215128': 'Frisdrank Mix 12x 250ml',
        '8719327215135': 'Mix Originals 4x 1L',
        '8719327215180': 'Probiotica Ampullen 28x 9ml',
    }       

    for sku in batch_skus.keys():
        df = stock_batch(sku)
        stock_df = stock_df._append(df, ignore_index=True)

    non_batch_skus = {
        '8719327215159': 'EAN 30 dagen challenge kalender',
        '265220495mm': 'Verzenddoos tbv 2 colli',
        '634266242mm': 'Verzenddoos tbv 3 colli'
    }

    for sku in non_batch_skus.keys():
        df = stock_non_batch(sku)
        stock_df = stock_df._append(df, ignore_index=True)

    try:
    # Voer de query uit en laad de resultaten in een DataFrame
        pd_gbq.to_gbq(stock_df, destination_table=f'{project_id}.{dataset_id}.{table_id}', project_id=f'{project_id}', credentials=credentials, if_exists='append')
        print("Upload naar BigQuery succesvol voltooid!")
    except Exception as e:
        error_message = f"Fout bij het uploaden naar BigQuery: {str(e)}"
        print(error_message)