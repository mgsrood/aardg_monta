from dotenv import load_dotenv
import os
from google.oauth2 import service_account
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd 
import pandas_gbq as pd_gbq
import json

# Load environment variables
load_dotenv()

# Each description for a given SKU
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
    '8719327215159': 'EAN 30 dagen challenge kalender',
    '265220495mm': 'Verzenddoos tbv 2 colli',
    '634266242mm': 'Verzenddoos tbv 3 colli'
    }     

# Get the GCP keys
gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

credentials = service_account.Credentials.from_service_account_file(gc_keys)
project_id = credentials.project_id

# Define variables
api_url = os.getenv("MONTA_API_URL")
username = os.getenv("MONTA_USERNAME")
password = os.getenv("MONTA_PASSWORD")
dataset_id = os.environ.get('DATASET_ID')
table_id = os.environ.get('ORDER_TABLE_ID')

def fetch_orders(created_since, api_url):
    page = 1
    orders = []
    
    # Fetch orders from Monta API
    while True:
        endpoint = f"orders?created_since={created_since}&page={page}"
        url = api_url + endpoint
        
        response = requests.get(url, auth=HTTPBasicAuth(username, password))
        if response.status_code == 200:
            response_data = response.json()
            if not response_data:
                break  # Break the loop if response_data is empty
            orders.extend(response_data)
            page += 1
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}. {response.text}")
            break
    
    return orders

def process_orders(orders):
    # Initialize empty lists for each order attribute
    order_id_list = []
    shipped_date_list = []
    first_name_list = []
    last_name_list = []
    street_list = []
    house_number_list = []
    zipcode_list = []
    city_list = []
    country_list = []

    # Iterate over orders and extract relevant information
    for item in orders:
        if 'ShippedDate' in item and item['ShippedDate'] is not None:
            order_id_list.append(item['WebshopOrderId'])
            shipped_date = datetime.strptime(item['ShippedDate'][:10], '%Y-%m-%d')
            shipped_date_formatted = shipped_date.strftime('%Y-%m-%d')
            shipped_date_list.append(shipped_date_formatted)
            first_name_list.append(item['ConsumerDetails']['ShippingContactDetails']['FirstName'])
            last_name_list.append(item['ConsumerDetails']['ShippingContactDetails']['LastName'])
            street_list.append(item['ConsumerDetails']['ShippingContactDetails']['Street'])
            house_number_list.append(item['ConsumerDetails']['ShippingContactDetails']['HouseNo'])
            zipcode_list.append(item['ConsumerDetails']['ShippingContactDetails']['PostalCode'])
            city_list.append(item['ConsumerDetails']['ShippingContactDetails']['City'])
            country_list.append(item['ConsumerDetails']['ShippingContactDetails']['Country'])

    # Create a DataFrame from the extracted information
    df = pd.DataFrame({
        'Order_ID': order_id_list,
        'Shipped_Date': shipped_date_list,
        'First_Name': first_name_list,
        'Last_Name': last_name_list,
        'Street': street_list,
        'House_Number': house_number_list,
        'Zipcode': zipcode_list,
        'City': city_list,
        'Country': country_list
    })

    return df

# Get order details
def get_order_details(order_ids):
    all_orders_df = pd.DataFrame()

    for order_id in order_ids:
        endpoint = f"order/{order_id}/batches"
        url = api_url + endpoint

        response = requests.get(url, auth=HTTPBasicAuth(username, password))

        if response.status_code == 200:
            response_data = response.json()
            
            # Extract product line details
            order_id_list = []
            sku_list = []
            quantity_list = []
            batch_list = []

            # Iterate over product lines
            for item in response_data['BatchLines']:
                if item['BatchContent'] is not None:
                    order_id_list.append(order_id)
                    sku_list.append(item['Sku'])
                    # Turn quantity into an absolute value
                    if item['Quantity'] < 0:
                        item['Quantity'] = abs(item['Quantity'])
                    quantity_list.append(item['Quantity'])
                    batch_list.append(item['BatchContent']['Title'])
                else:
                    continue

            # Create a DataFrame from the extracted information for the current order_id
            order_df = pd.DataFrame({
                'Order_ID': order_id_list,
                'SKU': sku_list,
                'Quantity': quantity_list,
                'Batch_ID': batch_list
            })

            # Append current order_df to all_orders_df
            all_orders_df = pd.concat([all_orders_df, order_df], ignore_index=True)
        
    return all_orders_df

# Function to extract Product from Omschrijving
def determine_base_product(row):
        omschrijving = row['Omschrijving']

        if pd.isna(omschrijving):
            return 'unknown'

        omschrijving = row['Omschrijving'].lower()

        if 'kombucha' in omschrijving and ('citroen' not in omschrijving and 'bloem' not in omschrijving):
            return 'Kombucha'
        elif 'citroen' in omschrijving:
            return 'Citroen'
        elif 'bloem' in omschrijving:
            return 'Bloem'
        elif 'waterkefir' in omschrijving:
            return 'Waterkefir'
        elif 'frisdrank' in omschrijving:
            return 'Frisdrank Mix'
        elif 'mix' in omschrijving and 'frisdrank' not in omschrijving:
            return 'Mix Originals'
        elif 'starter' in omschrijving or 'introductie' in omschrijving:
            return 'Starter Box'
        elif 'gember' in omschrijving:
            return 'Gember'
        elif 'probiotica' in omschrijving:
            return 'Probiotica'
        elif 'kalender' in omschrijving:
            return 'Kalender'
        elif '2 colli' in omschrijving:
            return 'Verzenddoos 2'
        elif '3 colli' in omschrijving:
            return 'Verzenddoos 3'
        else:
            return 'unknown'

if __name__ == "__main__":
    # Create dates
    today = datetime.now()
    seven_days_ago = today - timedelta(days=7)
    six_days_ago = today - timedelta(days=6)
    five_days_ago = today - timedelta(days=5)
    four_days_ago = today - timedelta(days=4)
    three_days_ago = today - timedelta(days=3)
    two_days_ago = today - timedelta(days=2)
    yesterday = today - timedelta(days=1)

    # Fetch orders for the last 7 days
    order_list = fetch_orders(four_days_ago, api_url)

    # Process orders to create a DataFrame
    df = process_orders(order_list)

    # Limit to orders shipped yesterday
    order_df = df[df['Shipped_Date'] == yesterday.strftime('%Y-%m-%d')]

    # Turn Order_ID column into a list
    order_ids = order_df['Order_ID'].tolist()

    # Continue if order list is not empty
    if order_ids:
        # Return order details
        batch_df = get_order_details(order_ids)

        # Merge order_df into the batch_df
        merged_df = pd.merge(batch_df, order_df, on='Order_ID') 

        # Create a description column
        merged_df['Omschrijving'] = merged_df['SKU'].apply(lambda x: batch_skus[x])

        # Determine base product
        merged_df['Product'] = merged_df.apply(determine_base_product, axis=1)

        # Rename columns
        merged_df = merged_df.rename(columns={
            'Quantity': 'Aantal',
            'Batch_ID': 'Lotnummer',
            'Shipped_Date': 'Datum',
            'First_Name': 'Voornaam',
            'Last_Name': 'Achternaam',
            'Street': 'Straat',
            'Zipcode': 'Postcode',
            'City': 'Plaats',
            'Country': 'Land',
            
        })

        # Select relevant columns
        relevant_columns = [
            'Order_ID',
            'Product',
            'Aantal',
            'Lotnummer',
            'Datum',
            'Voornaam',
            'Achternaam',
            'Straat',
            'Postcode',
            'Plaats',
            'Land'
        ]

        # Select only the desired columns
        df = merged_df[relevant_columns]

        # Length of df
        length = len(df)

        # Write to BigQuery using pandas_gbq
        try:
        # Voer de query uit en laad de resultaten in een DataFrame
            pd_gbq.to_gbq(df, destination_table=f'{project_id}.{dataset_id}.{table_id}', project_id=f'{project_id}', credentials=credentials, if_exists='append')
            print("Upload naar BigQuery succesvol voltooid! Rijen toegevoegd: " + str(length))
        except Exception as e:
            error_message = f"Fout bij het uploaden naar BigQuery: {str(e)}"
            print(error_message)

    else:
        print("No orders found for yesterday.")
    
    
