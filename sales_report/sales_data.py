import pandas as pd
import pandas_gbq as pd_gbq
from dotenv import load_dotenv
import os
from google.oauth2 import service_account

# Import .env
load_dotenv()

# Load GCP credentials
gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

# Extract credentials and project_id
credentials = service_account.Credentials.from_service_account_file(gc_keys)
project_id = credentials.project_id

# Load BigQuery variables
dataset_id = os.environ.get('MASSABALANS_DATASET_ID')
table_id = os.environ.get('MONTA_VERKOOP_TABLE_ID')

# Load excel
sales_data = pd.read_excel("report.xlsx")

# Function to extract Product from Omschrijving
def determine_base_product(row):
        omschrijving = row['Omschrijving']

        if omschrijving is None:
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
        else:
            return 'unknown'

# Apply function to Omschrijving column
sales_data['Product'] = sales_data.apply(determine_base_product, axis=1)

# Desired columns
desired_columns = [
    "OrderNummer",
    "Verzenddatum",
    "Voornaam",
    "Achternaam",
    "Email",
    "Straat",
    "Postcode",
    "Plaats",
    "Land",
    "Product",
    "Aantal",
    "Batch",
    "Orderstatus"
]

# Select only the desired columns
sales_data = sales_data[desired_columns]

# Rename "Batch" column to "Lotnummer"
desired_column_name = {
    "OrderNummer": "Order_ID",
    'Batch': 'Lotnummer'
}

sales_data = sales_data.rename(columns=desired_column_name)

if __name__ == "__main__":
    try:
    # Voer de query uit en laad de resultaten in een DataFrame
        df_gbq = pd_gbq.to_gbq(sales_data, destination_table=f'{project_id}.{dataset_id}.{table_id}', project_id=f'{project_id}', credentials=credentials, if_exists='replace')
        print("Upload naar BigQuery succesvol voltooid!")
    except Exception as e:
        error_message = f"Fout bij het uploaden naar BigQuery: {str(e)}"