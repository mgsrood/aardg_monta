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
table_id = os.environ.get('MONTA_VERKOOP_BATCH_TABLE_ID')

# Load excel
batch_data = pd.read_excel("report.xlsx")

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
batch_data['Product'] = batch_data.apply(determine_base_product, axis=1)

# Desired columns
desired_columns = [
    "Omschrijving",
    "Product",
    "Aantal",
    "Batch"
]

# Select only the desired columns
batch_data = batch_data[desired_columns]

# Rename "Batch" column to "Lotnummer"
desired_column_name = {
    'Batch': 'Lotnummer'
}

batch_data = batch_data.rename(columns=desired_column_name)

# Sum the values in the "Aantal" column, groupby "Batch" and "Product"
batch_totals = batch_data.groupby(["Lotnummer", "Product"]).sum()

if __name__ == "__main__":
    try:
    # Voer de query uit en laad de resultaten in een DataFrame
        df_gbq = pd_gbq.to_gbq(batch_totals, destination_table=f'{project_id}.{dataset_id}.{table_id}', project_id=f'{project_id}', credentials=credentials, if_exists='replace')
        print("Upload naar BigQuery succesvol voltooid!")
    except Exception as e:
        error_message = f"Fout bij het uploaden naar BigQuery: {str(e)}"