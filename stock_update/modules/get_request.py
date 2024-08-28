import requests
from requests.auth import HTTPBasicAuth
from modules.dictionaries import product_ids, box_values, unit_values

def get_request(api_url, username, password):
    # GET Request loop
    for name, product_id in product_ids.items():
        # Endpoint
        url = f"{api_url}product/{product_id}/stock"

        # GET Request
        response = requests.get(url, auth=HTTPBasicAuth(username, password))

        # Retrieve stock value
        data = response.json()
        stock_value = data['Stock']['StockAll']

        # Update box value dictionary
        box_values[name] += stock_value

        # Append to stock value dictionary
        if name in ["citroen", "bloem", "gember"]:
            # Add stock value to dictionary
            unit_values[name] += stock_value * 12
        elif name == "starter":
            # Add stock value to dictionary
            unit_values["citroen"] += stock_value
            unit_values["bloem"] += stock_value
            unit_values["gember"] += stock_value
            unit_values["kombucha"] += stock_value
            unit_values["waterkefir"] += stock_value
            unit_values["probiotica"] += stock_value
        elif name in ["kombucha", "waterkefir"]:
            # Add stock value to dictionary
            unit_values[name] += stock_value * 4
        elif name == "frisdrank_mix":
            # Add stock value to dictionary
            unit_values["citroen"] += stock_value * 4
            unit_values["bloem"] += stock_value * 4
            unit_values["gember"] += stock_value * 4
        elif name == "mix_originals":
            # Add stock value to dictionary
            unit_values["kombucha"] += stock_value * 2
            unit_values["waterkefir"] += stock_value * 2
        elif name == "probiotica":
            unit_values["probiotica"] += stock_value * 28

    # Divide probiotica value by 28 for unit values
    unit_values["probiotica"] /= 28

    return box_values, unit_values