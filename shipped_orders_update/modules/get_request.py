import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from urllib.parse import quote

def shipment_label(base_url, username, password, order_id):
    # Order ID URL encoden
    encoded_order_id = quote(order_id, safe='')
    
    # Ednpoint definieren
    endpoint = f"order/{encoded_order_id}/colli"

    # Header definieren
    headers = {
        "Content-Type": "application/json",
        'Accept': 'application/json'
    }

    try:
        # Request definieren
        response = requests.get(base_url + endpoint, auth=HTTPBasicAuth(username, password), headers=headers)
        if response.status_code == 200:
            response_data = response.json()
            return response_data
        else:
            print(f"Failed to fetch data for order {order_id}. Status code: {response.status_code}")
    except Exception as e:
        print(f"Fout bij het verwerken van order {order_id}: {str(e)}")

def order_ids(base_url, username, password):

    # Endpoint definieren
    created_since = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    created_until = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    page = 1
    order_ids = []

    while True:
        # Endpoint definiëren
        endpoint = f"orders?created_since={created_since}&created_until={created_until}&page={page}"
        full_url = base_url + endpoint

        # Header definieren
        headers = {
            "Content-Type": "application/json",
            'Accept': 'application/json'
        }

        try:
            # Request definieren
            request = requests.get(full_url, auth=HTTPBasicAuth(username, password), headers=headers)

            # Checken of het verzoek succesvol was
            if request.status_code == 200:
                response_json = request.json()
            
                # Als er geen data meer is, breek de loop
                if not response_json or len(response_json) == 0:
                    break
                    
                # Order ID extraheren
                for order in response_json:
                    order_ids.append(order['WebshopOrderId'])

                # Ga naar de volgende pagina
                page += 1
            else:
                print(f"Request mislukt met statuscode {request.status_code}")
                print(request.text)
                break
        except requests.exceptions.RequestException as e:
            print(f"Er is een fout opgetreden: {e}")
            break

    return order_ids