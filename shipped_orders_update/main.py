from dotenv import load_dotenv
import os
from modules.get_request import shipment_label, order_ids
from modules.labels import count_package_types, calculate_shipping_labels, determine_actual_shipments, product_catalogue
from modules.send_mail import send_email

def main():

    load_dotenv()

    # Variabelen definiëren
    base_url = os.getenv("BASE_URL")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    smtp_server = os.getenv("MAIL_SMTP_SERVER")
    smtp_port = os.getenv("MAIL_SMTP_PORT")
    sender_email = os.getenv("MAIL_SENDER_EMAIL")
    sender_password = os.getenv("MAIL_SENDER_PASSWORD")
    recipient_email = os.getenv("MAIL_RECIPIENT_EMAIL")

    # Orders vorige week ophalen
    order_list = order_ids(base_url, username, password)

    # Message vaststellen om de vals negatief en positief aan toe te voegen
    message = ""

    # Shipmentlabel data ophalen
    for order in order_list:
        response_data = shipment_label(base_url, username, password, order)

        # Pakkettype bepalen
        package_count, mailbox_count = count_package_types(response_data, product_catalogue)

        # Labels berekenen
        shipping_labels = calculate_shipping_labels(package_count, mailbox_count)
        print(shipping_labels)
        # Aantal labels bepalen
        actual_labels = determine_actual_shipments(response_data)

        # Afwijking vaststellen
        if actual_labels > shipping_labels:
            message += f"Vals negatief: order {order} heeft {actual_labels} verzendlabels, maar {shipping_labels} nodig.\n"
        elif actual_labels < shipping_labels:
            message += f"Vals positief: order {order} heeft {actual_labels} verzendlabels, maar {shipping_labels} nodig.\n"
        else: 
            continue

    # Mail verzenden
    if message:
        send_email(message, recipient_email, smtp_server, smtp_port, sender_email, sender_password)

if __name__ == "__main__":
    main()

