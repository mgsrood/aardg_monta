product_catalogue = {
    "Product Gember Limonade 12x 250ml": "Pakket",
    "Product Kombucha Original 4x 1L": "Pakket",
    "Product Frisdrank Mix 12x 250ml": "Pakket",
    "Product Bloem Kombucha 12x 250ml": "Pakket",
    "Product Probiotica Ampullen 28x 9ml": "Brievenbus",
    "Product Citroen Kombucha 12x 250ml": "Pakket",
    "Product Waterkefir Original 4X 1L": "Pakket",
    "Product Mix Originals 4x 1L": "Pakket",
    "Product Starter Box": "Pakket"
}

def count_package_types(response_data, product_catalogue):
    package_count = 0
    mailbox_count = 0

    for box in response_data["ShippedBoxesNotOnPallets"]:
        for product in box["ShippedProduct"]:
            product_desc = product["Description"]
            quantity = product["Quantity"]
            if product_desc in product_catalogue:
                package_type = product_catalogue[product_desc]
                if package_type == "Pakket":
                    package_count += quantity
                elif package_type == "Brievenbus":
                    mailbox_count += quantity

    return package_count, mailbox_count

def calculate_shipping_labels(package_count, mailbox_count):
    labels = 0

    # Gebruik eerst de 3 omdoos
    while package_count >= 3:
        labels += 1
        package_count -= 3
        mailbox_count = 0

    # Gebruik dan de 2 omdoos
    while package_count >= 2:
        labels += 1
        package_count -= 2
        mailbox_count = 0

    # Verwerk de overige dozen en, als 
    if package_count > 0:
        labels += 1
        package_count = 0
        mailbox_count = 0  # All mailbox packages fit into the package box

    # Use the 2-mailbox boxes for remaining mailbox packages
    while mailbox_count >= 2:
        labels += 1
        mailbox_count -= 2

    # Handle remaining mailbox packages
    if mailbox_count > 0:
        labels += 1

    return labels

def determine_actual_shipments(response_data):
    actual_labels = 0
    for labels in response_data["ShippedBoxesNotOnPallets"]:
        actual_labels += 1
    
    return actual_labels

