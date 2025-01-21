from client_connect import Connection
connection = Connection()
client = connection.client
# The data as provided
data = {
    "Personal_Identification": [
        "AADHAAR", "PASSPORT", "DRIVERLICENSE", "VOTERID", "SSN", "UID", "PAN", "VID", "RATION_CARD_NUMBER"
    ],
    "Contact_Information": [
        "ADDRESS", "EMAIL_ADDRESS", "PHONE_NUMBER", "POBOX", "ZIPCODE", "LOCATION"
    ],
    "Biometric Data": [
        "BIOMETRIC", "IMEI", "IMSI"
    ],
    "Financial_Information": [
        "BANK_ACCOUNT_NUMBER", "BANK_CARD", "CREDIT_CARD", "AMEX_CARD", "MAESTRO_CARD", "RUPAY_CARD", 
        "VISA_CARD", "MASTER_CARD", "CVV", "PAYMENTCARD", "UPI_ID", "GST_NUMBER", "IFSC"
    ],
    "System_Data": [
        "IP_ADDRESS", "MAC_ADDRESS", "DATE_TIME", "USERNAME", "USER_NAME", "PASSWORD"
    ],
    "Demographic_Information": [
        "GENDER", "NATIONALITY", "RELIGION", "POLITICAL_OPINION", "SEXUAL_ORIENTATION", "TITLE", "PERSON"
    ],
    "Health": [
        "MEDICAL_LICENSE"
    ],
    "Vehicle_Information": [
        "VEHICLE_IDENTIFICATION_NUMBER"
    ]
}
id_counter = 1
data_uppercase = {key.upper(): [value.upper() for value in values] for key, values in data.items()}
print(data_uppercase)
# Loop through the categories and insert into ClickHouse
for category, values in data_uppercase.items():
    # Prepare the values in the required format
    parameter_name = category
    parameter_value = values
    
    # Insert into ClickHouse
    insert_query = f"""
    INSERT INTO data_element (id, parameter_name, parameter_value)
    VALUES ({id_counter}, '{parameter_name}', {parameter_value})
    """
    client.command(insert_query)
    print(f"Inserted {parameter_name} with ID {id_counter}")

    # Increment the ID counter
    id_counter += 1

print("Data insertion complete.")

