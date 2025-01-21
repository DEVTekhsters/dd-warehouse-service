from faker import Faker
import pandas as pd
import random
import mysql.connector

# Initialize Faker
faker = Faker("en_IN")

# Define number of records
records_per_table = 1000
combined_records = 500

# Personal Identification Data
personal_data = [
    {
        "AADHAAR": faker.ssn(),
        "PASSPORT": faker.bothify("???######"),
        "DRIVERLICENSE": faker.bothify("DL-###########"),
        "VOTERID": faker.bothify("VOTERID-####"),
        "SSN": faker.ssn(),
        "UID": faker.uuid4(),
        "PAN": faker.bothify("?????####?"),
        "VID": faker.bothify("VID#############"),
        "RATION_CARD_NUMBER": faker.bothify("RCN#######"),
    }
    for _ in range(records_per_table)
]

# Contact Information Data
contact_data = [
    {
        "ADDRESS": faker.address().replace("\n", ", "),
        "EMAIL_ADDRESS": faker.email(),
        "PHONE_NUMBER": faker.phone_number(),
        "POBOX": faker.bothify("PO###"),
        "ZIPCODE": faker.postcode(),
        "LOCATION": f"{faker.latitude()}, {faker.longitude()}",
    }
    for _ in range(records_per_table)
]

# Financial Information Data
financial_data = [
    {
        "BANK_ACCOUNT_NUMBER": faker.bban(),
        "BANK_CARD": faker.credit_card_number(),
        "CREDIT_CARD": faker.credit_card_number(card_type="visa"),
        "CVV": faker.credit_card_security_code(),
        "UPI_ID": faker.bothify(f"{faker.first_name().lower()}##@upi"),
        "GST_NUMBER": faker.bothify("##?????#####?1Z#"),
        "IFSC": faker.bothify("????000#####"),
    }
    for _ in range(records_per_table)
]

# System Data
system_data = [
    {
        "IP_ADDRESS": faker.ipv4_private(),
        "MAC_ADDRESS": faker.mac_address(),
        "DATE_TIME": faker.date_time_this_year(),
        "USERNAME": faker.user_name(),
        "PASSWORD": faker.password(),
    }
    for _ in range(records_per_table)
]

# Combined Data (Sample from all tables)
combined_data = [
    {**random.choice(personal_data), **random.choice(contact_data), 
     **random.choice(financial_data), **random.choice(system_data)}
    for _ in range(combined_records)
]

# Convert to DataFrames
personal_df = pd.DataFrame(personal_data)
contact_df = pd.DataFrame(contact_data)
financial_df = pd.DataFrame(financial_data)
system_df = pd.DataFrame(system_data)
combined_df = pd.DataFrame(combined_data)

# MySQL Database Configuration
db_config = {
    "host": "148.113.5.60",
    "port": 3309,
    "user": "root",  # Replace with your MySQL username
    "password": "root_password",  # Replace with your MySQL password
    "database": "pii_data_test",  # Database name
}

# Upload Data to MySQL
def upload_to_mysql(dataframe, table_name, db_config):
    connection = None
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        # Create Table
        cols = ", ".join([f"{col} TEXT" for col in dataframe.columns])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols});")
        # Insert Data
        for _, row in dataframe.iterrows():
            values = "', '".join([str(x).replace("'", "''") for x in row])
            cursor.execute(f"INSERT INTO {table_name} VALUES ('{values}');")
        connection.commit()
        print(f"Data uploaded to table {table_name} successfully.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Save Data to MySQL
upload_to_mysql(personal_df, "personal_pii", db_config)
upload_to_mysql(contact_df, "contact_info", db_config)
upload_to_mysql(financial_df, "financial_info", db_config)
upload_to_mysql(system_df, "system_data", db_config)
upload_to_mysql(combined_df, "combined_pii", db_config)
