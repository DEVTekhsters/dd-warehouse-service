import os
import clickhouse_connect
from dotenv import load_dotenv



load_dotenv()
# Connect to ClickHouse
client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST'),
        port=os.getenv('CLICKHOUSE_PORT'),
        username=os.getenv('CLICKHOUSE_USERNAME'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        database=os.getenv('CLICKHOUSE_DATABASE')
    )
# -------------------------------------------------------------------------------------
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
# -------------------------------------------------------------------------------------

# Directory containing migration files
migration_dir = './migrations'
migrations = sorted(os.listdir(migration_dir))

# Create a migration log table to keep track of applied migrations
client.command("""
CREATE TABLE IF NOT EXISTS migration_log (
    id String,
    applied_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;
""")

# Fetch already applied migrations
applied_migrations = set(row[0] for row in client.query("SELECT id FROM migration_log").result_rows)

# Apply new migrations
for migration in migrations:
    migration_id = migration.split('_')[0]  # Extract ID from filename
    if migration_id not in applied_migrations:
        with open(os.path.join(migration_dir, migration), 'r') as f:
            sql = f.read()
            client.command(sql)
            client.command("INSERT INTO migration_log (id) VALUES (%s)", (migration_id,))
            print(f"Applied migration {migration_id}")

print("All migrations applied.")

# Add data elements to the table `data_element`
id_counter = 1
data_uppercase = {key.upper(): [value.upper() for value in values] for key, values in data.items()}

for category, values in data_uppercase.items():
    parameter_name = category
    parameter_value = values  # Keep it as a Python list

    # Insert into ClickHouse
    insert_query = f"""
    INSERT INTO data_element (id, parameter_name, parameter_value)
    VALUES ({id_counter}, '{parameter_name}', {parameter_value})
    """
    client.command(insert_query)
    print(f"Inserted {parameter_name} with ID {id_counter}")

    id_counter += 1


print("Data insertion complete.")
# Apply views from structured and unstructured 

# Directories containing view definitions
structured_views_dir = './views/structured'
unstructured_views_dir = './views/unstructured'

# Apply views in structured folder
structured_views = [f for f in os.listdir(structured_views_dir) if f.endswith('.sql')]
for view in structured_views:
    with open(os.path.join(structured_views_dir, view), 'r') as f:
        sql = f.read()
        client.command(sql)
        print(f"Applied structured view {view}")

# Apply views in unstructured folder
unstructured_views = [f for f in os.listdir(unstructured_views_dir) if f.endswith('.sql')]
for view in unstructured_views:
    with open(os.path.join(unstructured_views_dir, view), 'r') as f:
        sql = f.read()
        client.command(sql)
        print(f"Applied unstructured view {view}")