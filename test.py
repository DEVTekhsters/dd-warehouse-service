import clickhouse_connect

# Establish connection to ClickHouse
client = clickhouse_connect.get_client(
    host='148.113.6.50',
    port="8123",
    username='default',
    password='',
    database='default'
)

# # Query to add a new column to the table
# alter_query = "ALTER TABLE column_ner_results ADD COLUMN data_element String after json;"

# try:
#     client.command(alter_query)
#     print("Column 'detected_entity' added successfully.")
# except Exception as e:
#     print(f"An error occurred: {e}")