

# # client.command('CREATE TABLE data_element (id Int32, parameter_name String, parameter_value Array(String)) ENGINE = MergeTree() ORDER BY id')
# # result = client.query('SELECT max(key), avg(metric) FROM new_table')
# # print(result.result_rows)


# from client_connect import Connection
# connection = Connection()
# client = connection.client
# # The data as provided
# data = {
#     "Personal_Identification": [
#         "AADHAAR", "PASSPORT", "DRIVERLICENSE", "VOTERID", "SSN", "UID", "PAN", "VID", "RATION_CARD_NUMBER"
#     ],
#     "Contact_Information": [
#         "ADDRESS", "EMAIL_ADDRESS", "PHONE_NUMBER", "POBOX", "ZIPCODE", "LOCATION"
#     ],
#     "Biometric Data": [
#         "BIOMETRIC", "IMEI", "IMSI"
#     ],
#     "Financial_Information": [
#         "BANK_ACCOUNT_NUMBER", "BANK_CARD", "CREDIT_CARD", "AMEX_CARD", "MAESTRO_CARD", "RUPAY_CARD", 
#         "VISA_CARD", "MASTER_CARD", "CVV", "PAYMENTCARD", "UPI_ID", "GST_NUMBER", "IFSC"
#     ],
#     "System_Data": [
#         "IP_ADDRESS", "MAC_ADDRESS", "DATE_TIME", "USERNAME", "USER_NAME", "PASSWORD"
#     ],
#     "Demographic_Information": [
#         "GENDER", "NATIONALITY", "RELIGION", "POLITICAL_OPINION", "SEXUAL_ORIENTATION", "TITLE", "PERSON"
#     ],
#     "Health": [
#         "MEDICAL_LICENSE"
#     ],
#     "Vehicle_Information": [
#         "VEHICLE_IDENTIFICATION_NUMBER"
#     ]
# }
# id_counter = 1
# data_uppercase = {key.upper(): [value.upper() for value in values] for key, values in data.items()}
# print(data_uppercase)
# # Loop through the categories and insert into ClickHouse
# for category, values in data_uppercase.items():
#     # Prepare the values in the required format
#     parameter_name = category
#     parameter_value = values
    
#     # Insert into ClickHouse
#     insert_query = f"""
#     INSERT INTO data_element (id, parameter_name, parameter_value)
#     VALUES ({id_counter}, '{parameter_name}', {parameter_value})
#     """
#     client.command(insert_query)
#     print(f"Inserted {parameter_name} with ID {id_counter}")

#     # Increment the ID counter
#     id_counter += 1

# print("Data insertion complete.")


# # # # client.command("truncate table data_element")
# # # client.command("truncate table data_element")
# client.command("""ALTER TABLE ner_unstructured_data ADD COLUMN data_element String AFTER json;""")
# client.command("""insert into ner_unstructured_data (data_element) values ('AADHAR')""")
# print(f"Atered and added COLUMN where data_element = 'AADHAR'")

# client.close()
# structured_view_data_locations = """
# CREATE VIEW IF NOT EXISTS structured_view_data_locations AS 
# SELECT 
#     d.region AS data_system_location_name,
#     COUNT(DISTINCT c.detected_entity) AS total_data_elements,
#     COUNT(DISTINCT CAST(splitByString(p.entityFQNHash, '.')[5] AS String)) AS total_columns,
#     COUNT(DISTINCT CAST(splitByString(p.entityFQNHash, '.')[4] AS String)) AS total_tables,
#     COUNT(DISTINCT CAST(splitByString(p.entityFQNHash, '.')[3] AS String)) AS total_schemas,
#     COUNT(DISTINCT CAST(splitByString(p.entityFQNHash, '.')[2] AS String)) AS total_databases,
#     COUNT(DISTINCT e.serviceType) AS total_data_systems
# FROM 
#     dbservice_entity_meta_info d
# LEFT JOIN 
#     column_ner_results c
#     ON d.dbservice_entity_id = c.table_id
# LEFT JOIN 
#     dbservice_entity e
#     ON e.id = d.dbservice_entity_id
# LEFT JOIN 
#     profiler_data_time_series p
#     ON splitByString(p.entityFQNHash, '.')[1] = e.nameHash
# GROUP BY 
#     d.region;

# """
# structured_view_data_shankyl = """
# CREATE VIEW IF NOT EXISTS structured_view_data_shanky AS 
# SELECT  
#     region AS locations 
    
#         (SELECT
#             DISTINCT serviceType AS data_system,
#             DISTINCT updatedBy AS data_system_owners
#         from dbservice_entity)

#         (select 
#             DISTINCT name AS databases 
#         from database_schema_entity)

#         (select 
#             DISTINCT column_name AS tags
#             DISTINCT detected_entity AS pii
#         from column_ner_results)
    
    
#     from dbservice_entity_meta_info d
#     JOIN
#     column_ner_results.table_id = dbservice_entity_meta_info.dbservice_entity_id
#     JOIN
#     dbservice_entity_meta_info.dbservice_entity_id = dbservice_entity.id
#     JOIN 
#     dbservice_entity.nameHash= splitByString(database_schema_entity.fqnHash, '.')[1]

# group by region;
# """


# structured_view_location = """
# CREATE VIEW IF NOT EXISTS structured_view_location AS
# SELECT 
#     COUNT(DISTINCT region) AS location
# FROM
#     dbservice_entity_meta_info;
# """

# try:

#     result = client.command(structured_view_location)
    

# except Exception as e:
#     print(f"Error occurred: {e}")




    
    # 
    # client.command("drop table structured_view_data_details;")
    # client.command("drop table structured_view_data_element_categories;")
    # client.command("drop table structured_view_data_element_types;")
