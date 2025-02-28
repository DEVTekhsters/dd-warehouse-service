from client_connect import Connection
import pandas as pd
import datetime
import logging
import json
import re
from app.utils.common_utils import BaseFileProcessor
# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

base_file_processor = BaseFileProcessor()

def save_omd_table_data(entity_type: str, data: pd.DataFrame, batch_size: int = 1000):
    """
    Optimized create or update data in ClickHouse based on 'id' or 'entityFQNHash' in batches, with type checking and NaN handling.
    """
    # Check if the data is empty
    if data.empty:
        logger.error("The DataFrame is empty. No data to insert or update.")
        raise ValueError("The DataFrame is empty. No data to insert or update.")
    
    connection = Connection()  # Example connection object (replace with your actual connection logic)
    client = connection.client
    
    table_name = f"{entity_type}"
    
    # Determine whether to use 'id' or 'entityFQNHash' based on entity_type
    if entity_type in ["dbservice_entity", "database_schema_entity", "field_relationship", "table_entity"]:
        identifier_column = 'id'
        logger.info(f"Using 'id' as the identifier for entity type '{entity_type}'")
    elif entity_type == "profiler_data_time_series":
        identifier_column = 'entityFQNHash'
        logger.info(f"Using 'entityFQNHash' as the identifier for entity type '{entity_type}'")
    else:
        logger.error(f"Unknown entity type: {entity_type}")
        raise ValueError(f"Unknown entity type: {entity_type}")

    # Ensure the identifier column exists and is converted to string (in case of float or NaN values)
    logger.debug(f"Data columns: {data.columns}")
    if identifier_column not in data.columns:
        logger.error(f"The DataFrame must contain a '{identifier_column}' column for updates.")
        raise ValueError(f"The DataFrame must contain a '{identifier_column}' column for updates.")
    
    # Drop rows where the identifier column has NaN or None values
    data = data.dropna(subset=[identifier_column])
    logger.info(f"Dropped rows with NaN in '{identifier_column}'. {len(data)} rows remaining.")

    # Ensure all identifiers are treated as strings
    data[identifier_column] = data[identifier_column].astype(str)
    logger.info(f"Converted '{identifier_column}' to string for all rows.")

    # Insert the data in batches    
    data = data.fillna({
        col: 'NA' if data[col].dtype == 'object' else 0  # Replace NaN with 'NA' for strings, 0 for numeric types
        for col in data.columns
    })
    
    try:
        # Collect all identifiers (either 'id' or 'entityFQNHash') from the incoming data
        ids = data[identifier_column].tolist()  # Convert to list here, after handling NaNs
        logger.debug(f"IDs: {ids}, Type: {type(ids)}")
        
        # Process deletion in batches to avoid exceeding query size limits
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i + batch_size]
            id_conditions = ",".join([f"'{row_id}'" for row_id in batch_ids])
            delete_query = f"ALTER TABLE {table_name} DELETE WHERE {identifier_column} IN ({id_conditions})"
            logger.debug(f"Executing query: {delete_query}")
            client.command(delete_query)
            logger.info(f"Deleted batch {i // batch_size + 1} from table '{table_name}'")

        logger.info("step 1 ---------------------- deleted ")
        logger.info("*"*20)
        logger.info(f"Data for '{identifier_column}': {data[identifier_column]}, Type: {type(data[identifier_column])},Type data: {type(data)},  Length: {len(data[identifier_column])}")

        import json

        # Now iterate over the data in batches
        length = len(data[identifier_column])
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>")

        for i in range(0, length, batch_size):
            batch_data = data.iloc[i:i + batch_size].copy()  # Copy to avoid modifying original data
            # Convert JSON-like column correctly
            for idx, row in batch_data.iterrows():
                try:
                    json_str = row['json']
                    # Ensure it's a valid JSON format
                    json_str = json_str.replace("'", '"').replace("False", "false").replace("True", "true")
                    # Remove trailing commas if any
                    json_str = json_str.rstrip(',')
                    # Validate JSON format
                    json.loads(json_str)  # Check if it's valid JSON
                    # Update the DataFrame with the corrected JSON
                    batch_data.at[idx, 'json'] = json_str
                except Exception as e:
                    print(f"Error in row {idx}: {e}")
                    continue  # Skip row if there's an error

            # Convert DataFrame to list of tuples
            rows = batch_data.to_records(index=False).tolist()
            column_names = list(batch_data.columns)

            # Insert into ClickHouse
            client.insert(table_name, rows, column_names=column_names)
            logger.info(f"Inserted batch {i // batch_size + 1} into table '{table_name}'")

        logger.info("*" * 20)
        
        
        logger.info("step 2 ----------------------")

        # Delete from dbservice_entity_meta_info
        meta_info_table = "dbservice_entity_meta_info"
        identifier_column_meta = "dbservice_entity_id"
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i + batch_size]
            id_conditions = ",".join([f"'{row_id}'" for row_id in batch_ids])
            delete_meta_query = f"ALTER TABLE {meta_info_table} DELETE WHERE {identifier_column_meta} IN ({id_conditions})"
            logger.debug(f"Executing delete query for meta info: {delete_meta_query}")
            client.command(delete_meta_query)
            logger.info(f"Deleted batch {i // batch_size + 1} from table '{meta_info_table}'")

        # Insert data into dbservice_entity_meta_info
        for i in range(0, len(data[identifier_column]), batch_size):
            batch_data = data.iloc[i:i + batch_size]
            rows = []  # Initialize an empty list to collect rows
            for index in batch_data.index:
                row = batch_data.loc[index]
                row = row.to_dict()
                logger.debug(f"Row_json:{row['json']}")
        
                json_str = row['json'].replace("'", '"').replace("False", "false").replace("True", "true")
                json_str = json_str.rstrip(',')  # Remove any trailing commas

                logger.debug(f"Modified JSON string: {json_str}")
                
                try:
                    data_host = json.loads(json_str)  # JSON is also the column name in data
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON: {e}")
                    continue

                host_port = data_host.get('connection', {}).get('config', {}).get('hostPort', '')
                # Split the hostPort value
                host_parts = host_port.split(".") if host_port else []
                # Ensure host_parts has enough elements before accessing indices
                host_region = host_parts[2] if len(host_parts) > 2 else "N/A"
                source = host_parts[4] if len(host_parts) > 4 else "N/A"
                # Updating the aws region to countries
                region = base_file_processor.aws_region_update(host_region)
                
                # Extract other fields directly from the DataFrame
                if entity_type == "profiler_data_time_series":
                    dbservice_entity_id = row['entityFQNHash']
                    dbservice_entity_name = row.get('name', "N/A")
                else:
                    dbservice_entity_id = row['id']
                    dbservice_entity_name = row['name']

                logger.debug(f"Host port: {host_port}, Region: {region}, Source: {source}")

                # Append the extracted data as a tuple to the rows list
                rows.append((dbservice_entity_id, dbservice_entity_name, source, region))

            # Prepare column names for the insert statement
            column_names = ['dbservice_entity_id', 'dbservice_entity_name', 'source', 'region']
            
            # Insert the collected rows into the ClickHouse table
            client.insert(meta_info_table, rows, column_names=column_names)
            logger.info(f"Inserted batch {i // batch_size + 1} into table '{meta_info_table}'")

        # if entity_type == "profiler_data_time_series":
        #     profiler_meta_data(ids, batch_size, identifier_column, data, client)
        #     logger.info("Successfully deleted/inserted data in profiler_metadata table")

        logger.info(f"Successfully inserted or updated {len(data[identifier_column])} rows in table '{table_name}' and '{meta_info_table}'")
        return {"table": table_name, "rows_inserted_or_updated": len(data[identifier_column])}

    except Exception as e:
        logger.error(f"Error processing data: {e}")
        return {"error": str(e)}

# def profiler_meta_data(ids, batch_size, identifier_column, data, client):
#     table_name = "profiler_metadata"
#     for i in range(0, len(ids), batch_size):
#         # Deleting existing records in batches
#         batch_ids = ids[i:i + batch_size]
#         id_conditions = ",".join([f"'{row_id}'" for row_id in batch_ids])
#         delete_query = f"ALTER TABLE {table_name} DELETE WHERE {identifier_column} IN ({id_conditions})"
#         logger.debug(f"Executing query: {delete_query}")
#         client.command(delete_query)
#         logger.info(f"Deleted batch {i // batch_size + 1} from table '{table_name}'")
        

#     # Insert data in batches
#     for i in range(0, len(data), batch_size):
#         batch_data = data.iloc[i:i + batch_size]
#         column_names = ["entityFQNHash", "rowCount", "timestamp", "sizeInByte", "columnCount", "profileSample", "createDateTime", "profileSampleType"]

#         # Filter rows where jsonSchema is "tableProfile"
#         filtered_data = batch_data[batch_data["jsonSchema"] == "tableProfile"]

#         if not filtered_data.empty:
#             rows = []
#             for _, row in filtered_data.iterrows():
#                 try:
                
#                     json_str = row['json'].replace("'", '"').replace("False", "false").replace("True", "true")
#                     json_str = json_str.rstrip(',')  # Remove any trailing commas
#                     try:
#                         json_data = json.loads(json_str)  # JSON is also the column name in data
#                     except json.JSONDecodeError as e:
#                         logger.error(f"Error decoding JSON: {e}")
#                         continue
                
#                     # Convert createDateTime to Unix timestamp (seconds since epoch)
#                     createDateTime_str = json_data.get("createDateTime")
#                     if createDateTime_str:
#                         createDateTime = datetime.datetime.strptime(createDateTime_str, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
#                     else:
#                         createDateTime = None  # Handle case where 'createDateTime' is missing

#                     rows.append((
#                         row["entityFQNHash"],
#                         json_data.get("rowCount"),
#                         json_data.get("timestamp"),
#                         json_data.get("sizeInByte"),
#                         json_data.get("columnCount"),
#                         json_data.get("profileSample"),
#                         createDateTime,  # Insert the Unix timestamp here
#                         json_data.get("profileSampleType")
#                     ))
#                 except Exception as parse_error:
#                     logger.error(f"Error parsing JSON for row: {row['entityFQNHash']}, error: {parse_error}")

#             try:
#                 # Uncomment this to execute the insertion
#                 client.insert(table_name, rows, column_names=column_names)
#                 logger.info(f"Inserted batch {i // batch_size + 1} into table '{table_name}'")
#             except Exception as insert_error:
#                 logger.error(f"Error during insert operation: {insert_error}")
#         else:
#             logger.info(f"No rows with jsonSchema='tableProfile' in batch {i // batch_size + 1}")
