from client_connect import Connection
import pandas as pd
import logging
import json
# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    print(data.columns)
    if identifier_column not in data.columns:
        logger.error(f"The DataFrame must contain a '{identifier_column}' column for updates.")
        raise ValueError(f"The DataFrame must contain a '{identifier_column}' column for updates.")
    
    # Drop rows where the identifier column has NaN or None values
    data = data.dropna(subset=[identifier_column])
    logger.info(f"Dropped rows with NaN in '{identifier_column}'. {len(data)} rows remaining.")

    # Ensure all identifiers are treated as strings
    data[identifier_column] = data[identifier_column].astype(str)
    print(data[identifier_column])
    logger.info(f"Converted '{identifier_column}' to string for all rows.")

    # Insert the data in batches    
    data = data.fillna({
        col: 'NA' if data[col].dtype == 'object' else 0  # Replace NaN with 'NA' for strings, 0 for numeric types
        for col in data.columns
    })
    
    try:
        # Collect all identifiers (either 'id' or 'entityFQNHash') from the incoming data
        ids = data[identifier_column].tolist()
        # Process deletion in batches to avoid exceeding query size limits
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i + batch_size]
            id_conditions = ",".join([f"'{row_id}'" for row_id in batch_ids])
            delete_query = f"ALTER TABLE {table_name} DELETE WHERE {identifier_column} IN ({id_conditions})"
            logger.debug(f"Executing query: {delete_query}")
            client.command(delete_query)
            logger.info(f"Deleted batch {i//batch_size + 1} from table '{table_name}'")


        # Now iterate over the data in batches
        for i in range(0, len(data[identifier_column]), batch_size):

            batch_data = data.iloc[i:i + batch_size]
            rows = batch_data.to_records(index=False).tolist()
            column_names = list(batch_data.columns)
            client.insert(table_name, rows, column_names=column_names)
            logger.info(f"Inserted batch {i // batch_size + 1} into table '{table_name}'")

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
                logger.info(f"row:{row}")
                logger.info(f"row_json :{row['json']}")
                data_host = json.loads(row['json'])  # JSON is also the column name in data
                # host_port = data_host['connection']['config']['hostPort']
                host_port = data_host.get('connection', {}).get('config', {}).get('hostPort', '')
                host_parts = host_port.split(".") if host_port else []

                # Ensure host_parts has enough elements before accessing indices
                region = host_parts[2] if len(host_parts) > 2 else "N/A"
                source = host_parts[4] if len(host_parts) > 4 else "N/A"

                # Extract other fields directly from the DataFrame
                # Extract fields based on entity_type
                if entity_type == "profiler_data_time_series":
                    dbservice_entity_id = row['entityFQNHash']  # 'entityFQNHash' is the actual column name for profiler_data_time_series
                    dbservice_entity_name = row['name'] if 'name' in row else "N/A"  # 'name' might not be present in profiler_data_time_series
                else:
                    dbservice_entity_id = row['id']  # 'id' is the actual column name for other tables
                    dbservice_entity_name = row['name']  # 'name' is the actual column name for other tables


                # Log the hostPort and extracted parts
                logger.info(f"Host port: {host_port}, Region: {region}, Source: {source}")


                # Append the extracted data as a tuple to the rows list
                rows.append((dbservice_entity_id, dbservice_entity_name, source, region))

            # Prepare column names for the insert statement
            column_names = ['dbservice_entity_id', 'dbservice_entity_name', 'source', 'region' ]

            # Insert the collected rows into the ClickHouse table
            client.insert(meta_info_table, rows, column_names=column_names)
            logger.info(f"Inserted batch {i // batch_size + 1} into table '{meta_info_table}'")


        logger.info(f"Successfully inserted or updated {len(data[identifier_column])} rows in table '{table_name}' and '{meta_info_table}'")
        return {"table": table_name, "rows_inserted_or_updated": len(data[identifier_column])}

    except Exception as e:
        logger.error(f"Error processing data: {e}")
        return {"error": str(e)}
