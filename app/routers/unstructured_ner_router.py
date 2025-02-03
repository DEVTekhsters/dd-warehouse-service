import os
import json
import logging
from pathlib import Path
from minio import Minio
from dotenv import load_dotenv
from client_connect import Connection
from collections import defaultdict
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pii_scanner.scanner import PIIScanner
from pii_scanner.constants.patterns_countries import Regions
from pydantic import BaseModel
from app.constants.sensitivity_data import SENSITIVITY_MAPPING
import pandas as pd
import csv
from typing import Dict
from io import StringIO
import nltk

# Download necessary NLTK resources for natural language processing
nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger_eng')

# Setup logging for the application
logger = logging.getLogger(__name__)

# Load environment variables from a .env file
load_dotenv()

# Define the temporary folder path for storing files
TEMP_FOLDER = Path(__file__).resolve().parent.parent / 'utils/pii_scan/temp_files'
if not TEMP_FOLDER.exists():
    TEMP_FOLDER.mkdir(parents=True, exist_ok=True)  # Create the temp folder if it doesn't exist

# Initialize FastAPI router for defining API endpoints
router = APIRouter()
scanner = PIIScanner()  # Initialize the PII scanner

# Pydantic model to receive metadata from the request
class DataReceived(BaseModel):
    source_type: str  # Type of the source (e.g., service type)
    source_bucket: str  # Name of the source bucket
    region: str  # Region information
    message: str  # Additional message

# MinIO configuration for object storage
MINIO_URL = os.getenv("MINIO_URL")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"  # Boolean for secure connection

# Define supported file formats
UNSTRUCTURED_FILE_FORMATS = os.getenv("UNSTRUCTURED_FILE_FORMATS").split(',')
STRUCTURED_FILE_FORMATS = os.getenv("STRUCTURED_FILE_FORMATS").split(',')

# Initialize MinIO client for accessing object storage
minio_client = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

# Function to get ClickHouse client for database operations
def get_clickhouse_client():
    return Connection.client 

# FastAPI route to process unstructured files in the background
@router.post("/process_unstructured")
async def process_unstructured_files(data_received: DataReceived, background_tasks: BackgroundTasks):
    bucket_name = MINIO_BUCKET_NAME  # Get the bucket name from the environment
    folder_name = data_received.source_bucket  # Get the folder name from the request

    # Start the background task to process files from MinIO
    background_tasks.add_task(process_files_from_minio, bucket_name, folder_name, data_received)

    return {"results": f"Processing started for the folder: {folder_name}"}

# Function to process files from MinIO
async def process_files_from_minio(bucket_name: str, folder_name: str, data_received: DataReceived):
    try:
        # List objects (files) under the specified folder in MinIO
        objects = minio_client.list_objects(bucket_name, prefix=folder_name + "/", recursive=True)

        for obj in objects:
            file_name = obj.object_name  # Get the file name
            file_extension = file_name.split(".")[-1].lower()  # Get the file extension

            logger.info(f"Processing file: {file_name}")
            # Check if the file extension is supported
            if file_extension not in UNSTRUCTURED_FILE_FORMATS and file_extension not in STRUCTURED_FILE_FORMATS:
                logger.warning(f"File {file_name} has an unsupported format. Logging and removing it.")
                # log_and_remove_file(bucket_name, file_name, folder_name)  # Log and remove unsupported files
                continue

            # Download the file to the local temp directory
            temp_file_path = TEMP_FOLDER / file_name.split("/")[-1]
            minio_client.fget_object(bucket_name, file_name, str(temp_file_path))

            try:
                # Process the file with NER
                await process_ner_for_file(temp_file_path, data_received)
                # If processing is successful, remove the file from MinIO
                minio_client.remove_object(bucket_name, file_name)
                logger.info(f"Successfully deleted file: {file_name} from MinIO.")
            except Exception as ner_error:
                logger.error(f"NER processing failed for file {file_name}: {str(ner_error)}")
                continue  # Continue processing next files

            # Remove the local temp file
            if temp_file_path.exists():
                os.remove(temp_file_path)
                logger.info(f"Deleted local temp file: {temp_file_path}")

    except Exception as e:
        logger.error(f"Error during file processing: {e}")
        raise HTTPException(status_code=500, detail=f"Error during file processing: {str(e)}")

# # Function to log unsupported files and remove them from MinIO
# def log_and_remove_file(bucket_name: str, file_name: str, folder_name: str):
#     log_file_path = TEMP_FOLDER / f"{folder_name}_unsupported_files.txt"  # Log file path
#     with open(log_file_path, "a") as log_file:
#         log_file.write(f"{file_name}\n")  # Log the unsupported file name
#     minio_client.remove_object(bucket_name, file_name)  # Remove the file from MinIO
#     logger.info(f"Logged and removed unsupported file: {file_name}")

# Process the file with the NER model
async def process_ner_for_file(file_path: Path, data_received: DataReceived):
    file_name = file_path.name  # Get the file name
    file_size = file_path.stat().st_size  # Get the file size
    file_type = file_name.split(".")[-1].upper()  # Get the file type

    logger.info(f"Processing file: {file_name}, Size: {file_size} bytes, Type: {file_type}")

    # Extract source type and sub-service from the received data
    source_type = data_received.source_type.split(',')
    source = source_type[0].strip() if len(source_type) > 0 else "N/A"
    sub_service = source_type[1].strip() if len(source_type) > 1 else "N/A"

    # Prepare metadata for the processed file
    metadata = {
        "source_bucket": data_received.source_bucket,
        "file_name": file_name,
        "file_size": file_size,
        "file_type": file_type,
        "source": source,
        "sub_service": sub_service,
        "region": data_received.region,
    }

    try:
        # Check if the file type is structured or unstructured and process accordingly
        if file_type.lower() in STRUCTURED_FILE_FORMATS:
            results = await process_and_update_ner_results_structured(file_name, file_path, file_type, metadata)
        elif file_type.lower() in UNSTRUCTURED_FILE_FORMATS:
            results = await process_and_update_ner_results_unstructured(file_path, file_type, file_name, metadata)

        if not results:
            logger.error("Results are not detected")

        logger.info(f"PII results for file {file_name}")
        return {"message": "File processed successfully", "metadata": metadata}

    except Exception as e:
        logger.error(f"Error processing file {file_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error during NER processing: {str(e)}")

# Function to process NER results and update them in ClickHouse for structured files
async def process_and_update_ner_results_structured(file_name: str, file_path: Path, file_type: str, metadata: dict):
    
    data = process_file_data(file_path, file_type)  # Process the file data based on its type
    try:
        for column_name, column_data in data.items():
            # Perform NER scanning on the column data
            json_result = await scanner.scan(data=column_data, sample_size=5, region=Regions.IN)
            entity_counts = defaultdict(int)  # Dictionary to count detected entities
            total_entities = 0  # Total number of entities detected
            ner_results = 'NA'  # Initialize NER results

            if json_result:  # Check if there is a valid response from the NER scanner
                for result in json_result.get("results", []):  # Iterate over each result in the JSON response
                    # Check if 'entity_detected' exists and is a non-empty list
                    if result.get("entity_detected") and len(result["entity_detected"]) > 0:
                        entity_type = result["entity_detected"][0].get("type", "Unknown")  # Extract entity type
                        entity_counts[entity_type] += 1  # Increment count for detected entity type
                        total_entities += 1  # Increment total entity count

                # If entities were detected, determine the most frequent entity type and its confidence score
                if entity_counts:
                    highest_label = max(entity_counts.items(), key=lambda x: x[1])[0]  
                    # Compute confidence score (ratio of most frequent entity type occurrences to total entities detected)
                    confidence_score = round(max(entity_counts.values()) / total_entities, 2)
                    ner_results = {
                        'highest_label': highest_label,  # Most frequently detected entity type
                        'confidence_score': confidence_score,  # Confidence score of the detected entity
                        'detected_entities': {k: v for k, v in entity_counts.items()}  # Dictionary of all detected entity counts
                    }
                else:
                    # No entities were detected, return default values
                    ner_results = {'highest_label': 'NA', 'confidence_score': 0, 'detected_entities': {}}

                # Log the NER results for the specific column
                logger.info(f"NER results for column {column_name}: {ner_results}")

                # Extract the highest detected entity type, defaulting to 'NA' if none found
                if isinstance(ner_results, dict) and 'highest_label' in ner_results:
                    detected_entity = ner_results['highest_label']
                else:
                    detected_entity = 'NA'

                # **Fetch data element category and sensitivity**
                # These functions are used to classify the detected entity type and assign sensitivity levels.
                # - `data_element_category(detected_entity)`: Maps the detected entity to a predefined category (e.g., Name, Address, Email, etc.).
                # - `sensitivity_of_detected_entity(detected_entity)`: Assigns a sensitivity score (e.g., High, Medium, Low) based on the type of entity detected.

                data_element = await data_element_category(detected_entity)  # Retrieve the category of the detected entity
                data_sensitivity = await sensitivity_of_deteceted_enetity(detected_entity)  # Determine the sensitivity level of the detected entity

                # Uncomment the following lines to save the NER results to the database
                update_result = save_unstructured_ner_data(ner_results, metadata, data_element, data_sensitivity, detected_entity, column_name)
                if not update_result:
                    logger.error(f"Failed to save NER results for file_name: {file_name}, column: {column_name}")
                    return False

        return True  # Return success if processing is complete
    except Exception as e:
        logger.error(f"Error processing file {file_name} {file_type}: {str(e)}")
        return False  # Return failure if an error occurs

# Function to process file data based on its extension
def process_file_data(file_path: Path, file_type: str) -> Dict:
    file_type = file_type.lower()  # Normalize the file type to lowercase
    try:
        # Read CSV files
        if file_type == 'csv':
            with open(file_path, "r", encoding="utf-8") as f:
                content_str = f.read()
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(content_str).delimiter  # Automatically detect the delimiter
            
            data = pd.read_csv(file_path, sep=delimiter)  # Read the CSV file with the detected delimiter

        # Read Excel files
        elif file_type in ['xlsx', 'xls']:
            data = pd.read_excel(file_path)

        # Read JSON files
        elif file_type == 'json':
            with open(file_path, "r", encoding="utf-8") as f:
                data = pd.read_json(f)

        else:
            raise ValueError("Unsupported file format")  # Raise an error for unsupported formats

        if data.empty:
            raise ValueError(f"No valid data found in {file_type.upper()} file")  # Check for empty data

        # Convert DataFrame columns to a dictionary of lists
        column_data = {col: [str(value) for value in data[col]] for col in data.columns}
        logger.info(f"Processed {file_type.upper()} file {file_path} with columns: {list(data.columns)}")
        return column_data  # Return the processed data

    except Exception as e:
        logger.error(f"Error processing {file_type.upper()} file '{file_path}': {str(e)}")
        raise ValueError(f"Error processing {file_type.upper()} file: {str(e)}")  # Raise an error if processing fails

# Function to process NER results and update them in ClickHouse for unstructured files
async def process_and_update_ner_results_unstructured(file_path: Path, file_type: str, file_name: str, metadata: dict):
    
    entity_counts = defaultdict(int)  # Dictionary to count detected entities
    total_entities = 0  # Total number of entities detected
    ner_results = 'NA'  # Initialize NER results
    try:
        # Perform NER scanning on the file
        json_result = await scanner.scan(str(file_path), sample_size=0.2, region=Regions.IN)

        if not json_result:
            logger.error(f"No PII detected in the file {file_name} {file_type}. Skipping further processing.")
            raise ValueError("No PII data detected in the file.")
        
        if json_result:
            for result in json_result:
                if isinstance(result, dict) and "entity_detected" in result:
                    detected_entities = result["entity_detected"]
                    if isinstance(detected_entities, list):
                        for entity in detected_entities:
                            if isinstance(entity, dict):
                                entity_type = entity.get("type")  # Get the entity type
                                if entity_type:
                                    entity_counts[entity_type] += 1  # Increment the count for the detected entity type
                                    total_entities += 1  # Increment the total entity count

            if entity_counts:
                # Determine the highest label and confidence score
                highest_label = max(entity_counts.items(), key=lambda x: x[1])[0]
                confidence_score = round(max(entity_counts.values()) / total_entities, 2)
                ner_results = {
                    'highest_label': highest_label,
                    'confidence_score': confidence_score,
                    'detected_entities': {k: v for k, v in entity_counts.items()}
                }
                
                # Fetch data element category and sensitivity
                data_element = await data_element_category(highest_label)
                data_sensitivity = await sensitivity_of_deteceted_enetity(highest_label)
                column_name = "N/A"  # Placeholder for column name
                save_unstructured_ner_data(ner_results, metadata, data_element, data_sensitivity, highest_label, column_name)  # Save results

            return True  # Return success if processing is complete

    except Exception as e:
        logger.error(f"Error processing file {file_name} {file_type}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error during NER processing: {str(e)}")  # Raise an error if processing fails

# Function to fetch data element category from ClickHouse
async def data_element_category(detected_entity):
    logger.info("Processing result in data_element_category")
    try:
        client = get_clickhouse_client()  # Get the ClickHouse client
        data_element_query = f"""SELECT parameter_name FROM data_element WHERE has(parameter_value, '{detected_entity}');"""
        result = client.query(data_element_query)  # Execute the query to fetch the data element category
        
        if result.result_rows:
            category = result.result_rows[0][0]  # Get the first result
            logger.info(f"Data element category for {detected_entity}: {category}")
            return category
        else:
            logger.info(f"No data element category found for {detected_entity}. Adding to 'unknown' category.")
            check_unknown_query = f"""SELECT parameter_value FROM data_element WHERE parameter_name = 'UNKNOWN';"""
            unknown_result = client.query(check_unknown_query)  # Check if 'UNKNOWN' category exists
            
            if unknown_result.result_rows:
                existing_values = unknown_result.result_rows[0][0]
                if detected_entity not in existing_values:
                    updated_values = existing_values + [detected_entity]
                    update_unknown_query = f"""
                    ALTER TABLE data_element UPDATE parameter_value = {updated_values} WHERE parameter_name = 'UNKNOWN';
                    """
                    client.command(update_unknown_query)  # Update the existing 'unknown' category
                    logger.info(f"Updated 'UNKNOWN' category with {detected_entity}.")
            else:
                insert_unknown_query = f"""
                INSERT INTO data_element (parameter_name, parameter_value) VALUES ('UNKNOWN', ['{detected_entity}'])
                """
                client.command(insert_unknown_query)  # Insert a new 'unknown' category
                logger.info(f"Added {detected_entity} to 'UNKNOWN' category.")
            
            return "UNKNOWN"  # Return 'UNKNOWN' if no category found

    except Exception as e:
        logger.error(f"Error fetching data element category from ClickHouse: {str(e)}")
        return f"Error: {str(e)}"  # Return error message if fetching fails

# Function to fetch sensitivity of detected entity
async def sensitivity_of_deteceted_enetity(detected_entity):
    logger.info(f"Fetching sensitivity for detected entity: {detected_entity}")
    if detected_entity in SENSITIVITY_MAPPING:
        sensitivity = SENSITIVITY_MAPPING[detected_entity]  # Get sensitivity from mapping
        logger.info(f"Sensitivity for {detected_entity}: {sensitivity}")
        return sensitivity
    else:
        logger.warning(f"Sensitivity for {detected_entity} not found. Returning 'Unknown'.")
        return "Unknown"  # Return 'Unknown' if sensitivity not found

# Save NER results to ClickHouse database
def save_unstructured_ner_data(ner_results, metadata, data_element, data_sensitivity, detected_entity, column_name):
    if not ner_results:
        logger.error("No PII data detected in the file.")
        raise ValueError("No PII data detected")  # Raise an error if no PII data is detected

    ner_results_json = json.dumps(ner_results)  # Convert results to JSON

    # Prepare data for insertion into the database
    data_to_insert = {
        "source_bucket": metadata.get("source_bucket"),
        "file_name": metadata.get("file_name"),
        "column_name": column_name,
        "json": ner_results_json,
        "detected_entity": detected_entity,
        "data_element": data_element,
        "data_sensitivity": data_sensitivity,
        "file_size": metadata.get("file_size"),
        "file_type": metadata.get("file_type"),
        "source": metadata.get("source"),
        "sub_service": metadata.get("sub_service"),
        "region": metadata.get("region")
    }

    connection = Connection()  # Create a connection to the database
    client = connection.client  # Get the client

    try:
        # Insert data into the ner_unstructured_data table
        insert_query = """
        INSERT INTO ner_unstructured_data (source_bucket, file_name, column_name, json, detected_entity, data_element, data_sensitivity, file_size, file_type, source, sub_service, region)
        VALUES (%(source_bucket)s, %(file_name)s, %(column_name)s, %(json)s, %(detected_entity)s, %(data_element)s, %(data_sensitivity)s, %(file_size)s, %(file_type)s, %(source)s, %(sub_service)s, %(region)s)
        """
        client.command(insert_query, data_to_insert)  # Execute the insert command
        logger.info("Successfully inserted data into the ner_unstructured_data table.")
        return True
    except Exception as e:
        logger.error(f"Error inserting data into the database: {e}")
        raise HTTPException(status_code=500, detail="Error inserting data into the database")  # Raise an error if insertion fails
    