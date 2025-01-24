from fastapi import APIRouter, UploadFile, HTTPException
from pii_scanner.scanner import PIIScanner
from pii_scanner.constants.patterns_countries import Regions
import logging
import os
import json
from dotenv import load_dotenv
from collections import defaultdict
from typing import Dict
import pandas as pd
import csv
from io import StringIO
from client_connect import Connection
from app.constants.sensitivity_data import SENSITIVITY_MAPPING

# Setup logging
logger = logging.getLogger(__name__)

# Load environment variables from the .env file
load_dotenv()

# Initialize FastAPI router
router = APIRouter()

# Initialize PIIScanner instance
pii_scanner = PIIScanner()

# Load structured file formats from environment variables
STRUCTURED_FILE_FORMATS = os.getenv("STRUCTURED_FILE_FORMATS").split(',')

# Function to get a ClickHouse client
def get_clickhouse_client():
    return Connection.client 

# Main endpoint to process uploaded file and perform NER
@router.post("/process/{table_id}")
async def predict_ner(table_id: str, file: UploadFile):
    if not table_id:
        raise HTTPException(status_code=400, detail="Missing required parameter: table_id")
    
    # Get the file extension
    file_extension = file.filename.split(".")[-1]

    # Process the file based on its extension
    data = await process_file_data(file, file_extension)

    # Process NER and update results in ClickHouse
    save_result = await process_and_update_ner_results(table_id, data)
    
    if not save_result:
        raise HTTPException(status_code=500, detail=f"Failed to save or update NER data for table_id: {table_id}")
    
    return {"message": "Data uploaded and processed successfully", "details": save_result}

# Function to process file data based on its extension
async def process_file_data(file: UploadFile, file_extension: str) -> Dict:
    """
    Processes a file based on its extension (CSV, Excel, JSON) into a dictionary structure.
    """
    if file_extension.lower() not in STRUCTURED_FILE_FORMATS:
        logger.error(f"Unsupported file format: {file_extension} for {file}")
        raise HTTPException(status_code=400, detail="Unsupported file format")

    file_content = await file.read()
    try:
        if file_extension == 'csv':
            content_str = file_content.decode("utf-8")  # Decode byte content to string
            # Detect the delimiter automatically
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(content_str).delimiter  # Detect delimiter automatically
            # Read CSV with detected delimiter
            data = pd.read_csv(StringIO(content_str), sep=delimiter)
        elif file_extension in ['xlsx', 'xls']:
            data = pd.read_excel(file_content)
        elif file_extension == 'json':
            content_str = file_content.decode("utf-8")
            data = pd.read_json(StringIO(content_str))
        else:
            raise ValueError("Unsupported file format")

        if data.empty:
            raise ValueError(f"No valid data found in {file_extension.upper()} file")

        column_data = {col: [str(value) for value in data[col]] for col in data.columns}
        logger.info(f"Processed {file_extension.upper()} file {file.filename} with columns: {list(data.columns)}")
        return column_data
    
    except Exception as e:
        logger.error(f"Error processing {file_extension.upper()} file '{file.filename}': {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error processing {file_extension.upper()} file: {str(e)}")

# Function to process NER results and update them in ClickHouse
async def process_and_update_ner_results(table_id: str, data: dict):
    try:
        for column_name, column_data in data.items():
            # Perform NER scanning on the column data
            json_result = await pii_scanner.scan(data=column_data, sample_size=5, region=Regions.IN)
            
            entity_counts = defaultdict(int)
            total_entities = 0
            ner_results = 'NA'
            
            # Process the NER results
            if json_result:
                for result in json_result.get("results", []):
                    if "entity_detected" in result and result["entity_detected"]:
                        entity_type = result["entity_detected"][0].get("type")
                        if entity_type:
                            entity_counts[entity_type] += 1
                            total_entities += 1

                if entity_counts:
                    highest_label = max(entity_counts.items(), key=lambda x: x[1])[0]
                    confidence_score = round(max(entity_counts.values()) / total_entities, 2)
                    ner_results = {
                        'highest_label': highest_label,
                        'confidence_score': confidence_score,
                        'detected_entities': {k: v for k, v in entity_counts.items()}
                    }
            logger.info(f"NER results for column {column_name}: {ner_results}")        
            if isinstance(ner_results, dict) and 'highest_label' in ner_results:
                detected_entity = ner_results['highest_label']
            else:
                detected_entity = 'NA'

            # Update the NER results in ClickHouse
            data_element = await fetch_data_element_category(detected_entity)
            data_sensitivity = await sensitivity_of_deteceted_enetity(detected_entity)

            update_result = await update_entity_for_column(table_id, column_name, ner_results, detected_entity, data_element, data_sensitivity)
            
            if not update_result:
                logger.error(f"Failed to save NER results for table_id: {table_id}, column: {column_name}")
                return False

        return True
    except Exception as e:
        logger.error(f"Error processing NER results: {str(e)}")
        return False

# Function to fetch data element category from ClickHouse
async def fetch_data_element_category(detected_entity):
    try:
        client = get_clickhouse_client()
        data_element_query = f"""SELECT parameter_name
        FROM data_element
        WHERE has(parameter_value, '{detected_entity}');"""

        # Execute query to fetch data element category
        result = client.query(data_element_query)
        
        # Check if result is not empty and fetch the first row
        if result.result_rows:
            category = result.result_rows[0][0]
            logger.info(f"Data element category for {detected_entity}: {category}")
            return category
        else:
            logger.info(f"No data element category found for {detected_entity}. Adding to 'UNKNOWN' category.")
            
            # Check if the 'unknown' category already exists
            check_unknown_query = f"""SELECT parameter_value FROM data_element WHERE parameter_name = 'UNKNOWN';"""
            unknown_result = client.query(check_unknown_query)
            
            if unknown_result.result_rows:
                # Update the existing 'unknown' category
                existing_values = unknown_result.result_rows[0][0]
                if detected_entity not in existing_values:
                    updated_values = existing_values + [detected_entity]
                    update_unknown_query = f"""
                    ALTER TABLE data_element UPDATE parameter_value = {updated_values} WHERE parameter_name = 'UNKNOWN';
                    """
                    client.command(update_unknown_query)
                    logger.info(f"Updated 'UNKNOWN' category with {detected_entity}.")
            else:
                # Insert a new 'unknown' category
                insert_unknown_query = f"""
                INSERT INTO data_element (parameter_name, parameter_value) VALUES ('UNKNOWN', ['{detected_entity}'])
                """
                client.command(insert_unknown_query)
                logger.info(f"Added {detected_entity} to 'UNKNOWN' category.")
            
            return "UNKNOWN"
        
    except Exception as e:
        logger.info(f"Error fetching data element category from ClickHouse: {str(e)}")
        return f"Error: {str(e)}"
    
async def sensitivity_of_deteceted_enetity(detected_entity):
    logger.info(f"Fetching sensitivity for detected entity: {detected_entity}")
    if detected_entity in SENSITIVITY_MAPPING:
        sensitivity = SENSITIVITY_MAPPING[detected_entity]
        logger.info(f"Sensitivity for {detected_entity}: {sensitivity}")
        return sensitivity
    else:
        logger.warning(f"Sensitivity for {detected_entity} not found. Returning 'Unknown'.")
        return "Unknown"
    
# Function to update NER results for each column in ClickHouse
async def update_entity_for_column(table_id, column_name, ner_results, detected_entity, data_element, data_sensitivity):
    try:
        client = get_clickhouse_client()
        
        query = """
        INSERT INTO column_ner_results (table_id, column_name, json, detected_entity, data_element, data_sensitivity)
        VALUES (%(table_id)s, %(column_name)s, %(json)s, %(detected_entity)s, %(data_element)s, %(data_sensitivity)s)
        """
        
        params = {
            "table_id": table_id,
            "column_name": column_name,
            "json": json.dumps(ner_results),
            "detected_entity": detected_entity,
            "data_element": data_element,
            "data_sensitivity":data_sensitivity
        }

        # Execute query to insert/update data
        client.command(query, params)
        logger.info(f"Successfully inserted/updated NER result for table_id: {table_id}, column_name: {column_name}")
        return True
    except Exception as e:
        logger.error(f"Error inserting/updating data in ClickHouse: {str(e)}")
        return False