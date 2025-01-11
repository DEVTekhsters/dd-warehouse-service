from fastapi import APIRouter, UploadFile, HTTPException, BackgroundTasks
from app.utils.csv_processor import process_csv
from pii_scanner.scanner import PIIScanner
from pii_scanner.constants.patterns_countries import Regions
import clickhouse_connect
import logging
import tempfile
import os
import json
import uuid
from datetime import datetime
from collections import defaultdict
from typing import Dict
import asyncio
import pandas as pd
import csv
from io import StringIO

router = APIRouter()
logger = logging.getLogger(__name__)
pii_scanner = PIIScanner()

# Function to get a ClickHouse client
def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host='148.113.6.50',
        port="8123",
        username='default',
        password='',
        database='default'
    )

# # Asynchronous function to determine category and insert data into ClickHouse
# async def get_category_and_insert_data(element: str, parameter_value: str):
#     data_categories = {
#         "Personal": ["AADHAAR", "ADDRESS", "DRIVER LICENSE", "EMAIL_ADDRESS", "GENDER", "IMEI", "IMSI", "IP_ADDRESS", 
#                      "LOCATION", "MAC_ADDRESS", "NATIONALITY", "PASSPORT", "PASSWORD", "PERSON", "PHONE_NUMBER", 
#                      "POBOX", "RATION CARD NUMBER", "TITLE", "VEHICLE IDENTIFICATION NUMBER", "VID", "VOTERID", 
#                      "ZIPCODE", "RELIGION", "POLITICAL OPINION", "SEXUAL ORIENTATION"],
#         "Financial": ["BANK_ACCOUNT_NUMBER", "BANK_CARD", "CVV", "GST_NUMBER", "IFSC", "PAN", "UID", "UPI_ID"],
#         "Medical": ["MEDICAL_LICENSE", "BIOMETRIC"],
#         "System Data": ["DATE_TIME"]
#     }

#     table_id = "service_id_1"
#     service_type = "service_type_1"

#     element = element.upper()
#     category = "Unknown Category"
    
#     # Determine the category based on the element
#     for cat, elements in data_categories.items():
#         if element in elements:
#             category = cat
#             break

#     # Prepare query to insert data into ClickHouse
#     query = """
#     INSERT INTO data_element (table_id, service_type, parameter_name, parameter_value)
#     VALUES (%(table_id)s, %(service_type)s, %(parameter_name)s, %(parameter_value)s)
#     """
#     params = {
#         "table_id": table_id,
#         "service_type": service_type,
#         "parameter_name": element,
#         "parameter_value": parameter_value
#     }

#     client = get_clickhouse_client()
#     try:
#         # Execute the query to insert data
#         await client.command(query, params)
#         logger.info(f"Successfully inserted data for {element}.")
#     except Exception as e:
#         logger.error(f"Error inserting data into ClickHouse: {e}")
#         raise HTTPException(status_code=500, detail="Error inserting data into the database.")

# Main endpoint to process uploaded CSV file and perform NER
@router.post("/process/{table_id}")
async def predict_ner(table_id: str, file: UploadFile):
    if not table_id:
        raise HTTPException(status_code=400, detail="Missing required parameter: table_id")
    file_extension = file.filename.split(".")[-1]

    # Process the CSV file
    data = await process_file_data(file,file_extension)

    # Process NER and update results in ClickHouse
    save_result = await process_and_update_ner_results(table_id, data)
    
    if not save_result:
        raise HTTPException(status_code=500, detail=f"Failed to save or update NER data for table_id: {table_id}")
    
    return {"message": "Data uploaded and processed successfully", "details": save_result}  

async def process_file_data(file: UploadFile, file_extension: str) -> Dict:
    """
    Processes a file based on its extension (CSV, Excel, JSON) into a dictionary structure.
    """
    file_content = await file.read()
    try:
        if file_extension == 'csv':
            content_str = file_content.decode("utf-8")  # Decode byte content to string
            # Detect the delimiter automatically
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(content_str).delimiter
            
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
        logger.info(f"Processed {file_extension.upper()} file {file} with columns: {list(data.columns)}")
        return column_data
    
    except Exception as e:
        logger.error(f"Error processing {file_extension.upper()} file '{file}': {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error processing {file_extension.upper()} file: {str(e)}")

# Function to process NER results and update them in ClickHouse
async def process_and_update_ner_results(table_id: str, data: dict):
    try:
        for column_name, column_data in data.items():
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
            print(f"NER results for column {column_name}: {ner_results}")        
            if isinstance(ner_results, dict) and 'highest_label' in ner_results:
                detected_entity = ner_results['highest_label']
            else:
                detected_entity = 'NA'
            # Update the NER results in ClickHouse
            update_result = await update_entity_for_column(table_id, column_name, ner_results, detected_entity)
            if not update_result:
                logger.error(f"Failed to save NER results for table_id: {table_id}, column: {column_name}")
                return False

        return True
    except Exception as e:
        logger.error(f"Error processing NER results: {str(e)}")
        return False

# Function to update NER results for each column in ClickHouse
async def update_entity_for_column(table_id, column_name, ner_results, detected_entity):
    try:
        client = get_clickhouse_client()
        
        query = """
        INSERT INTO column_ner_results (table_id, column_name, json, detected_entity)
        VALUES (%(table_id)s, %(column_name)s, %(json)s, %(detected_entity)s)
        """
        
        params = {
            "table_id": table_id,
            "column_name": column_name,
            "json": json.dumps(ner_results),
            "detected_entity": detected_entity
        }

        # Execute query to insert/update data
        client.command(query, params)
        logger.info(f"Successfully inserted/updated NER result for table_id: {table_id}, column_name: {column_name}")
        return True
    except Exception as e:
        logger.error(f"Error inserting/updating data in ClickHouse: {str(e)}")
        return False
