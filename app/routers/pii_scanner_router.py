from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from typing import List, Dict, Any
import os
import logging
from uuid import uuid4
from datetime import datetime
from client_connect import Connection
from collections import defaultdict
import asyncio
import json
from dotenv import load_dotenv
from pii_scanner.scanner import PIIScanner
from pii_scanner.constants.patterns_countries import Regions

load_dotenv()
router = APIRouter()

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Define supported file formats
UNSTRUCTURED_FILE_FORMATS = os.getenv("UNSTRUCTURED_FILE_FORMATS").split(',')
STRUCTURED_FILE_FORMATS = os.getenv("STRUCTURED_FILE_FORMATS").split(',')

async def save_file(file: UploadFile, temp_dir: str) -> str:
    """
    Saves the uploaded file temporarily and returns the file path.
    """
    file_path = os.path.join(temp_dir, file.filename)
    try:
        with open(file_path, "wb") as temp_file:
            temp_file.write(await file.read())
        logger.info(f"File {file.filename} saved successfully to {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Failed to save file '{file.filename}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to save file '{file.filename}'.")

def get_human_readable_size(file_path: str) -> str:
    size_bytes = os.path.getsize(file_path)
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.0f} KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes / 1024 ** 2:.0f} MB"
    else:
        return f"{size_bytes / 1024 ** 3:.0f} GB"

async def process_instant_classifier_files(file_path: str, file_extension: str, file_name: str) -> Dict[str, Any]:
    """
    Applies the NER scanner on the file and returns the results.
    """
    entity_counts = defaultdict(int)
    total_entities = 0
    ner_results = 'NA'
    scanner = PIIScanner()
    
    try:
        json_result = await scanner.scan(str(file_path), sample_size=0.2, region=Regions.IN)
        
        if not json_result:
            logger.error(f"No PII detected in the file {file_name} ({file_extension}).")
            raise ValueError("No PII data detected in the file.")

        # Check if json_result is a list
        if isinstance(json_result, list):
            # Process document file results
            for result in json_result:
                if isinstance(result, dict) and "entity_detected" in result:
                    detected_entities = result["entity_detected"]
                    if isinstance(detected_entities, list):
                        for entity in detected_entities:
                            if isinstance(entity, dict):
                                entity_type = entity.get("type")
                                if entity_type:
                                    entity_counts[entity_type] += 1
                                    total_entities += 1
                                    
                # Process image file results
                elif isinstance(result, dict) and "file_path" in result:
                    pii_class = result.get("pii_class")
                    if pii_class:
                        entity_counts[pii_class] += 1
                        total_entities += 1
                        logger.info(f"Detected PII: {pii_class}, Score: {result.get('score')}, Country: {result.get('country_of_origin')}")

        # Check if json_result structured is a dictionary
        elif isinstance(json_result, dict):
            for _, data in json_result.items():
                if isinstance(data, dict) and isinstance(data.get("results"), list):
                    for result in data["results"]:
                        detected_entities = result.get("entity_detected", [])
                        if isinstance(detected_entities, list):
                            for entity in detected_entities:
                                entity_type = entity.get("type")
                                if entity_type:
                                    entity_counts[entity_type] += 1
                                    total_entities += 1

        # Prepare NER results
        if entity_counts:
            highest_label = max(entity_counts.items(), key=lambda x: x[1])[0]
            confidence_score = round(max(entity_counts.values()) / total_entities, 2)
            ner_results = {
                'highest_label': highest_label,
                'confidence_score': confidence_score,
                'detected_entities': dict(entity_counts)
            }
        else:
            ner_results = {
                'highest_label': "NA",
                'confidence_score': 0.00,
                'detected_entities': {"NA"}
            }

        logger.info(f"NER results: {ner_results}")

        return ner_results
    except Exception as e:
        logger.error(f"Error processing file {file_name}--{file_extension}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error during NER processing: {str(e)}")

@router.post("/process-files/")
async def process_multiple_files(
    customer_id: int = Form(...),
    files: List[UploadFile] = File(...)
) -> Dict[str, Any]:
    """
    Processes multiple uploaded files using the PII scanner.
    """
    # Validate file extensions
    if not files:
        logger.error("No files uploaded.")
        raise HTTPException(status_code=400, detail="No files uploaded.")
    
    for file in files:
        file_extension = file.filename.lower().split('.')[-1]
        if file_extension not in STRUCTURED_FILE_FORMATS and file_extension not in UNSTRUCTURED_FILE_FORMATS:
            allowed_types = ', '.join(STRUCTURED_FILE_FORMATS + UNSTRUCTURED_FILE_FORMATS)
            logger.error(f"Unsupported file type '.{file_extension}'. Allowed types: {allowed_types}")
            raise HTTPException(status_code=400, detail=f"Unsupported file type '.{file_extension}'.")

    temp_dir = "/tmp/pii_scanner"
    os.makedirs(temp_dir, exist_ok=True)
    all_final_results = []
    file_names = []

    try:
        for file in files:
            file_path = await save_file(file, temp_dir)
            file_extension = file.filename.lower().split('.')[-1]
            file_name = file.filename

            try:
                all_scan_results = {}
                logger.info(f"Processing file: {file_name} with extension: {file_extension}")
                save_result = await process_instant_classifier_files(file_path, file_extension, file_name)

                if save_result:
                    all_scan_results["file_name"] = file_name
                    all_scan_results["file_extension"] = file_extension
                    all_scan_results["file_size"] = get_human_readable_size(file_path)
                    all_scan_results["results"] = save_result
                    file_names.append(file_name)
                    logger.info(f"File {file_name} processed successfully.")
                else:
                    all_scan_results[file_name] = {"error": "No entities detected."}
                    logger.warning(f"No entities detected in file {file_name}")

                all_final_results.append(all_scan_results)
            
            except Exception as e:
                all_scan_results[file_name] = {"error": str(e)}
                logger.error(f"Error processing file {file_name}: {str(e)}")
            
            finally:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Temporary file {file_name} removed.")

    except Exception as e:
        logger.error(f"An error occurred while processing files: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred while processing files: {str(e)}")

    # Save results to ClickHouse asynchronously
    await save_instant_data(customer_id, file_names, all_final_results)

    logger.info(f"Files processed successfully for customer {customer_id}: {file_names}")

    return {
        "message": "Files processed successfully",
        "customer_id": customer_id,
        "files_processed": file_names,
        "scan_results": all_final_results
    }

async def save_instant_data(customer_id: int, file_names: List[str], all_final_results: List[Dict[str, Any]]) -> None:
    """
    Saves the processed scan results to a ClickHouse database.
    """
    client = Connection.client
    try:
        json_output = json.dumps(all_final_results)
        current_time = datetime.now()
        await asyncio.to_thread(client.insert, 
            'instant_classifier',
            [(
                str(uuid4()),    
                customer_id,      
                file_names,       
                json_output,      
                current_time,     
                current_time      
            )],
            column_names=['id', 'customer_id', 'list_of_files', 'json_output', 'created_at', 'updated_at']
        )
        logger.info("Data saved successfully to ClickHouse.")
    finally:
        client.close()