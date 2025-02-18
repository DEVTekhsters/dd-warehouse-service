from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from typing import List, Dict
import os
import pandas as pd
from uuid import uuid4
from datetime import datetime
from collections import defaultdict
import asyncio
import clickhouse_connect
import json
import logging
from pii_scanner.scanner import PIIScanner
from pii_scanner.constants.patterns_countries import Regions

router = APIRouter()
pii_scanner = PIIScanner()

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

async def save_file(file: UploadFile, temp_dir: str):
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
        raise HTTPException(status_code=500, detail=f"Failed to save file '{file.filename}': {str(e)}")

async def process_and_update_ner_results(data: Dict) -> List[Dict]:
    """
    Scans structured data for PII entities and calculates confidence scores.
    """
    results = []
    try:
        for column_name, column_data in data.items():
            logger.info(f"Scanning column: {column_name}")
            json_result = await pii_scanner.scan(data=column_data, sample_size=5, region=Regions.IN)
            column_result = {'column': column_name}
            
            if json_result:
                entity_counts = defaultdict(int)
                for result in json_result.get("results", []):
                    if "entity_detected" in result and result["entity_detected"]:
                        for entity in result["entity_detected"]:
                            entity_type = entity.get("type")
                            if entity_type:
                                entity_counts[entity_type] += 1

                if entity_counts:
                    total_entities = sum(entity_counts.values())
                    highest_label = max(entity_counts.items(), key=lambda x: x[1])[0]
                    confidence_score = round(entity_counts[highest_label] / total_entities, 2)
                    column_result['highest_label'] = highest_label
                    column_result['confidence_score'] = confidence_score
                    column_result['detected_entities'] = dict(entity_counts)

            results.append(column_result)
            logger.info(f"Finished scanning column: {column_name}")
    except Exception as e:
        logger.error(f"Error processing NER results: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing NER results: {str(e)}")

    return results

def process_file_data(file_path: str, file_extension: str) -> Dict:
    """
    Processes a file based on its extension (CSV, Excel, JSON) into a dictionary structure.
    """
    try:
        if file_extension == 'csv':
            data = pd.read_csv(file_path, sep=";")
        elif file_extension in ['xlsx', 'xls']:
            data = pd.read_excel(file_path)
        elif file_extension == 'json':
            data = pd.read_json(file_path)
        else:
            raise ValueError("Unsupported file format")
        
        if data.empty:
            raise ValueError(f"No valid data found in {file_extension.upper()} file")

        column_data = {col: [str(value) for value in data[col]] for col in data.columns}
        logger.info(f"Processed {file_extension.upper()} file {file_path} with columns: {list(data.columns)}")
        return column_data
    except Exception as e:
        logger.error(f"Error processing {file_extension.upper()} file '{file_path}': {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error processing {file_extension.upper()} file: {str(e)}")

async def process_file(file_path: str, file_extension: str) -> Dict:
    """
    Processes the file based on its extension (CSV, PDF, Excel, JSON, etc.) and performs PII scanning.
    """
    logger.info(f"Processing file: {file_path} with extension: {file_extension}")
    
    if file_extension in ['csv', 'xlsx', 'xls', 'json']:
        data = process_file_data(file_path, file_extension)
        return await process_and_update_ner_results(data)
    
    elif file_extension in ['pdf', 'txt', 'doc', 'docx','jpg','jpeg','png']:
        return await process_unstructured_file(file_path)
    
    return {"error": "Unsupported file format."}


async def process_unstructured_file(file_path: str) -> Dict:
    """
    Scans unstructured files (e.g., PDF, TXT, DOCX , JPEG , JPG , PNG) for PII data.
    """
    try:
        logger.info(f"Starting scan for unstructured file: {file_path}")
        
        # Logging the size of the file for tracking
        file_size = os.path.getsize(file_path)
        file_extension = os.path.splitext(file_path)[1].lower().replace('.', '')
        logger.info(f"File size: {file_size} bytes")
        
        # Initiating the scan
        logger.info("Calling PII scanner with provided file.")
        result = await pii_scanner.scan(file_path, sample_size=0.2, region=Regions.IN)
        print("RESULT!!!!!!!",result)
        # Logging the raw result from the scanner
        logger.debug(f"Raw scan result: {result}")

        if result:
            if file_extension in {"jpg", "jpeg", "png"}:
        # Extracting and processing detected entities for image files
                processed_data = []
                for item in result:
                    entity_types = item.get("pii_class")
                    score = item.get("score")
                    country_of_origin = item.get("country_of_origin")
                    faces = item.get("faces")
                    identifiers = item.get("identifiers", [])
                    emails = item.get("emails", [])
                    phone_numbers = item.get("phone_numbers", [])
                    addresses = item.get("addresses", [])

                    processed_data.append({
                    "entity_types": entity_types,
                    "score": score,
                    "country_of_origin": country_of_origin,
                    "faces": faces,
                    "identifiers": identifiers,
                    "emails": emails,
                    "phone_numbers": phone_numbers,
                    "addresses": addresses,
                    })

                    logger.info(f"Entities detected in image file: {processed_data}")
                    return processed_data

            else:
            # Extracting and logging detected entity types for other document files
                entity_types = list({
                entity['type']
                for item in result
                if "entity_detected" in item
                for entity in item['entity_detected']
                })
                logger.info(f"Entities detected in unstructured file: {entity_types}")
                return {"entity_types": entity_types}

        # Logging if no entities are detected
        logger.warning("No entities detected in the unstructured file.")
        return {"entity_types": []}
    
    except Exception as e:
        # Logging error with traceback for deeper analysis
        logger.error(f"Error processing unstructured file '{file_path}': {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing unstructured file: {str(e)}")


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

@router.post("/process-files/")
async def process_multiple_files(
    customer_id: int = Form(...),
    files: List[UploadFile] = File(...)
):
    """
    Processes multiple uploaded files using the PII scanner.
    """
    allowed_extensions = {'csv', 'xlsx', 'xls', 'json', 'txt', 'pdf',"docx","doc","jpg","jpeg","png"}
    
    # Validate file extensions
    if not files:
        logger.error("No files uploaded.")
        raise HTTPException(status_code=400, detail="No files uploaded")
    
    for file in files:
        file_extension = file.filename.lower().split('.')[-1]
        if file_extension not in allowed_extensions:
            logger.error(f"Unsupported file type '.{file_extension}'. Allowed types: {', '.join(allowed_extensions)}")
            raise HTTPException(status_code=400, detail=f"Unsupported file type '.{file_extension}'. Allowed types: {', '.join(allowed_extensions)}")
    
    temp_dir = "/tmp/pii_scanner"
    os.makedirs(temp_dir, exist_ok=True)
    all_final_results = []
    file_names = []

    try:
        for file in files:
            file_path = await save_file(file, temp_dir)
            file_extension = file.filename.lower().split('.')[-1]
            
            try:
                all_scan_results = {}
                logger.info(f"Processing file: {file.filename} with extension: {file_extension}")
                save_result = await process_file(file_path, file_extension)

                if save_result:
                    all_scan_results["file_name"] = file.filename
                    all_scan_results["file_extension"] = file_extension
                    all_scan_results["file_size"] = get_human_readable_size(file_path)
                    all_scan_results["columns"] = save_result
                    file_names.append(file.filename)
                    logger.info(f"File {file.filename} processed successfully.")
                else:
                    all_scan_results[file.filename] = {"error": "No entities detected."}
                    logger.warning(f"No entities detected in file {file.filename}")

                all_final_results.append(all_scan_results)
            
            except Exception as e:
                all_scan_results[file.filename] = {"error": str(e)}
                logger.error(f"Error processing file {file.filename}: {str(e)}")
            
            finally:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Temporary file {file.filename} removed.")

    except Exception as e:
        logger.error(f"An error occurred while processing files: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred while processing files: {str(e)}")

    # Save results to ClickHouse asynchronously
    await save_instant_data(customer_id, file_names, all_final_results)

    logger.info(f"Files processed successfully for customer {customer_id}: {file_names}")

    return {
        "message": "Files processed successfully",
        "customer_id": customer_id,
        "files_processed": file_names,
        "scan_results": all_final_results
    }

async def save_instant_data(customer_id: int, file_names: List[str], all_final_results: Dict) -> None:
    """
    Saves the processed scan results to a ClickHouse database.
    """
    client = clickhouse_connect.get_client(
            host='148.113.6.50',
            port="8123",
            username='default',
            password='',
            database='default'
        )
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
