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

def process_csv_data(file_path: str) -> Dict:
    """
    Processes a CSV file from a file path into a dictionary structure.
    """
    try:
        data = pd.read_csv(file_path, sep=";")
        if data.empty:
            raise ValueError("No valid data found in CSV")
        
        column_data = {col: [str(value) for value in data[col]] for col in data.columns}
        logger.info(f"Processed CSV file {file_path} with columns: {list(data.columns)}")
        return column_data
    except Exception as e:
        logger.error(f"Error processing CSV file '{file_path}': {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error processing CSV file: {str(e)}")

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

async def process_file(file_path: str, file_extension: str) -> Dict:
    """
    Processes the file based on its extension (CSV, PDF, etc.) and performs PII scanning.
    """
    logger.info(f"Processing file: {file_path} with extension: {file_extension}")
    
    if file_extension in ['xlsx', 'xls', 'csv', 'json']:
        data = process_csv_data(file_path)
        return await process_and_update_ner_results(data)
    
    elif file_extension in ['pdf', 'txt', 'doc', 'docx']:
        return await process_unstructured_file(file_path)
    
    return {"error": "Unsupported file format."}

async def process_unstructured_file(file_path: str) -> Dict:
    """
    Scans unstructured files (e.g., PDF, TXT) for PII data.
    """
    try:
        logger.info(f"Scanning unstructured file: {file_path}")
        result = await pii_scanner.scan(file_path, sample_size=0.2, region=Regions.IN)
        if result:
            entity_types = list({
                entity['type']
                for item in result
                for entity in item['entity_detected']
            })
            logger.info(f"Entities detected in unstructured file: {entity_types}")
            return {"entity_types": entity_types}
        return {"entity_types": []}
    except Exception as e:
        logger.error(f"Error processing unstructured file '{file_path}': {str(e)}")
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
    allowed_extensions = {'csv', 'xlsx', 'xls', 'json', 'txt', 'pdf'}
    
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
