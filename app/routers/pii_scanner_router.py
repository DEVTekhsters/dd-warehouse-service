 
from fastapi import APIRouter, UploadFile, HTTPException
import os
import shutil
import time
import json
import clickhouse_connect
from typing import Dict, Any, List, Union
import asyncio
from enum import Enum
import asyncio
import time
import io
from fastapi import Form, File, UploadFile
from typing import List
from uuid import uuid4
from datetime import datetime
from pii_scanner.scanner import PIIScanner
from pii_scanner.constants.patterns_countries import Regions
 
 
router = APIRouter()
 
 
def normalize_scan_results(results: Union[Dict, List], filename: str) -> Dict:
    """
    Normalizes scan results from different file types into a consistent format.
    """
    file_ext = filename.lower().split('.')[-1]
    
    if isinstance(results, dict):
        # For CSV and JSON files that already return dict
        return {"Sheet1": results}
    
    if file_ext in ['xlsx', 'xls']:
        # Results are already properly structured for Excel files
        return results
    
    if file_ext in ['pdf', 'doc', 'docx', 'txt']:
        # For unstructured text files, create a normalized structure
        normalized = {
            "Sheet1": {
                "Content": {
                    "results": []
                }
            }
        }
        
        # Convert list results to the expected format
        for item in results:
            if isinstance(item, dict) and 'entity_detected' in item:
                normalized["Sheet1"]["Content"]["results"].append(item)
            else:
                # Handle any other unexpected formats
                normalized["Sheet1"]["Content"]["results"].append({
                    "entity_detected": [],
                    "check_digit": False
                })
        
        return normalized
    
    # Default case - return empty structure
    return {"Sheet1": {}}
 
def extract_entities(column_results: Dict) -> List[str]:
    """
    Extracts unique entity types from scan results.
    """
    detected_entities = []
    
    if isinstance(column_results, dict) and 'results' in column_results:
        for result in column_results['results']:
            if 'entity_detected' in result:
                for entity in result['entity_detected']:
                    if 'type' in entity:
                        detected_entities.append(entity['type'])
    
    return detected_entities
 
 
 
@router.post("/process-files/")
async def process_multiple_files(
    customer_id: int = Form(...),
    files: List[UploadFile] = File(...)
):
    """
    Processes multiple uploaded files using the PII scanner and saves results to ClickHouse.
    Accepts customer_id and one or more files for scanning.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")
    ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'json', 'txt', 'pdf'}
    for file in files:
        file_extension = file.filename.lower().split('.')[-1]
        if file_extension not in ALLOWED_EXTENSIONS:
            raise HTTPException(
                status_code=400,
                detail=f"File type '.{file_extension}' is not supported. Allowed file types are: {', '.join(ALLOWED_EXTENSIONS)}"
            )
    try:
        temp_dir = "/tmp/pii_scanner"
        os.makedirs(temp_dir, exist_ok=True)
        all_scan_results = {}
        file_names = []
        start_time = time.time()
        for file in files:
            temp_file_path = os.path.join(temp_dir, file.filename)
            file_names.append(file.filename)
            try:
                with open(temp_file_path, "wb") as temp_file:
                    content = await file.read()
                    temp_file.write(content)
                    await file.seek(0)  
                pii_scanner = PIIScanner()
                results = await pii_scanner.scan(
                    file_path=temp_file_path,
                    sample_size=50,
                    region=Regions.IN
                )
                normalized_results = normalize_scan_results(results, file.filename)
                all_scan_results[file.filename] = normalized_results
            except Exception as file_error:
                print(f"Error processing file {file.filename}: {str(file_error)}")
                all_scan_results[file.filename] = {
                    "error": str(file_error)
                }
            finally:
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
        client = clickhouse_connect.get_client(
            host='148.113.6.50',
            port="8123",
            username='default',
            password='',
            database='default'
        )
        try:
            json_output = json.dumps(all_scan_results)
            current_time = datetime.now()
            client.insert(
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
        finally:
            client.close()
        execution_time = time.time() - start_time
        return {
            "message": "Files processed successfully",
            "customer_id": customer_id,
            "files_processed": file_names,
            "execution_time": execution_time,
            "scan_results": all_scan_results
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error during file processing: {str(e)}"
        )
 
 
