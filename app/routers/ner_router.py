from fastapi import APIRouter, UploadFile, HTTPException
from pii_scanner.scanner import PIIScanner
from pii_scanner.constants.patterns_countries import Regions
import clickhouse_connect
import logging
import tempfile
import os
from datetime import datetime
import json
from collections import defaultdict
import uuid
from typing import Dict, List, Union
router = APIRouter()
logger = logging.getLogger(__name__)
 
pii_scanner = PIIScanner()
 
 
 
def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host='148.113.6.50',
        port="8123",
        username='default',
        password='',
        database='default'
    )
 
@router.post("/process/{table_id}")
async def scan_pii_data(table_id: str, file: UploadFile):
    client = None
    try:
        current_time = datetime.now()
        temp_dir = "/tmp/pii_scanner"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file_path = os.path.join(temp_dir, file.filename)
 
        # Save uploaded file
        with open(temp_file_path, "wb") as temp_file:
            content = await file.read()
            temp_file.write(content)
            await file.seek(0)
 
        logger.info(f"Starting PII scan for file: {temp_file_path}")
        
        # Perform the scan
        raw_results = await pii_scanner.scan(
            file_path=temp_file_path,
            sample_size=50,
            region=Regions.IN
        )
        
        logger.debug(f"Raw scan results: {raw_results}")  # Add this for debugging
        
        # Process results for ClickHouse storage
        if isinstance(raw_results, str):
            try:
                scan_results = json.loads(raw_results)
            except json.JSONDecodeError:
                scan_results = {"pii_entities": [{"column_name": "content", "entity": "text", "data": raw_results}]}
        elif isinstance(raw_results, list):
            scan_results = {"pii_entities": raw_results}
        elif isinstance(raw_results, dict):
            if "pii_entities" not in raw_results:
                scan_results = {"pii_entities": [raw_results]}
            else:
                scan_results = raw_results
 
        logger.debug(f"Processed scan results: {scan_results}")  # Add this for debugging
 
        # Get ClickHouse client
        try:
            client = get_clickhouse_client()
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Database connection failed: {str(e)}"
            )
 
        # Process and store PII entities in ClickHouse
        if "pii_entities" in scan_results and len(scan_results["pii_entities"]) > 0:
            for entity_group in scan_results["pii_entities"]:
                for column_name, column_data in entity_group.items():
                    try:
                        if "results" in column_data:
                            # Count entity types and find the most common one
                            entity_counts: Dict[str, int] = defaultdict(int)
                            total_entities = 0
                            
                            for result in column_data["results"]:
                                if "entity_detected" in result and result["entity_detected"]:
                                    entity_type = result["entity_detected"][0].get("type")
                                    if entity_type:
                                        entity_counts[entity_type] += 1
                                        total_entities += 1
                            
                            if entity_counts:
                                # Find highest occurring entity type
                                highest_label = max(entity_counts.items(), key=lambda x: x[1])[0]
                                
                                # Calculate confidence score
                                confidence_score = round(max(entity_counts.values()) / total_entities, 2)
                                
                                # Prepare JSON data
                                json_data = {
                                    'highest_label': highest_label,
                                    'confidence_score': confidence_score,
                                    'detected_entities': {k: v for k, v in entity_counts.items()}
                                }
                                
                                # Insert into ClickHouse using clickhouse_connect
                                data_dict = {
                                            'id': str(uuid.uuid4()),
                                        'table_id': table_id,
                                        'column_name': column_name,
                                        'json': json.dumps(json_data),
                                        'detected_entity': highest_label,
                                        'created_at': current_time,
                                        'updated_at': current_time
                                    }
                                data_values = [
                                        data_dict['id'],
                                        data_dict['table_id'],
                                        data_dict['column_name'],
                                        data_dict['json'],
                                        data_dict['detected_entity'],
                                        data_dict['created_at'],
                                        data_dict['updated_at']
                                    ]
 
                                logger.debug(f"Inserting data: {data_values}")  
                                
                                client.insert('column_ner_results',
                                            [data_values],
                                            column_names=['id', 'table_id', 'column_name', 'json',
                                                        'detected_entity', 'created_at', 'updated_at'])
                                
                                logger.info(f"Stored NER results for column: {column_name}")
                    except Exception as e:
                        logger.error(f"Error processing column {column_name}: {str(e)}")
                        raise HTTPException(
                            status_code=500,
                            detail=f"Error processing column {column_name}: {str(e)}"
                        )
 
        return {
            "status": "success",
            "message": "File scanned successfully",
            "table_id": table_id,
            "scan_results": scan_results,
            "entities_found": len(scan_results.get("pii_entities", []))
        }
 
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}", exc_info=True)  # Added exc_info=True
        raise HTTPException(
            status_code=500,
            detail=f"Error processing file: {str(e)}"
        )
    finally:
        if os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
                logger.info(f"Temporary file removed: {temp_file_path}")
            except Exception as e:
                logger.error(f"Error removing temporary file: {str(e)}")
        
        # Close ClickHouse connection
        if client:
            try:
                client.close()
            except Exception as e:
                logger.error(f"Error closing database connection: {str(e)}")
 
