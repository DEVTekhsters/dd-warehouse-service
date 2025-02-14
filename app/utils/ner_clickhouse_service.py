from fastapi import UploadFile, HTTPException
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
from pii_scanner.scanner import PIIScanner
from pii_scanner.constants.patterns_countries import Regions
from app.utils.common_utils import BaseFileProcessor

# Load environment variables
load_dotenv()

# Setup logging
logger = logging.getLogger(__name__)

# Load structured file formats from environment variables
STRUCTURED_FILE_FORMATS = os.getenv("STRUCTURED_FILE_FORMATS", "").split(',')

class OmdFileProcesser(BaseFileProcessor):
    def __init__(self):
        self.pii_scanner = PIIScanner()

    async def process_file_data(self, file: UploadFile, file_extension: str) -> Dict:
        """
        Processes a file (CSV, Excel, JSON) into a dictionary structure.
        """
        if file_extension.lower() not in STRUCTURED_FILE_FORMATS:
            logger.error(f"Unsupported file format: {file_extension}")
            raise HTTPException(status_code=400, detail="Unsupported file format")

        file_content = await file.read()
        try:
            if file_extension.lower() == 'csv':
                content_str = file_content.decode("utf-8")
                # Auto-detect CSV delimiter
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(content_str).delimiter
                data = pd.read_csv(StringIO(content_str), sep=delimiter)
            elif file_extension.lower() in ['xlsx', 'xls']:
                data = pd.read_excel(file_content)
            elif file_extension.lower() == 'json':
                content_str = file_content.decode("utf-8")
                data = pd.read_json(StringIO(content_str))
            else:
                raise ValueError("Unsupported file format")

            if data.empty:
                raise ValueError(f"No valid data found in {file_extension.upper()} file")

            column_data = {col: [str(value) for value in data[col]] for col in data.columns}
            logger.info(f"Processed {file_extension.upper()} file '{file.filename}' with columns: {list(data.columns)}")
            return column_data

        except Exception as e:
            logger.error(f"Error processing {file_extension.upper()} file '{file.filename}': {str(e)}")
            raise HTTPException(status_code=400, detail=f"Error processing {file_extension.upper()} file: {str(e)}")

    async def process_and_update_ner_results(self, table_id: str, data: dict) -> bool:
        """
        Processes NER results for each column and updates the results in ClickHouse.
        """
        try:
            for column_name, column_data in data.items():
                # Perform NER scanning on the column data
                json_result = await self.pii_scanner.scan(data=column_data, sample_size=5, region=Regions.IN)
                entity_counts = defaultdict(int)
                total_entities = 0
                ner_results = 'NA'
                
                # Process the NER results
                if json_result:
                    for result in json_result.get("results", []):
                        if result.get("entity_detected"):
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
                            'detected_entities': dict(entity_counts)
                        }
                    else:
                        ner_results = {
                            'highest_label': "NA",
                            'confidence_score': 0.00,
                            'detected_entities': "{ NA }"
                        }

                logger.info(f"NER results for column '{column_name}': {ner_results}")
                
                # Determine the detected entity
                detected_entity = ner_results.get('highest_label') if isinstance(ner_results, dict) else 'NA'
                
                # Fetch additional details from ClickHouse
                data_element = await self.fetch_data_element_category(detected_entity)
                
                # Update ClickHouse with the NER results
                update_result = await self.update_entity_for_column(
                    table_id,
                    column_name,
                    ner_results,
                    detected_entity,
                    data_element
                )
                if not update_result:
                    logger.error(f"Failed to save NER results for table_id: {table_id}, column: {column_name}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Error processing NER results: {str(e)}")
            return False

    async def update_entity_for_column(
        self,
        table_id: str,
        column_name: str,
        ner_results,
        detected_entity: str,
        data_element
    ) -> bool:
        """
        Updates the NER result for a given column in ClickHouse.
        """
        try:
            client = self.get_clickhouse_client()
            query = """
                INSERT INTO column_ner_results 
                    (table_id, column_name, json, detected_entity, data_element)
                VALUES 
                    (%(table_id)s, %(column_name)s, %(json)s, %(detected_entity)s, %(data_element)s)
            """
            params = {
                "table_id": table_id,
                "column_name": column_name,
                "json": json.dumps(ner_results),
                "detected_entity": detected_entity,
                "data_element": data_element
                
            }
            client.command(query, params)
            logger.info(f"Successfully inserted/updated NER result for table_id: '{table_id}', column: '{column_name}'")
            return True
        except Exception as e:
            logger.error(f"Error inserting/updating data in ClickHouse: {str(e)}")
            return False
