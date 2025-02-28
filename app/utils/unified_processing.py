from collections import defaultdict
from pathlib import Path
import os
import logging
import json
from fastapi import HTTPException
from pii_scanner.scanner import PIIScanner
from pii_scanner.constants.patterns_countries import Regions
from app.utils.common_utils import BaseFileProcessor

logger = logging.getLogger(__name__)

# Define the temporary folder path for storing files
TEMP_FOLDER = Path(__file__).resolve().parent.parent / 'utils/temp_files'
TEMP_FOLDER.mkdir(parents=True, exist_ok=True)

class UnifiedProcessor(BaseFileProcessor):
    def __init__(self):
        self.scanner = PIIScanner()

    # async def process_and_update_ner_results(self, table_id: str = None, file: UploadFile = None, file_path: Path = None, profiler: str = None, metadata: dict = None):
    async def process_and_update_ner_results(self, **kwargs):
        """
        Processes NER results for both structured and unstructured data.
        Structured data (CSV, XLS, XLSX) updates ClickHouse per column.
        Unstructured data (TXT, DOC, PDFs, Images) updates ClickHouse with a generic entity detection.
        """
            # Extract known parameters from kwargs

        table_id = kwargs.get('table_id', None)  # Optional
        file = kwargs.get('file', None)           # Optional
        file_path = kwargs.get('file_path', None) # Required for processing
        profiler = kwargs.get('profiler', None)   # Optional
        metadata = kwargs.get('metadata', None)   # Optional

        entity_counts = defaultdict(int)
        total_entities = 0
        highest_label = "NA"
        ner_results = {'highest_label': "NA", 'confidence_score': 0.00, 'detected_entities': {"NA": 0}}
        try:
            # Determine input source: file upload (structured) vs file path (unstructured)
            if profiler == "structured":
                temp_file_path = TEMP_FOLDER / file.filename.split("/")[-1]
                with temp_file_path.open("wb") as buffer:
                    buffer.write(await file.read())
                file_path = temp_file_path
                file_name = file.filename
                file_type = file.filename.rsplit(".", 1)[-1].lower()

            elif profiler =="unstructured":
                file_type = metadata.get("file_type")
                file_name = metadata.get("file_name")

            file_size_mb = file_path.stat().st_size / (1024 * 1024)  # Convert bytes to MB
            sample_size = self.determine_sample_size(file_size_mb) if file_type.lower() in ["csv", "xls", "xlsx"] else 0.2
            
            json_result = await self.scanner.scan(str(file_path), sample_size=sample_size, region=Regions.IN)
            if not json_result:
                logger.warning(f"No PII detected in file {file_name} ({file_type}).")
                return False

            # Processing structured data
            if isinstance(json_result, dict):
                if profiler == "structured":
                    await self.process_structured_data(json_result, table_id, entity_counts, total_entities)
                elif profiler == "unstructured":
                    print(json_result)
                    await self.process_grouped_data(json_result, file_name, entity_counts, total_entities, metadata)

            # Processing unstructured data (list format)
            elif isinstance(json_result, list):
                await self.process_unstructured_data(json_result, file_name, entity_counts, total_entities, metadata)

            return True

        except Exception as e:
            logger.error(f"Error processing NER results for {file_name} {file_type.upper()}: {str(e)}")
            return False

        finally:
            if file and temp_file_path.exists():
                os.remove(temp_file_path)

    async def process_structured_data(self, json_result, table_id, entity_counts, total_entities):
        """Process structured data from the NER results."""
        for column_name, column_data in json_result.items():
            if not column_data:
                logger.warning(f"No data for column: {column_name}")
                continue
            
            for result in column_data.get("results", []):
                for entity in result.get("entity_detected", []):
                    entity_type = entity.get("type")
                    if entity_type:
                        entity_counts[entity_type] += 1
                        total_entities += 1
            
            highest_label, confidence_score = self.calculate_ner_scores(entity_counts, total_entities)
            ner_results = self.prepare_ner_results(entity_counts, highest_label, confidence_score)

            if table_id and highest_label != "NA":
                updated_ner_results = self.pii_filter(ner_results, column_name)
                if updated_ner_results:
                    data_element = await self.fetch_data_element_category(updated_ner_results.get("highest_label", "NA"))
                    update_result = await self.update_entity_for_column(table_id, column_name, updated_ner_results, updated_ner_results["highest_label"], data_element)
                    if not update_result:
                        logger.error(f"Failed to save NER results for table_id: {table_id}")

    async def process_grouped_data(self, json_result, file_name, entity_counts, total_entities, metadata):
        """Process grouped data from the NER results."""
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

        highest_label, confidence_score = self.calculate_ner_scores(entity_counts, total_entities)
        ner_results = self.prepare_ner_results(entity_counts, highest_label, confidence_score)

        if highest_label != "NA":
            updated_ner_results = self.pii_filter(ner_results, file_name)
            if updated_ner_results:
                data_element = await self.fetch_data_element_category(updated_ner_results.get("highest_label", "NA"))
                update_result = await self.save_unstructured_ner_data(updated_ner_results, data_element, updated_ner_results["highest_label"] , metadata)
                if not update_result:
                    logger.error(f"Failed to save NER results for file: {file_name}")

    async def process_unstructured_data(self, json_result, file_name, entity_counts, total_entities, metadata):
        """Process unstructured data from the NER results."""
        for result in json_result:
            detected_entities = result.get("entity_detected", []) if isinstance(result, dict) else []
            for entity in detected_entities:
                entity_type = entity.get("type")
                if entity_type:
                    entity_counts[entity_type] += 1
                    total_entities += 1
            
            if "file_path" in result and "pii_class" in result:
                entity_counts[result["pii_class"]] += 1
                total_entities += 1

        highest_label, confidence_score = self.calculate_ner_scores(entity_counts, total_entities)
        ner_results = self.prepare_ner_results(entity_counts, highest_label, confidence_score)

        if highest_label != "NA":
            updated_ner_results = self.pii_filter(ner_results, file_name)
            if updated_ner_results:
                data_element = await self.fetch_data_element_category(updated_ner_results.get("highest_label", "NA"))
                update_result = await self.save_unstructured_ner_data(updated_ner_results, data_element, updated_ner_results["highest_label"] , metadata)
                if not update_result:
                    logger.error(f"Failed to save NER results for file: {file_name}")

    def determine_sample_size(self, file_size_mb: float) -> float:
        """Determines the appropriate sample size percentage based on file size."""
        if 5 <= file_size_mb <= 10:
            return 0.05
        elif file_size_mb <= 30:
            return 0.004
        elif file_size_mb <= 50:
            return 0.003
        elif file_size_mb >= 80:
            return 0.002
        else:
            return 0.2

    def calculate_ner_scores(self, entity_counts, total_entities):
        """Calculates highest detected entity and confidence score."""
        if entity_counts:
            highest_label = max(entity_counts.items(), key=lambda x: x[1])[0]
            confidence_score = round(max(entity_counts.values()) / total_entities, 2)
        else:
            highest_label = "NA"
            confidence_score = 0.00
        return highest_label, confidence_score

    def prepare_ner_results(self, entity_counts, highest_label, confidence_score):
        """Prepares the NER results dictionary."""
        return {
            'highest_label': highest_label,
            'confidence_score': confidence_score,
            'detected_entities': dict(entity_counts) if entity_counts else {"NA": 0}
        }

    async def update_entity_for_column(self, table_id: str, column_name: str, updated_ner_results, detected_entity: str, data_element) -> bool:
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
                "json": json.dumps(updated_ner_results),
                "detected_entity": detected_entity,
                "data_element": data_element
            }
            client.command(query, params)
            logger.info(f"Successfully inserted/updated NER result for table_id: '{table_id}', column: '{column_name}'")
            return True
        except Exception as e:
            logger.error(f"Error inserting/updating data in ClickHouse: {str(e)}")
            return False

    async def save_unstructured_ner_data(self, ner_results, data_element, detected_entity, metadata,):
        """
        Saves the NER results and associated metadata into the ClickHouse database.
        """
        if not ner_results:
            logger.error("No PII data detected in the file.")
            raise ValueError("No PII data detected")
        ner_results_json = json.dumps(ner_results)

        data_to_insert = {
            "source_bucket": metadata.get("source_bucket"),
            "file_name": metadata.get("file_name"),
            "json": ner_results_json,
            "detected_entity": detected_entity,
            "data_element": data_element,
            "file_size": metadata.get("file_size"),
            "file_type": metadata.get("file_type"),
            "source": metadata.get("source"),
            "sub_service": metadata.get("sub_service"),
            "region": metadata.get("region")
        }
        client = self.get_clickhouse_client()
        try:
            insert_query = """
            INSERT INTO ner_unstructured_data 
            (source_bucket, file_name, json, detected_entity, data_element, file_size, file_type, source, sub_service, region)
            VALUES 
            (%(source_bucket)s, %(file_name)s, %(json)s, %(detected_entity)s, %(data_element)s, %(file_size)s, %(file_type)s, %(source)s, %(sub_service)s, %(region)s)
            """
            client.command(insert_query, data_to_insert)
            logger.info("Successfully inserted data into the ner_unstructured_data table.")
            return True
        except Exception as e:
            logger.error(f"Error inserting data into the database: {e}")
            raise HTTPException(status_code=500, detail="Error inserting data into the database")