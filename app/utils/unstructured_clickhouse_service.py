import os
import json
import logging
from pathlib import Path
from collections import defaultdict

from minio import Minio
from dotenv import load_dotenv
from fastapi import HTTPException
import nltk

from client_connect import Connection
from pii_scanner.scanner import PIIScanner
from pii_scanner.constants.patterns_countries import Regions


# Download necessary NLTK resources for natural language processing
nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger_eng')

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Define the temporary folder path for storing files
TEMP_FOLDER = Path(__file__).resolve().parent.parent / 'utils/pii_scan/temp_files'
if not TEMP_FOLDER.exists():
    TEMP_FOLDER.mkdir(parents=True, exist_ok=True)

class UnstructuredFileProcessor:
    """
    A class to encapsulate processing of unstructured files, applying NER and updating results in ClickHouse.
    """
    def __init__(self):
        # MinIO configuration for object storage
        self.MINIO_URL = os.getenv("MINIO_URL")
        self.MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
        self.MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
        self.MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

        # Define supported file formats
        self.UNSTRUCTURED_FILE_FORMATS = os.getenv("UNSTRUCTURED_FILE_FORMATS").split(',')
        self.STRUCTURED_FILE_FORMATS = os.getenv("STRUCTURED_FILE_FORMATS").split(',')

        # Initialize MinIO client
        self.minio_client = Minio(
            self.MINIO_URL,
            access_key=self.MINIO_ACCESS_KEY,
            secret_key=self.MINIO_SECRET_KEY,
            secure=self.MINIO_SECURE
        )

        # Initialize the PII scanner
        self.scanner = PIIScanner()

    @staticmethod
    def get_clickhouse_client():
        """Returns the ClickHouse client for database operations."""
        return Connection.client

    async def process_files_from_minio(self, bucket_name: str, folder_name: str, data_received):
        """
        Lists and processes files from a specified MinIO bucket and folder.
        Downloads each file, processes it with NER, and then deletes it from MinIO.
        """
        try:
            objects = self.minio_client.list_objects(bucket_name, prefix=f"{folder_name}/", recursive=True)
            for obj in objects:
                file_name = obj.object_name
                file_extension = file_name.split(".")[-1].lower()

                logger.info(f"Processing file: {file_name}")
                if file_extension not in self.UNSTRUCTURED_FILE_FORMATS and file_extension not in self.STRUCTURED_FILE_FORMATS:
                    logger.warning(f"File {file_name} has an unsupported format. Skipping it.")
                    continue

                temp_file_path = TEMP_FOLDER / file_name.split("/")[-1]
                self.minio_client.fget_object(bucket_name, file_name, str(temp_file_path))

                try:
                    await self.process_ner_for_file(temp_file_path, data_received)
                    self.minio_client.remove_object(bucket_name, file_name)
                    logger.info(f"Successfully deleted file: {file_name} from MinIO.")
                except Exception as ner_error:
                    logger.error(f"NER processing failed for file {file_name}: {str(ner_error)}")
                    continue

                if temp_file_path.exists():
                    os.remove(temp_file_path)
                    logger.info(f"Deleted local temp file: {temp_file_path}")

        except Exception as e:
            logger.error(f"Error during file processing: {e}")
            raise HTTPException(status_code=500, detail=f"Error during file processing: {str(e)}")

    async def process_ner_for_file(self, file_path: Path, data_received):
        """
        Processes a file with NER.
        Extracts metadata from the file and delegates processing to the NER functions.
        """
        file_name = file_path.name
        file_size = file_path.stat().st_size
        file_type = file_name.split(".")[-1].upper()

        logger.info(f"Processing file: {file_name}, Size: {file_size} bytes, Type: {file_type}")

        # Extract source and sub-service from the provided data
        source_parts = data_received.source_type.split(',')
        source = source_parts[0].strip() if source_parts else "N/A"
        sub_service = source_parts[1].strip() if len(source_parts) > 1 else "N/A"

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
            results = await self.process_and_update_ner_results_unstructured(file_path, file_type, file_name, metadata)
            if not results:
                logger.error("No NER results detected.")
            logger.info(f"PII results processed for file {file_name}")
            return {"message": "File processed successfully", "metadata": metadata}
        except Exception as e:
            logger.error(f"Error processing file {file_name}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error during NER processing: {str(e)}")

    async def process_and_update_ner_results_unstructured(self, file_path: Path, file_type: str, file_name: str, metadata: dict):
        """
        Applies the NER scanner on the file and updates the results in ClickHouse.
        """
        entity_counts = defaultdict(int)
        total_entities = 0
        ner_results = 'NA'

        try:
            json_result = await self.scanner.scan(str(file_path), sample_size=0.2, region=Regions.IN)
            if not json_result:
                logger.error(f"No PII detected in the file {file_name} ({file_type}).")
                raise ValueError("No PII data detected in the file.")

            if isinstance(json_result, list):
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
            elif isinstance(json_result, dict):
                for category, data in json_result.items():
                    if isinstance(data, dict) and isinstance(data.get("results"), list):
                        for result in data["results"]:
                            detected_entities = result.get("entity_detected", [])
                            if isinstance(detected_entities, list):
                                for entity in detected_entities:
                                    entity_type = entity.get("type")
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
                    'detected_entities': {"NA"}
                }

            data_element = await self.fetch_data_element_category(highest_label)

            logger.info(f"NER results: {ner_results}")

            self.save_unstructured_ner_data(ner_results, metadata, data_element, highest_label)

            return True

        except Exception as e:
            logger.error(f"Error processing file {file_name}--{file_type}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error during NER processing: {str(e)}")

    async def fetch_data_element_category(self, detected_entity: str):
        """
        Fetches the data element category from ClickHouse for a given detected entity.
        If the category does not exist, it adds 'UNKNOWN' as the parameter name
        and the detected entity as the parameter value, ensuring no duplicates.
        """
        try:
            client = self.get_clickhouse_client()
            
            # Query to fetch the category based on the detected entity
            data_element_query = f"""SELECT parameter_name FROM data_element WHERE parameter_value = '{detected_entity}';"""
            result = client.query(data_element_query)
            
            if result.result_rows:
                category = result.result_rows[0][0]
                logger.info(f"Data element category for '{detected_entity}': {category}")
                return category
            else:
                logger.info(f"No data element category found for '{detected_entity}'. Checking 'UNKNOWN' category.")
                
                # Check for existing 'UNKNOWN' category
                check_unknown_query = f"SELECT parameter_value FROM data_element WHERE parameter_name = 'UNKNOWN' AND parameter_value = '{detected_entity}';"
                unknown_result = client.query(check_unknown_query)
                
                if unknown_result.result_rows:
                    logger.info(f"'{detected_entity}' already exists in 'UNKNOWN' category. No action needed.")
                else:
                    # Insert into 'UNKNOWN' category without specifying the id
                    insert_unknown_query = f"""INSERT INTO data_element (parameter_name, parameter_value, parameter_sensitivity) 
                                                VALUES ('UNKNOWN', '{detected_entity}','UNKNOWN');"""
                    client.command(insert_unknown_query)
                    logger.info(f"Added '{detected_entity}' to 'UNKNOWN' category.")
                return "UNKNOWN"
        except Exception as e:
            logger.error(f"Error fetching data element category from ClickHouse: {str(e)}")
            return f"Error: {str(e)}"

    def save_unstructured_ner_data(self, ner_results, metadata, data_element, detected_entity):
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

        connection = Connection()
        client = connection.client
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
