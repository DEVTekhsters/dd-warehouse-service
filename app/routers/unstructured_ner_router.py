import os
import json
import shutil
import logging
from dotenv import load_dotenv
from pathlib import Path
from client_connect import Connection
from fastapi import APIRouter, HTTPException, File, BackgroundTasks
from app.utils.pii_scan.verify_pii.pii_scanner import PIIScanner
from app.utils.pii_scan.verify_pii.check_digit_pii import Verify_PII_Digit
from pydantic import BaseModel

# Setup logging
logger = logging.getLogger(__name__)

# Define the temp folder path
TEMP_FOLDER = Path(__file__).resolve().parent.parent / 'utils/pii_scan/temp_files'
if not TEMP_FOLDER.exists():
    TEMP_FOLDER.mkdir(parents=True, exist_ok=True)

# Initialize FastAPI router and logger
router = APIRouter()

class DataReceived(BaseModel):
    source_type: str
    source_bucket: str
    region: str
    message: str

# FastAPI route to process unstructured files in the background
@router.post("/process_unstructured")
async def process_unstructured_files(data_received: DataReceived, background_tasks: BackgroundTasks):
    # for server
    parent_directory_path = os.path.abspath("data_discovery_files")
    # for local machine
    #parent_directory_path = os.path.abspath("../data_discovery_files")

    data = {
        "source_bucket": data_received.source_bucket,
        "source_type": data_received.source_type,
        "region": data_received.region
    }

    processor = FileProcessor(parent_directory_path, data_received.source_bucket, data)

    # Add file processing to the background
    background_tasks.add_task(processor.process_files)

    return {"message": "Files are being processed in the background."}


# File processing logic within FileProcessor  and running PII scanner, Verify_PII_Digit, (making it async)
class FileProcessor:
    def __init__(self, directory_path: str, source_bucket: str, webhook_metadata: dict):
        self.directory_path = Path(directory_path)
        self.source_bucket = source_bucket
        self.webhook_metadata = webhook_metadata
        # Setup logger
        self.logger = logging.getLogger(__name__)

    async def process_files(self):
        """Asynchronously process all files in the directory."""
        if not self.directory_path.exists():
            self.logger.error(f"Directory {self.directory_path} does not exist.")
            return

        source_bucket_path = self.directory_path / self.source_bucket
        if not source_bucket_path.exists() or not source_bucket_path.is_dir():
            self.logger.error(f"Source bucket directory {self.source_bucket} does not exist or is not a directory.")
            return

        files_to_process = list(source_bucket_path.glob('*'))
        if not files_to_process:
            self.logger.info(f"No files to process in {self.source_bucket} directory.")
            return

        for file_item in files_to_process:
            if file_item.is_file():
                self.logger.info(f"Processing file: {file_item.name}")
                await self.predict_ner_unstructured(data=self.webhook_metadata, file=file_item)

                try:
                    os.remove(file_item)
                    self.logger.info(f"Successfully deleted file: {file_item.name}")
                except Exception as e:
                    self.logger.error(f"Failed to delete file {file_item.name}: {e}")

        if not any(source_bucket_path.iterdir()):
            try:
                source_bucket_path.rmdir()
                self.logger.info(f"Removed empty directory: {source_bucket_path}")
            except Exception as error:
                self.logger.error(f"Failed to remove empty directory {source_bucket_path}: {error}")

    async def predict_ner_unstructured(self, data: dict, file: Path):
        """Async method to handle NER prediction on unstructured files."""
        file_name = file.name
        file_size = file.stat().st_size
        file_type = file_name.split(".")[-1].upper()

        self.logger.info(f"File uploaded: {file_name}, Size: {file_size} bytes, Type: {file_type}")

        temp_file_path = TEMP_FOLDER / file_name
        try:
            # Save the file to the temp folder
            with open(temp_file_path, "wb") as temp_file:
                with open(file, "rb") as f:
                    shutil.copyfileobj(f, temp_file)

            # Initialize the PII scanner
            scanner = PIIScanner()
            result = scanner.main(temp_file_path, sample_size=0.2, chunk_size=500)

            metadata = {
                "source_bucket": data.get("source_bucket", ""),
                "file_name": file_name,
                "file_size": file_size,
                "file_type": file_type,
                "source": data.get("source_type", ""),
                "region": data.get("region", ""),
            }

            check = Verify_PII_Digit()
            check_pii_results = check.verify(result=result, file_type=file_type)

            save_unstructured_ner_data(result, metadata)  # Save data to ClickHouse
            return {"message": "File processed successfully", "pii_results": result, "metadata": metadata, "check_pii_result": check_pii_results}

        except Exception as e:
            self.logger.error(f"Error processing file: {e}")
            raise HTTPException(status_code=500, detail="Error during PII scanning")

        finally:
            if temp_file_path.exists():
                os.remove(temp_file_path)

# Save PII scan results to database
def save_unstructured_ner_data(result, metadata):
    if not result:
        logger.error("No PII data detected in the file.")
        raise ValueError("No PII data detected")

    pii_results_json = json.dumps(result)

    data_to_insert = {
        "source_bucket": metadata.get("source_bucket"),
        "file_name": metadata.get("file_name"),
        "json": pii_results_json,
        "file_size": metadata.get("file_size"),
        "file_type": metadata.get("file_type"),
        "source": metadata.get("source"),
        "region": metadata.get("region"),
    }

    connection = Connection()
    client = connection.client

    try:
        insert_query = """
        INSERT INTO ner_unstructured_data (source_bucket, file_name, json, file_size, file_type, source, region)
        VALUES (%(source_bucket)s, %(file_name)s, %(json)s, %(file_size)s, %(file_type)s, %(source)s, %(region)s)
        """
        client.command(insert_query, data_to_insert)
        logger.info("Successfully inserted data into the -ner_unstructured- table.")

    except Exception as e:
        logger.error(f"Error inserting data into ClickHouse: {e}")
        raise HTTPException(status_code=500, detail="Error inserting data into the database")
