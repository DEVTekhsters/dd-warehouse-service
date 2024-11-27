import os
import json
import shutil
import logging
from pathlib import Path
from minio import Minio
from dotenv import load_dotenv
from client_connect import Connection
from fastapi import APIRouter, HTTPException, File, BackgroundTasks
from app.utils.pii_scan.verify_pii.pii_scanner import PIIScanner
from app.utils.pii_scan.verify_pii.check_digit_pii import Verify_PII_Digit
from pydantic import BaseModel


# Setup logging
logger = logging.getLogger(__name__)
load_dotenv()

# Define the temp folder path
TEMP_FOLDER = Path(__file__).resolve().parent.parent / 'utils/pii_scan/temp_files'
if not TEMP_FOLDER.exists():
    TEMP_FOLDER.mkdir(parents=True, exist_ok=True)

# Initialize FastAPI router and logger
router = APIRouter()

# Pydantic model to receive metadata from the request
class DataReceived(BaseModel):
    source_type: str
    source_bucket: str  # This is the folder name (not the bucket)
    region: str
    message: str

MINIO_URL = os.getenv("MINIO_URL")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"  # Ensure it’s a boolean


# Initialize MinIO client
minio_client = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE  # This will automatically be a boolean
    )

# FastAPI route to process unstructured files in the background
@router.post("/process_unstructured")
async def process_unstructured_files(data_received: DataReceived, background_tasks: BackgroundTasks):
    bucket_name = MINIO_BUCKET_NAME # The actual bucket name
    folder_name = data_received.source_bucket  # Folder name is provided by the user (e.g., "c762bd76-7fd2-4d8c-812a-a8eae6debce1")

    # Start the background task to process files
    background_tasks.add_task(process_files_from_minio, bucket_name, folder_name, data_received)

    return {"message": "Files are being processed in the background."}

# Function to process files one by one from MinIO
async def process_files_from_minio(bucket_name: str, folder_name: str, data_received: DataReceived):
    """Fetch files from the given folder in MinIO, process them with the NER model, and delete them."""
    try:
        # List objects (files) under the folder in MinIO
        objects = minio_client.list_objects(bucket_name, prefix=folder_name + "/", recursive=True)

        # Process each object (file) one by one
        for obj in objects:
            
            
            file_name = obj.object_name
            logger.info(f"Processing file: {file_name}")

            # Download the file to the local temp directory
            temp_file_path = TEMP_FOLDER / file_name.split("/")[-1]  # Saving with the same file name

            # Download the file from MinIO to the temporary folder
            minio_client.fget_object(bucket_name, file_name, str(temp_file_path))

            # Process the file (call NER model)
            try:
                await process_ner_for_file(temp_file_path, data_received)
                # If processing is successful, remove the file from MinIO
                minio_client.remove_object(bucket_name, file_name)
                logger.info(f"Successfully deleted file: {file_name} from MinIO.")
            except Exception as ner_error:
                logger.error(f"NER processing failed for file {file_name}: {str(ner_error)}")
                continue  # Continue processing next files

            # Remove the local temp file
            if temp_file_path.exists():
                os.remove(temp_file_path)
                logger.info(f"Deleted local temp file: {temp_file_path}")

        # # After processing all files, you can delete the folder itself (if needed).
        
        # await remove_folder_if_empty(bucket_name, folder_name)
        

    except Exception as e:
        logger.error(f"Error during file processing: {e}")
        raise HTTPException(status_code=500, detail=f"Error during file processing: {str(e)}")
    
   
async def remove_folder_if_empty(bucket_name: str, folder_name: str):
    """Remove the folder from MinIO if it's empty (i.e., no objects with that prefix)."""
    try:
        # List objects under the given folder (prefix)
        objects = list(minio_client.list_objects(bucket_name, prefix=folder_name + "/", recursive=True))

        # Check if there are any objects under the folder
        if not objects:
            # No objects found, the folder is empty. We can safely remove the folder.
            logger.info(f"The folder '{folder_name}' is empty. Removing the folder prefix from the bucket.")
            # Remove the folder prefix (this just means no objects with the prefix remain)
            minio_client.remove_object(bucket_name, folder_name)
            logger.info(f"Successfully removed the folder prefix '{folder_name}' from the bucket.")
        else:
            # Objects are still present, so we cannot remove the folder yet
            logger.info(f"The folder '{folder_name}' is not empty. Skipping deletion.")

    except Exception as e:
        logger.error(f"Error during folder deletion check: {e}")
        raise HTTPException(status_code=500, detail=f"Error checking folder {folder_name} for deletion: {str(e)}")


# Process the file with the NER model (your existing model logic)
async def process_ner_for_file(file_path: Path, data_received: DataReceived):
    """Process the file with the NER model and PII scanner."""
    file_name = file_path.name
    file_size = file_path.stat().st_size
    file_type = file_name.split(".")[-1].upper()

    logger.info(f"Processing file: {file_name}, Size: {file_size} bytes, Type: {file_type}")

    try:
        # Initialize the PII scanner
        scanner = PIIScanner()
        result = scanner.main(file_path, sample_size=0.2, chunk_size=500)

        if not result:
            logger.error(f"No PII detected in the file {file_name}. Skipping further processing.")
            raise ValueError("No PII data detected in the file.")

        # Prepare metadata for the processed file
        metadata = {
            "source_bucket": data_received.source_bucket,
            "file_name": file_name,
            "file_size": file_size,
            "file_type": file_type,
            "source": data_received.source_type,
            "region": data_received.region,
        }

        # Verify PII results
        check = Verify_PII_Digit()
        check_pii_results = check.verify(result=result, file_type=file_type)

        # Save results to the database (ClickHouse)
        save_unstructured_ner_data(result, metadata)

        return {"message": "File processed successfully", "pii_results": result, "metadata": metadata, "check_pii_result": check_pii_results}

    except Exception as e:
        logger.error(f"Error processing file {file_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error during NER processing: {str(e)}")


# Save NER results to ClickHouse database (example)
def save_unstructured_ner_data(result, metadata):
    """Save the processed NER results into the database (e.g., ClickHouse)."""
    if not result:
        logger.error("No PII data detected in the file.")
        raise ValueError("No PII data detected")

    # Convert the results to JSON
    pii_results_json = json.dumps(result)

    # Insert data into the database (example query)
    data_to_insert = {
        "source_bucket": metadata.get("source_bucket"),
        "file_name": metadata.get("file_name"),
        "json": pii_results_json,
        "file_size": metadata.get("file_size"),
        "file_type": metadata.get("file_type"),
        "source": metadata.get("source"),
        "region": metadata.get("region"),
    }

    # Database connection and insertion logic here
    connection = Connection()  # Example connection object (replace with your actual connection logic)
    client = connection.client

    try:
        insert_query = """
        INSERT INTO ner_unstructured_data (source_bucket, file_name, json, file_size, file_type, source, region)
        VALUES (%(source_bucket)s, %(file_name)s, %(json)s, %(file_size)s, %(file_type)s, %(source)s, %(region)s)
        """
        client.command(insert_query, data_to_insert)
        logger.info("Successfully inserted data into the ner_unstructured_data table.")
    except Exception as e:
        logger.error(f"Error inserting data into the database: {e}")
        raise HTTPException(status_code=500, detail="Error inserting data into the database")
