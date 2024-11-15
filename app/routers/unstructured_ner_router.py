import os
import json
import shutil
import logging
from pathlib import Path
from fastapi import APIRouter, UploadFile, HTTPException
from app.utils.pii_scan.verify_pii.pii_scanner import PIIScanner
from app.utils.pii_scan.verify_pii.check_pii import Verify_PII_Digit
from client_connect import Connection


# Define the temp folder path
TEMP_FOLDER = Path(__file__).resolve().parent.parent / 'utils/pii_scan/temp_files'

# Initialize FastAPI router and logger
router = APIRouter()



# Define the path where you want to store your logs
LOG_FOLDER = Path(__file__).resolve().parent.parent / 'logs'

# Make sure the directory exists, create it if not
if not LOG_FOLDER.exists():
    LOG_FOLDER.mkdir(parents=True, exist_ok=True)

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,  # Log level INFO will capture INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Format for log messages
    handlers=[
        logging.FileHandler(LOG_FOLDER / 'app.log'),  # Log to 'app.log' inside the 'logs' folder
        logging.StreamHandler()  # Log to the console as well
    ]
)

# Create a logger instance
logger = logging.getLogger(__name__)


# FastAPI route to process unstructured data (e.g., PDFs, DOCs)
@router.post("/process_unstructured/{source_bucket}")
async def predict_ner_unstructured(source_bucket: str, file: UploadFile):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file uploaded")
    # Extract metadata
    file_name = file.filename
    file_size = len(file.file.read())  # Read the file to get the size in bytes
    file_type = file_name.split(".")[-1].upper() # MIME type of the file
    
     # Log the metadata information
    logger.info(f"File uploaded: {file_name}, Size: {file_size} bytes, Type: {file_type}")

    # Rewind the file pointer to the beginning (important for further file processing)
    file.file.seek(0)
    # Save the uploaded file temporarily
    temp_file_path = TEMP_FOLDER / file.filename
    

    try:
        # Save the file to the temp folder
        with open(temp_file_path, "wb") as temp_file:
            shutil.copyfileobj(file.file, temp_file)

        # Initialize the PII scanner
        scanner = PIIScanner()

        # Process the file using the PII scanner
        
        result = scanner.main(temp_file_path, sample_size=0.2, chunk_size=500)
        metadata = {
            "source_bucket": source_bucket,
            "file_name": file_name,
            "file_size": file_size,
            "file_type": file_type,
            "source": "source", # CHANGE WHEN API starts getiing 
            "region": "region", # CHANGE WHEN API starts getiing 
            
        }

        # Validate the authenticity of the results based on the digits returned from the PII entities
        check = Verify_PII_Digit()
        check_pii_results = check.verify(result=result,file_type=file_type)
        # Return the results (Adding to clickhouse)
        # save_unstructured_ner_data(result, metadata)
        return {"message": "File processed successfully", "pii_results": result, "metadata":metadata, "check_pii_result":check_pii_results}

    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise HTTPException(status_code=500, detail=f"Error during PII scanning")

    finally:
        # Cleanup: remove the temporary file after processing
        if temp_file_path.exists():
            os.remove(temp_file_path)


def save_unstructured_ner_data(result, metadata):
    # Check if result is empty
    if not result:
        logger.error("No PII data detected in the file.")
        raise ValueError("No PII data detected")
    
    # Convert PII results to a JSON string for easy storage
    pii_results_json = json.dumps(result)  # Serialize PII results into JSON string
    
    # Create the payload to insert into the database
    data_to_insert = {
        "source_bucket": metadata.get("source_bucket"),  # Use file name as a unique identifier or generate a UUID
        "file_name": metadata.get("file_name"),
        "json": pii_results_json,  # Store PII results as JSON string
        "file_size": metadata.get("file_size"),
        "file_type": metadata.get("file_type"),
        "source": "AWS", # CHANGE WHEN API starts getiing 
        "region": "South-AS", # CHANGE WHEN API starts getiing 

        
    }

    #  Connect to ClickHouse  
    connection = Connection()
    client = connection.client
    

    # Insert data into the ner_unstructured table
    try:
        insert_query = """
        INSERT INTO ner_unstructured_data (source_bucket, file_name, json, file_size, file_type, source, region  )
        VALUES
        (%(source_bucket)s, %(file_name)s, %(json)s, %(file_size)s, %(file_type)s,%(source)s,%(region)s)
        """
        # Execute the insert
        client.command(insert_query, data_to_insert)
        logger.info("Successfully inserted data into the -ner_unstructured- table.")
    
    except Exception as e:
        logger.error(f"Error inserting data into ClickHouse: {e}")
        raise HTTPException(status_code=500, detail="Error inserting data into the database")

