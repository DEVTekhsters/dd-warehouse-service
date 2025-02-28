import os
import logging
from pathlib import Path
from minio import Minio
from dotenv import load_dotenv
from fastapi import HTTPException
from app.utils.unified_processing import UnifiedProcessor


logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Define the temporary folder path for storing files
TEMP_FOLDER = Path(__file__).resolve().parent.parent / 'utils/temp_files'
if not TEMP_FOLDER.exists():
    TEMP_FOLDER.mkdir(parents=True, exist_ok=True)

class UnstructuredFileProcessor(UnifiedProcessor):
    """
    A class to encapsulate processing of unstructured files, applying NER and updating results in ClickHouse.
    """
    def __init__(self):
        super().__init__()
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
                temp_file_path_t = None  # Initialize temp_file_path_t

                logger.info(f"Processing file: {file_name}")
                if file_extension not in self.UNSTRUCTURED_FILE_FORMATS and file_extension not in self.STRUCTURED_FILE_FORMATS:
                    logger.warning(f"File {file_name} has an unsupported format. Skipping it.")
                    continue

                temp_file_path_t = TEMP_FOLDER / file_name.split("/")[-1]
                self.minio_client.fget_object(bucket_name, file_name, str(temp_file_path_t))

                try:
                    await self.process_ner_for_file(temp_file_path_t, data_received)
                    self.minio_client.remove_object(bucket_name, file_name)
                    logger.info(f"Successfully deleted file: {file_name} from MinIO.")
                except Exception as ner_error:
                    logger.error(f"NER processing failed for file {file_name}: {str(ner_error)}")

                    if temp_file_path_t.exists():
                        os.remove(temp_file_path_t)
                        logger.info(f"Deleted local temp file: {temp_file_path_t}")
                    continue

                if temp_file_path_t.exists():
                    os.remove(temp_file_path_t)
                    logger.info(f"Deleted local temp file: {temp_file_path_t}")

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
        
        # Updating the aws region to countries
        region = self.aws_region_update(data_received.region)
        
        metadata = {
            "source_bucket": data_received.source_bucket,
            "file_name": file_name,
            "file_size": file_size,
            "file_type": file_type,
            "source": source,
            "sub_service": sub_service,
            "region": region,
        }
        try:
            results = await self.process_and_update_ner_results(
                    file_path=file_path,
                    metadata=metadata,
                    profiler="unstructured"
                )
            if not results:
                logger.error("No NER results detected.")
            logger.info(f"PII results processed for file {file_name}")
            return {"message": "File processed successfully", "metadata": metadata}
        except Exception as e:
            logger.error(f"Error processing file {file_name}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error during NER processing: {str(e)}")
