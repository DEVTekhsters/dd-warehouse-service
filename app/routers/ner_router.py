from fastapi import APIRouter, UploadFile, HTTPException
import logging
import os
from dotenv import load_dotenv
from app.utils.unified_processing import UnifiedProcessor
from pii_scanner.scanner import PIIScanner
from pii_scanner.constants.patterns_countries import Regions

# Load environment variables
load_dotenv()

# Setup logging
logger = logging.getLogger(__name__)


# Load structured file formats from environment variables
STRUCTURED_FILE_FORMATS = os.getenv("STRUCTURED_FILE_FORMATS", "").split(',')

# Initialize FastAPI router
router = APIRouter()

# Initialize the file processor instance
processor = UnifiedProcessor()

@router.post("/process/{table_id}")
async def predict_ner(table_id: str, file: UploadFile):
    """
    Endpoint to process an uploaded file and perform NER, then update the results in ClickHouse.
    """
    if not table_id:
        raise HTTPException(status_code=400, detail="Missing required parameter: table_id")
    
    file_extension = file.filename.rsplit(".", 1)[-1].lower()
    if file_extension not in STRUCTURED_FILE_FORMATS:
        raise HTTPException(status_code=400, detail=f"Unsupported file format: {file_extension}")
    
    save_result = await processor.process_and_update_ner_results(table_id=table_id, file=file,profiler="structured")
    
    if not save_result:
        raise HTTPException(status_code=500, detail=f"Failed to save or update NER data for table_id: {table_id}")
    
    return {"message": "Data uploaded and processed successfully", "details": save_result}
