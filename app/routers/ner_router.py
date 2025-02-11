from fastapi import APIRouter, UploadFile, HTTPException
import logging
from dotenv import load_dotenv
from app.utils.ner_clickhouse_service import OmdFileProcesser

# Load environment variables
load_dotenv()

# Setup logging
logger = logging.getLogger(__name__)

# Initialize FastAPI router
router = APIRouter()

# Initialize the file processor instance
processor = OmdFileProcesser()

@router.post("/process/{table_id}")
async def predict_ner(table_id: str, file: UploadFile):
    """
    Endpoint to process an uploaded file and perform NER, then update the results in ClickHouse.
    """
    if not table_id:
        raise HTTPException(status_code=400, detail="Missing required parameter: table_id")
    
    # Extract file extension
    file_extension = file.filename.split(".")[-1]
    
    # Process the file based on its extension using the processor instance
    data = await processor.process_file_data(file, file_extension)
    
    # Process NER and update results in ClickHouse
    save_result = await processor.process_and_update_ner_results(table_id, data)
    
    if not save_result:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to save or update NER data for table_id: {table_id}"
        )
    
    return {"message": "Data uploaded and processed successfully", "details": save_result}
