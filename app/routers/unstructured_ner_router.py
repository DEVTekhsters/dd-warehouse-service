from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel
import os
from app.utils.unstructured_clickhouse_service import UnstructuredFileProcessor

router = APIRouter()
processor = UnstructuredFileProcessor()

class DataReceived(BaseModel):
    source_type: str
    source_bucket: str
    region: str
    message: str

@router.post("/process_unstructured")
async def process_unstructured_files(data_received: DataReceived, background_tasks: BackgroundTasks):
    bucket_name = os.getenv("MINIO_BUCKET_NAME")
    folder_name = data_received.source_bucket
    background_tasks.add_task(processor.process_files_from_minio, bucket_name, folder_name, data_received)
    
    return {"results": f"Processing started for the folder: {folder_name}"}
