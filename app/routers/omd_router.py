from fastapi import APIRouter, UploadFile, HTTPException
from app.utils.clickhouse_service import save_omd_table_data
from app.utils.csv_processor import process_csv
from app.constants.omd_db_entity import ALLOWED_OMD_DB_ENTITY
import os
router = APIRouter()

@router.post("/upload/{entity_type}")
async def upload_data(entity_type: str, file: UploadFile):
    # Validate entity_type
    if entity_type not in ALLOWED_OMD_DB_ENTITY:
        raise HTTPException(status_code=400, detail="Invalid entity type")

    # Process the CSV file
    data = process_csv(file)
    # Send the data to ClickHouse
    result = save_omd_table_data(entity_type, data)
    return {"message": "Data uploaded successfully", "details": {"entity_type": entity_type, "result": result}}