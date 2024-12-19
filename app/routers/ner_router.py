from fastapi import APIRouter, UploadFile, HTTPException
from app.utils.csv_processor import process_csv
from app.utils.ner_scanner.ner_scanner import NERScanner
import clickhouse_connect
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

scanner = NERScanner()

@router.post("/process/{table_id}")
async def predict_ner(table_id: str, file: UploadFile):
    """
    Predicts Named Entities for each column in the provided CSV data and updates the results in ClickHouse.
    """
    
    # Validate inputs
    if not table_id:
        raise HTTPException(status_code=400, detail="Missing required parameter: table_id")
    
    # Process CSV file
    data = process_csv_data(file)
    
    # Process NER for each column and handle ClickHouse update
    save_result = process_and_update_ner_results(table_id, data)

    if not save_result:
        raise HTTPException(status_code=500, detail=f"Failed to save or update NER data for table_id: {table_id}")

    return {"message": "Data uploaded and processed successfully", "details": save_result}


def process_csv_data(file: UploadFile):
    """
    Process and validate the uploaded CSV file.
    """
    try:
        # Assume process_csv handles column-wise processing and returns a dictionary where keys are column names
        data = process_csv(file, sep="~")  # Assuming this function is asynchronous
        if data.empty:
            raise ValueError("No valid data found in CSV")
        return data
    except Exception as e:
        logger.error(f"Error processing CSV file: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error processing CSV file: {str(e)}")


def process_and_update_ner_results(table_id: str, data: dict):
    """
    Process NER on each column in the CSV data and update the results in ClickHouse.
    """
    print(f"------------------------------{table_id}")
    try:
        # Iterate over each column in the data
        for column_name, column_data in data.items():
            # Process NER for the column data
            print(f"------------------------------{table_id}-----------{column_name}")
            json = scanner.scan(sample_data_rows=column_data, chunk_size=1000)
            if json:
                ner_results = json.get("highest_label", None)
            else:
                json = 'NA'
                ner_results = 'NA'
            # Save or update the NER results in ClickHouse
            logger.info(f"Data process for coulumn {column_name}")
            print(f"------------------------------{json}--")
            print(f"------------------------------{ner_results}")
            update_result = update_entity_for_column(table_id, column_name, ner_results, json)
            
            if not update_result:
                logger.error(f"Failed to save NER results for table_id: {table_id}, column: {column_name}")
                return False

        return True

    except Exception as e:
        logger.error(f"Error processing NER: {str(e)}")
        return False


def update_entity_for_column(table_id: str, column_name: str, ner_result: str, json: dict):
    """
    Save or update the processed NER result in ClickHouse based on table_id and column_name.
    """
    try:
        # Connect to ClickHouse
        client = clickhouse_connect.get_client(host='clickhouse-server', username='default', password='')

        # Delete existing records for the given table_id and column_name
        client.command(f"""
            ALTER TABLE column_ner_results DELETE WHERE table_id = '{table_id}' AND column_name = '{column_name}'
        """)

        # Insert the NER result into the ClickHouse table (no id, letting ClickHouse auto-generate it)
        client.insert(
            'column_ner_results',  # Table name as the first argument
            [(table_id, column_name, str(json), ner_result)],  # Data rows as a list of tuples
            column_names=['table_id', 'column_name', 'json', 'detected_entity']  # Column names as a separate argument
        )

        logger.info(f"Successfully inserted/updated NER result for table_id: {table_id}, column_name: {column_name}")
        return True

    except Exception as e:
        logger.error(f"Error inserting/updating data in ClickHouse: {str(e)}")
        return False
