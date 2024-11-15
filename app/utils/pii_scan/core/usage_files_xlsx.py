import logging
from typing import Dict, Any, Optional, Union, List
from app.utils.pii_scan.file_readers.xlsx_file import read_all_sheets, extract_column_data, clean_data
from app.utils.pii_scan.structured_ner_main import MLBasedNERScannerForStructuredData  # Adjust import according to your actual module

# Setup logging
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_column_data(column_data: List[str], scanner: MLBasedNERScannerForStructuredData, chunk_size: int, sample_size: Optional[Union[int, float]]) -> Dict[str, Any]:

    return scanner.scan(column_data, chunk_size=chunk_size, sample_size=sample_size)

def process_sheet(sheet_data: Dict[str, Any], sheet_name: str, columns_to_process: List[str], chunk_size: int, sample_size: Optional[Union[int, float]]) -> Dict[str, Any]:
    scanner = MLBasedNERScannerForStructuredData()  # Initialize your NER Scanner
    sheet_results = {}

    for col in columns_to_process:
        logger.info(f"Processing column: {col} in sheet: {sheet_name}")
        column_data = extract_column_data(sheet_data, col)
        cleaned_data = clean_data(column_data)
        result = process_column_data(cleaned_data, scanner, chunk_size, sample_size)
        sheet_results[col] = result

    return sheet_results

def xlsx_file_pii_detector(file_path: str, sheet_name: Optional[str] = None, column_name: Optional[str] = None, sample_size: Optional[Union[int, float]] = None, chunk_size: int = 100) -> Dict[str, Any]:
    """
    Main function to detect PII in an Excel file.
    """
    try:
        # Read all sheets from the Excel file
        all_sheets_data = read_all_sheets(file_path)
        logger.info(f"Successfully read file: {file_path}")

        # Check if any sheet data is empty
        if not all_sheets_data:
            logger.warning("No data found in the file.")
            return {"error": "No data found in the file."}

        # Determine sheet(s) to process
        if sheet_name:
            if sheet_name not in all_sheets_data:
                error_message = f"Error: The sheet '{sheet_name}' was not found in the file."
                logger.error(error_message)
                return {"error": error_message}
            sheets_to_process = {sheet_name: all_sheets_data[sheet_name]}
        else:
            sheets_to_process = all_sheets_data  # Process all sheets if no specific sheet is specified

        # Process each sheet sequentially
        results = {}
        for sheet, data in sheets_to_process.items():
            # Determine columns to process
            columns_to_process = [column_name] if column_name and column_name in data[0].keys() else list(data[0].keys())
            results[sheet] = process_sheet(data, sheet_name=None, columns_to_process=columns_to_process, chunk_size=chunk_size, sample_size=sample_size)

        logger.info("Processing completed successfully.")
        return results

    except FileNotFoundError:
        error_message = f"Error: The file '{file_path}' was not found."
        logger.error(error_message)
        return {"error": error_message}
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return {"error": str(e)}

