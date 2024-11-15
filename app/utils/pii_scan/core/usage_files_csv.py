import json
import logging
from typing import Dict, Any, Optional, Union, List
from app.utils.pii_scan.structured_ner_main import MLBasedNERScannerForStructuredData
from app.utils.pii_scan.file_readers.csv_file import read_csv, extract_column_data, clean_data

# Setup logging
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_column_data(column_data: List[str], column_name: str, scanner: MLBasedNERScannerForStructuredData, chunk_size: int, sample_size: Optional[Union[int, float]]) -> Dict[str, Any]:
    """
    Process column data using NER Scanner.

    Args:
        column_data (List[str]): List of column data.
        column_name (str): Name of the column.
        scanner (MLBasedNERScannerForStructuredData): NER Scanner instance.
        chunk_size (int): Size of data chunks for processing.
        sample_size (Optional[Union[int, float]]): Sample size as an integer or float (percentage).

    Returns:
        Dict[str, Any]: NER scan result for the column.
    """
    return scanner.scan(column_data, chunk_size=chunk_size, sample_size=sample_size)

def csv_file_pii_detector(file_path: str, column_name: Optional[str] = None, sample_size: Optional[Union[int, float]] = None, chunk_size: int = 100) -> Dict[str, Any]:
    """
    """
    scanner = MLBasedNERScannerForStructuredData()  # Initialize your NER Scanner

    try:
        # Read CSV file
        data = read_csv(file_path)
        logger.info(f"Successfully read file: {file_path}")

        # Check if data is empty
        if not data:
            logger.warning("No data found in the file.")
            return {"error": "No data found in the file."}

        # Initialize a dictionary to store results
        results = {}

        # Process the specified column or all columns if not specified
        columns_to_process = [column_name] if column_name and column_name in data[0] else list(data[0].keys())

        # Process each column sequentially
        for col in columns_to_process:
            logger.info(f"Processing column: {col}")
            try:
                column_data = clean_data(extract_column_data(data, col))
                result = process_column_data(column_data, col, scanner, chunk_size, sample_size)
                results[col] = result
            except Exception as e:
                logger.error(f"Error processing column: {col} - {e}")
                results[col] = {"error": str(e)}

        logger.info("Processing completed successfully.")
        return results

    except FileNotFoundError:
        error_message = f"Error: The file '{file_path}' was not found."
        logger.error(error_message)
        return {"error": error_message}
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return {"error": str(e)}

# Uncomment for example usage
# if __name__ == "__main__":
#     # Example usage with parameters
#     file_path = 'Fruit.csv'  # Change this to your file path
#     column_name = None       # Set to None to process all columns
#     sample_size = 0.20      # Set sample size as an integer or float (percentage)
    
#     # Run the main function
#     result = csv_file_pii_detector(file_path, column_name, sample_size)
#     print(json.dumps(result, indent=4))
