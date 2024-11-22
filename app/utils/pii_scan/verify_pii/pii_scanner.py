import os
import logging
from pathlib import Path
from app.utils.pii_scan.core.usage_files_csv import csv_file_pii_detector
from app.utils.pii_scan.Octopii.octopii_pii_detector import process_file_octopii
from app.utils.pii_scan.core.usage_text_docs_pdf import file_pii_detector
from app.utils.pii_scan.core.usage_files_json import json_file_pii_detector
from app.utils.pii_scan.core.usage_files_xlsx import xlsx_file_pii_detector
from app.utils.pii_scan.structured_ner_main import MLBasedNERScannerForStructuredData
from fastapi import HTTPException

# Setup logging
logger = logging.getLogger(__name__)

class PIIScanner:
    def __init__(self):
        """
        Initializes the PII Scanner with the necessary configuration for structured data scanning.
        """
        self.scanner = MLBasedNERScannerForStructuredData()  # Initialize NER Scanner for structured data scanning

    def files_data_pii_scanner(self, file_path: str, sample_size=0.2, chunk_size=1000):
        """
        Determine the file type based on its extension and call the appropriate PII detection function.
        
        Args:
            file_path (str): Path to the file to be scanned.
            sample_size (float): Proportion of data to sample from the file.
            chunk_size (int): Number of rows or data units to process at once.
        
        Returns:
            result (dict): PII detection result.
        """
        if isinstance(file_path, Path):
            file_path = str(file_path)
            
        file_extension = os.path.splitext(file_path)[1].lower()
        
        try:
            if file_extension == '.csv':
                result = csv_file_pii_detector(file_path, column_name=None, sample_size=sample_size, chunk_size=chunk_size)
                logger.info("CSV File PII Scanner Results:")
                return result
            elif file_extension in ['.jpg', '.jpeg', '.png']:
                result = process_file_octopii(file_path)
                logger.info("Image File PII Scanner Results:")
                return result
            elif file_extension in ['.txt', '.pdf', '.docx']:
                result = file_pii_detector(file_path, sample_size=sample_size)
                logger.info("Text/PDF/Docx File PII Scanner Results:")
                return result
            elif file_extension == '.json':
                result = json_file_pii_detector(file_path, column_name=None, sample_size=sample_size, chunk_size=chunk_size)
                logger.info("JSON File PII Scanner Results:")
                return result
            elif file_extension == '.xlsx':
                result = xlsx_file_pii_detector(file_path, sheet_name='Sheet1', column_name=None, sample_size=sample_size, chunk_size=chunk_size)
                logger.info("XLSX File PII Scanner Results:")
                return result
            else:
                raise Exception(f"Unsupported file type: {file_extension}")
        except Exception as e:
            logger.error(f"Error processing file: {e}")
            raise HTTPException(status_code=500, detail=f"Error processing file: {e}")

    def main(self, file_path=None, data=None, sample_size=0.2, chunk_size=1000, column_name="password"):
        """
        Process data or file for PII detection.
        
        Args:
            file_path (str, optional): Path to the file to be scanned (optional if data is provided).
            data (list, optional): List of data to be scanned for PII (optional if file_path is provided).
            sample_size (float): Proportion of data to sample from the file.
            chunk_size (int): Size of chunks to process at a time.
            column_name (str): Name of the column to scan for PII (for structured data).
        
        Returns:
            result (dict): PII detection results.
        """
        if data:
            logger.info("Scanning provided data for PII...")
            return self.column_data_pii_scanner(data, column_name, chunk_size)
        elif file_path:
            logger.info(f"Scanning file: {file_path}")
            return self.files_data_pii_scanner(file_path, sample_size, chunk_size)
        else:
            logger.warning("No data or file path provided for scanning.")
