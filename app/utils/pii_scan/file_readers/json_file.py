import json
from typing import List, Dict, Any
from app.utils.pii_scan.structured_ner_main import MLBasedNERScannerForStructuredData  # Adjust import according to your actual module

def read_json(file_path: str) -> List[Dict[str, Any]]:
    """
    Read a JSON file and return its content as a list of dictionaries.
    """
    with open(file_path, 'r', encoding='utf-8') as jsonfile:
        return json.load(jsonfile)

def extract_column_data(data: List[Dict[str, Any]], column_name: str) -> List[str]:
    """
    Extract data from a specific column, including nested objects.
    """
    def extract_from_dict(d: Dict[str, Any], column_name: str) -> List[str]:
        """
        Recursively extract data from a nested dictionary.
        """
        results = []
        for key, value in d.items():
            if key == column_name:
                if isinstance(value, str):
                    results.append(value)
                elif isinstance(value, (int, float)):
                    results.append(str(value))
            elif isinstance(value, dict):
                results.extend(extract_from_dict(value, column_name))
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        results.extend(extract_from_dict(item, column_name))
        return results

    all_results = []
    for entry in data:
        if isinstance(entry, dict):
            all_results.extend(extract_from_dict(entry, column_name))
        elif isinstance(entry, list):
            for item in entry:
                if isinstance(item, dict):
                    all_results.extend(extract_from_dict(item, column_name))
    return all_results

def clean_data(data: List[Any]) -> List[str]:
    """
    Clean the data by ensuring each item is a string,
    removing leading/trailing spaces, and filtering out empty strings.
    """
    cleaned_data = []
    for item in data:
        if isinstance(item, str):
            cleaned_item = item.strip()
            if cleaned_item:
                cleaned_data.append(cleaned_item)
        elif isinstance(item, (int, float)):
            cleaned_item = str(item).strip()
            if cleaned_item:
                cleaned_data.append(cleaned_item)
    return cleaned_data

def process_column_data(column_data: List[str], column_name: str, scanner: MLBasedNERScannerForStructuredData, chunk_size: int = 100) -> Dict[str, Any]:
    """
    Process column data using NER Scanner.
    """
    return scanner.scan(column_data, column_name=column_name, chunk_size=chunk_size, sample_size=100)
