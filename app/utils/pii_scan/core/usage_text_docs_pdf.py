import json
import logging
from typing import Dict, Any, Optional, Union
from unstructured.partition.auto import partition
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from app.utils.pii_scan.unstructured_ner_main import MLBasedNERScannerForUnStructuredData
import nltk


# Ensure you have NLTK stopwords downloaded
nltk.download('punkt')
nltk.download('stopwords')

# Setup logging
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def preprocess_text(text: str) -> str:
    """
    Clean and preprocess text by removing stopwords, unnecessary characters, and extra spaces.

    Args:
        text (str): Raw text to be cleaned.

    Returns:
        str: Preprocessed text.
    """
    # Strip leading and trailing whitespace
    text = text.strip()
    
    # Tokenize text
    tokens = word_tokenize(text)
    
    # Remove stopwords
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word.lower() not in stop_words]
    
    # Reconstruct text
    preprocessed_text = ' '.join(filtered_tokens)
    
    return preprocessed_text

def format_results_as_json(results: Dict[str, Any]) -> str:
    """
    Format the NER scan results as a JSON string.

    Args:
        results (Dict[str, Any]): NER scan results.

    Returns:
        str: JSON-formatted string of the results.
    """
    formatted_results = {}

    # Extracting entities and their details from the results
    for entity_type, analysis in results.get('entities', {}).items():
        formatted_results[entity_type] = {
            'score': analysis.score,
            'appearances': analysis.appearances
        }

    # Extracting sensitivity results
    sensitivity_results = results.get('sensitivity', {})
    
    # Combine results into final structure
    final_result = {
        'entities': formatted_results,
        'sensitivity': sensitivity_results
    }
    
    return json.dumps(final_result, indent=4)

def file_pii_detector(file_path :str, sample_size: Optional[Union[int, float]] = None, chunk_size: int = 100) -> str:
    """
    Detect PII in DOCX, PDF, or TXT files using NER Scanner.

    Args:
        
        sample_size (Optional[Union[int, float]]): Sample size for processing.
        chunk_size (int): Chunk size for processing.

    Returns:
        str: JSON-formatted string of NER scan results.
    """
    scanner = MLBasedNERScannerForUnStructuredData()  # Initialize your NER Scanner

    try:
        # Extract elements from the document using Unstructured
        
        elements = partition(filename = file_path)
        logger.info(f"Successfully read and partitioned file DOCX/ PDF / TEXT .")

        # Preprocess each element's text and concatenate all preprocessed texts into one string
        texts = [preprocess_text(element.text) for element in elements if hasattr(element, 'text')]
        
        combined_text = ' '.join(texts)  # Join all preprocessed texts into one string

        # Log the number of elements and the combined string
        logger.info(f"Preprocessed {len(texts)} elements. Combined text: {combined_text}")

        # Perform NER scan on the entire list of preprocessed texts
        results = scanner.scan([combined_text])
        
        logger.info("Processing completed successfully.")
        return results

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return json.dumps({"error": str(e)}, indent=4)

