import logging
import os
import time  # Import time module for capturing timestamps
from typing import List, Dict
from gliner import GLiNER
from app.utils.pii_scan.spacy_ner import SpaCyNERProcessor

# Define the local directory path where the model will be saved
model_dir = "local_pii_model"  # Local directory to store the model
model_name = "urchade/gliner_multi_pii-v1"  # GLiNER model name or identifier

# Setup logging
logging.basicConfig(
    filename='app.log',
    level=logging.DEBUG,  # Set to DEBUG to capture detailed log messages
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def chunk_text(text: str, chunk_size: int = 1000) -> List[str]:
    """
    Chunk the text into smaller segments of a specified size.
    """
    return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

class MLBasedNERScannerForUnStructuredData:
    """
    A simple scanner using the GLiNER model for PII detection.
    """

    def __init__(self):
        self.model_path = model_dir
        self.model_name = model_name
        if not self._model_exists():
            logger.info("Model not found locally. Downloading model from Hugging Face.")
            self._download_model()
        self.model = GLiNER.from_pretrained(self.model_name)
        self.spacy_processor = SpaCyNERProcessor()

    def _model_exists(self) -> bool:
        """
        Check if the model files exist locally.
        """
        return os.path.isdir(self.model_path)

    def _download_model(self):
        """
        Download the model and tokenizer from Hugging Face and save to local directory.
        """
        os.makedirs(self.model_path, exist_ok=True)
        # Download the GLiNER model
        GLiNER.from_pretrained(self.model_name, cache_dir=self.model_path)
        logger.info(f"Model downloaded and cached to {self.model_path}.")

    def scan(self, texts: List[str]) -> Dict[str, Dict[str, List[Dict[str, str]]]]:
        """
        Scan the input list of texts using the GLiNER model and SpaCy.
        """
        results = {}
        # Define the labels for GLiNER processing
        labels = [
            'BOOKING NUMBER', 'PERSONALLY IDENTIFIABLE INFORMATION', 'DRIVER LICENSE',
            'PERSON', 'ADDRESS', 'COMPANY', 'EMAIL', 'PASSPORT NUMBER', 'AADHAAR NUMBER',
            'PHONE NUMBER', 'BANK ACCOUNT NUMBER', 'PAN NUMBER'
        ]
        # Define sensitive labels to be further processed by SpaCy
        sensitive_labels_for_spacy = [
            'AADHAAR NUMBER', 'PHONE NUMBER', 'BANK ACCOUNT NUMBER', 'PAN NUMBER', 'VOTER ID NUMBER'
        ]

        for text in texts:
            logger.info("Processing text")
            text_start_time = time.time()  # Capture the start time of the text processing
            chunks = chunk_text(text)
            
            all_ml_entities = []
            all_spacy_entities = []

            # List to collect sensitive text entities for SpaCy processing
            spacy_input_texts = []

            for idx, chunk in enumerate(chunks):
                chunk_start_time = time.time()  # Capture the start time of the chunk processing
                try:
                    logger.debug(f"Processing chunk {idx}: {chunk[:100]}...")  # Print first 100 chars of chunk for brevity
                    
                    # GLiNER-based entity detection
                    ml_entities = self.model.predict_entities(chunk, labels)
                    
                    # Process the ML entities detected
                    ml_processed_entities = []
                    for entity in ml_entities:
                        processed_entity = {
                            'entity_group': entity['label'],
                            'score': entity.get('score', 1.0),  # Assuming a default score of 1.0 if not provided
                            'word': entity['text'],
                            'start': entity['start'],
                            'end': entity['end'],
                            'text': chunk[entity['start']:entity['end']]
                        }
                        ml_processed_entities.append(processed_entity)

                        # If entity group is sensitive, add to SpaCy input list
                        if entity['label'] in sensitive_labels_for_spacy:
                            spacy_input_texts.append(processed_entity['text'])

                    # Log GLiNER results
                    logger.debug(f"Chunk {idx} - GLiNER entities: {ml_processed_entities}")
                    
                    all_ml_entities.extend(ml_processed_entities)

                except Exception as e:
                    logger.error(f"Error processing chunk {idx}: {chunk} - {e}")

                chunk_end_time = time.time()  # Capture the end time of the chunk processing
                logger.info(f"Time taken for chunk {idx}: {chunk_end_time - chunk_start_time:.2f} seconds")

            # If there are entities to be processed by SpaCy, process them
            if spacy_input_texts:
                try:
                    spacy_start_time = time.time()  # Capture the start time of the SpaCy processing
                    spacy_entities = self.spacy_processor.process_texts(spacy_input_texts)
                    spacy_end_time = time.time()  # Capture the end time of the SpaCy processing
                    logger.info(f"Time taken for SpaCy processing: {spacy_end_time - spacy_start_time:.2f} seconds")
                    print("spacy output ->")
                    print(spacy_entities)
                    print("====")
                    all_spacy_entities.append(spacy_entities)
                    # Log SpaCy results
                    logger.debug(f"SpaCy entities: {spacy_entities}")
                except Exception as e:
                    logger.error(f"Error processing with SpaCy: {e}")

            # Combine results
            results["result"] = {
                'entity_detected_ml': all_ml_entities,
                'entity_detected_spacy': all_spacy_entities,
            }

            text_end_time = time.time()  # Capture the end time of the text processing
            logger.info(f"Total time taken for processing text: {text_end_time - text_start_time:.2f} seconds")
            # Log final results
            logger.info(f"Processed text: {text[:100]}... - Entities (ML): {all_ml_entities} - Entities (Spacy): {all_spacy_entities}")

        return results
