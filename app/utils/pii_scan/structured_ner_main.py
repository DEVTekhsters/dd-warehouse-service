import os
import time
import logging
import traceback
import random
import re
from collections import defaultdict
from typing import Dict, List, Optional, Union
from app.utils.pii_scan.regex_patterns.data_regex import patterns
from multiprocessing import Pool, cpu_count
import spacy

# Define logger
log_file = 'spacy_regex_scanner.log'
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MLBasedNERScannerForStructuredData:
    """
    NER Scanner using SpaCy and Regex for entity recognition.
    """

    def __init__(self):
        # Load the SpaCy model
        self.nlp = self.load_spacy_model()

    def load_spacy_model(self):
        """
        Load the SpaCy model for NER.
        """
        try:
            nlp = spacy.load("en_core_web_sm")
            print("SpaCy model loaded successfully.")
            return nlp
        except Exception as e:
            logger.error(f"Error loading SpaCy model: {e}")
            raise

    def _parse_spacy_results(self, doc) -> Dict[str, Dict[str, Union[int, List[str]]]]:
        """
        Parse the results from SpaCy and extract relevant details.
        """
        entities_info = defaultdict(lambda: {"count": 0, "texts": []})

        for ent in doc.ents:
            entity_type = ent.label_
            detected_text = ent.text

            if entity_type and detected_text:
                entities_info[entity_type]["count"] += 1
                entities_info[entity_type]["texts"].append(detected_text)

        return dict(entities_info)

    def _apply_regex_patterns(self, text: str) -> Dict[str, str]:
        """
        Apply regex patterns to a text and return detected entities.
        """
        detected_entities = {}
        for entity_type, pattern in patterns.items():
            matches = re.findall(pattern, str(text))
            if matches:
                detected_entities[entity_type] = ', '.join(matches)
        return detected_entities

    def _process_with_spacy(self, texts: List[str]) -> Dict[str, List[Dict[str, Union[str, Dict[str, Union[int, List[str]]]]]]]:
        """
        Process texts using SpaCy.
        """
        results = []
        for text in texts:
            text = text.strip()
            print("=------->", text)
            try:
                doc = self.nlp(text)
                spacy_results = self._parse_spacy_results(doc)
                regex_results = self._apply_regex_patterns(text)
                print(spacy_results, combined_result, "😂")
                combined_result = {
                    "text": text,
                    "entity_detected_spacy": spacy_results,
                    "entity_detected_regex": regex_results
                }
                results.append(combined_result)
            except Exception as exc:
                logger.warning(f"Error processing text with SpaCy model: '{ text}' - {exc}")
                logger.debug(traceback.format_exc())
                results.append({
                    "text": text,
                    "entity_detected_spacy": {},
                    "entity_detected_regex": {}
                })

        return {"results": results}

    def scan(self, sample_data: List[str], chunk_size: int=100, sample_size: Optional[Union[int, float]]=None) -> Dict[str, List[Dict[str, Union[str, Dict[str, Union[int, List[str]]]]]]]:
        """
        Scan the input list of text using SpaCy and Regex models and return results.
        Can process only a sample of the data if sample_size is specified.
        """
        start_time = time.time()  # Initialize start_time

        if sample_size:
            sample_data = self._sample_data(sample_data, sample_size)

        # Split the data into chunks
        chunks = [sample_data[i:i + chunk_size] for i in range(0, len(sample_data), chunk_size)]

        # Determine the number of workers based on available CPUs and chunk sizexx
        num_workers = min(cpu_count(), len(chunks))

        # Process the chunks in parallel
        with Pool(processes=num_workers) as pool:
            results = pool.map(self._process_with_spacy, chunks)

        # Combine results from all chunks
        combined_results = []
        for result in results:
            combined_results.extend(result.get("results", []))

        end_time = time.time()
        processing_time = end_time - start_time
        logger.info(f"Total processing time: {processing_time:.2f} seconds")

        # Return combined results
        return {
            "results": combined_results
        }

