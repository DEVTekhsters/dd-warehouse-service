import logging
import re
from collections import defaultdict
from typing import Dict, List
import spacy
from presidio_analyzer import AnalyzerEngine, PatternRecognizer
from presidio_analyzer.nlp_engine.spacy_nlp_engine import SpacyNlpEngine
from app.utils.pii_scan.regex_patterns.data_regex import patterns

# Define logger
# Setup logging
logging.basicConfig(
    filename='app.log',
    level=logging.DEBUG,  # Set to DEBUG to capture detailed log messages
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SPACY_EN_MODEL = "en_core_web_md"

class SpaCyNERProcessor:
    """
    SpaCy-based NER Processor to identify entities in texts with custom recognizers.
    """
    def __init__(self):
        self._load_spacy_model()
        self.analyzer = AnalyzerEngine(
            nlp_engine=SpacyNlpEngine(models={"en": SPACY_EN_MODEL})
        )
        self._setup_recognizers()

    def _load_spacy_model(self):
        """
        Load SpaCy model or download it if not present.
        """
        if not spacy.util.is_package(SPACY_EN_MODEL):
            logger.info(f"Downloading {SPACY_EN_MODEL} language model for SpaCy")
            spacy.cli.download(SPACY_EN_MODEL)
        spacy.load(SPACY_EN_MODEL)

    def _setup_recognizers(self):
        """
        Setup custom recognizers with defined patterns and add them to the analyzer.
        """
        try:
            for entity, pattern in patterns.items():
                recognizer = PatternRecognizer(supported_entity=entity, patterns=[pattern])
                self.analyzer.registry.add_recognizer(recognizer)
        except Exception as e:
            logger.error(f"Failed to setup recognizers: {e}")
            raise

    def parse_result_string(self, result_string: str) -> Dict[str, List[Dict[str, float]]]:
        """
        Parses a result string into a structured dictionary.

        :param result_string: Raw string output of detected entities.
        :return: Dictionary with structured entity data.
        """
        pattern = r"type: ([A-Z_]+), start: (\d+), end: (\d+), score: ([\d.]+)"
        matches = re.findall(pattern, result_string)

        entities = defaultdict(list)
        for match in matches:
            entity_type, start, end, score = match
            entities[entity_type].append({
                "start": int(start),
                "end": int(end),
                "score": float(score)
            })

        return dict(entities)

    def process_texts(self, texts: List[str]) -> Dict[str, Dict[str, List[str]]]:
        """
        Process a list of texts using Presidio Analyzer and return detected entities and their texts.
        """
        highest_scores = defaultdict(lambda: {"text": "", "score": 0, "count": 0})

        for text in texts:
            try:
                results = self.analyzer.analyze(text=text, language="en")
                if not results:
                    continue

                for result in results:
                    entity_type = result.entity_type
                    detected_text = text[result.start:result.end]
                    score = result.score

                    if score > highest_scores[entity_type]["score"]:
                        highest_scores[entity_type] = {"text": detected_text, "score": score, "count": 1}
                    elif score == highest_scores[entity_type]["score"]:
                        highest_scores[entity_type]["count"] += 1

            except Exception as exc:
                logger.warning(f"Error processing text: {text} - {exc}")

        final_entities = {}
        for entity, data in highest_scores.items():
            final_entities[entity] = {
                "count": data["count"],
                "texts": [data["text"]],
                "scores": [data["score"]]
            }

        return {"entities": final_entities}
