from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine.spacy_nlp_engine import SpacyNlpEngine
from .ner_regex_patteren_for_data import patterns
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StringAnalysis:
    """
    Used to store results from the sample data scans for each NER Entity.
    """
    def __init__(self, score: float = 0, appearances: int = 0):
        self.score = score
        self.appearances = appearances

class NERScanner:
    def __init__(self):
        import spacy
        from presidio_analyzer import PatternRecognizer

        SPACY_EN_MODEL = "en_core_web_md"

        # Load spaCy model
        try:
            spacy.load(SPACY_EN_MODEL)
        except OSError:
            from spacy.cli import download
            download(SPACY_EN_MODEL)
            spacy.load(SPACY_EN_MODEL)

        # Initialize Presidio Analyzer with spaCy NLP engine
        self.analyzer = AnalyzerEngine(
            nlp_engine=SpacyNlpEngine(models={"en": SPACY_EN_MODEL})
        )

        # Add custom recognizers based on provided patterns
        for entity, pattern in patterns.items():
            recognizer = PatternRecognizer(supported_entity=entity, patterns=[pattern])
            self.analyzer.registry.add_recognizer(recognizer)

    @staticmethod
    def get_highest_score_label(entities_score: Dict[str, StringAnalysis]) -> Tuple[str, float]:
        """
        Get the entity label with the highest score and number of appearances.
        """
        return max(
            entities_score.items(),
            key=lambda item: (item[1].appearances, item[1].score)
        )

    def scan(self, sample_data_rows: List[Any], chunk_size=None) -> Optional[Dict[str, Any]]:
        """
        Scans the input data rows and returns the highest scoring entity and detected entities with occurrences.
        """
        entities_score = defaultdict(StringAnalysis)
        str_sample_data_rows = map(str, filter(None, sample_data_rows))  # Optimized filtering and mapping

        for row in str_sample_data_rows:
            try:
                results = self.analyzer.analyze(row, language="en")

                for result in results:
                    entity_analysis = entities_score[result.entity_type]
                    if result.score > entity_analysis.score:
                        entity_analysis.score = result.score
                    entity_analysis.appearances += 1

            except Exception as exc:
                logger.warning(f"Error processing row: {exc}")
                logger.debug(traceback.format_exc())

        if entities_score:
            label, analysis = self.get_highest_score_label(entities_score)
            detected_entities = {entity: data.appearances for entity, data in entities_score.items()}
            return {
                "highest_label": label,
                "confidence_score": analysis.score,
                "detected_entities": detected_entities
            }

        return None
