import asyncio
from typing import List, Dict, Any, AsyncIterator
import logging
from pathlib import Path
from datetime import datetime
from app.domain.models.pii_entity import PIIEntity, ScanResult, ScanConfig
from app.domain.services.scanner_interface import PIIScannerInterface
from app.domain.services.model_manager import MLModelManager
from app.domain.services.file_processor import StreamingFileProcessor
from app.domain.models.memory_manager import MemoryManager

logger = logging.getLogger(__name__)

class PIIScanner(PIIScannerInterface):
    """
    Core PII scanning service implementing streaming processing
    and efficient resource management.
    """

    def __init__(self):
        """Initialize scanner with required dependencies."""
        self.model_manager = MLModelManager()
        self.file_processor = StreamingFileProcessor()
        self.memory_manager = MemoryManager()
        self.pii_categories = {
            'PERSON': ['NAME', 'FIRST_NAME', 'LAST_NAME'],
            'LOCATION': ['ADDRESS', 'CITY', 'COUNTRY'],
            'ID_NUMBER': ['SSN', 'PASSPORT', 'DRIVER_LICENSE'],
            'CONTACT': ['EMAIL', 'PHONE', 'FAX'],
            'FINANCIAL': ['CREDIT_CARD', 'BANK_ACCOUNT', 'IBAN']
        }

    async def scan_stream(self, data_stream: AsyncIterator[bytes], config: ScanConfig) -> AsyncIterator[ScanResult]:
        """
        Scan a stream of data for PII information.
        Processes data in chunks to maintain memory efficiency.
        """
        buffer = bytearray()
        chunk_count = 0

        try:
            spacy_model = await self.model_manager.get_model('spacy')
            gliner_model = await self.model_manager.get_model('gliner')

            async for chunk in data_stream:
                # Add chunk to buffer
                buffer.extend(chunk)
                chunk_count += 1

                # Process complete chunks
                while len(buffer) >= config.chunk_size:
                    process_chunk = buffer[:config.chunk_size]
                    buffer = buffer[config.chunk_size:]

                    # Check memory before processing
                    if not self.memory_manager.check_memory():
                        self.memory_manager.force_cleanup()
                        await asyncio.sleep(0.1)  # Give system time to recover

                    # Process chunk and yield results
                    try:
                        text = process_chunk.decode('utf-8', errors='ignore')
                        scan_result = await self.scan_text(text, config)
                        yield scan_result
                    except Exception as e:
                        logger.error(f"Error processing chunk {chunk_count}: {e}")
                        continue

            # Process remaining buffer if any
            if buffer:
                try:
                    text = buffer.decode('utf-8', errors='ignore')
                    scan_result = await self.scan_text(text, config)
                    yield scan_result
                except Exception as e:
                    logger.error(f"Error processing final chunk: {e}")

        finally:
            # Release model resources
            await self.model_manager.release_model('spacy')
            await self.model_manager.release_model('gliner')
            self.memory_manager.cleanup()

    async def scan_text(self, text: str, config: ScanConfig) -> ScanResult:
        """
        Scan a text string for PII information using multiple models.
        Combines results from different models for comprehensive scanning.
        """
        entities: List[PIIEntity] = []
        try:
            # Use SpaCy for initial NER
            spacy_model = await self.model_manager.get_model('spacy')
            doc = spacy_model(text)
            
            # Process SpaCy entities
            for ent in doc.ents:
                if self._is_pii_entity(ent.label_):
                    entities.append(PIIEntity(
                        entity_type=ent.label_,
                        text=ent.text,
                        confidence_score=0.8,  # SpaCy doesn't provide confidence scores
                        start_position=ent.start_char,
                        end_position=ent.end_char
                    ))

            # Use GLiNER for specialized PII detection
            gliner_model = await self.model_manager.get_model('gliner')
            gliner_results = gliner_model.predict_entities(
                text,
                labels=list(self.pii_categories.keys())
            )

            # Process GLiNER entities
            for result in gliner_results:
                entities.append(PIIEntity(
                    entity_type=result['label'],
                    text=result['text'],
                    confidence_score=result.get('score', 0.9),
                    start_position=result.get('start'),
                    end_position=result.get('end')
                ))

            # Remove duplicates while keeping highest confidence
            entities = self._deduplicate_entities(entities)

            return ScanResult(
                file_name="text_chunk",
                file_size=len(text.encode('utf-8')),
                scan_timestamp=datetime.now(),
                entities=entities,
                metadata={'chunk_size': config.chunk_size}
            )

        except Exception as e:
            logger.error(f"Error in text scanning: {e}")
            raise

    def _is_pii_entity(self, label: str) -> bool:
        """Check if an entity label represents PII information."""
        for category in self.pii_categories.values():
            if label in category:
                return True
        return False

    def _deduplicate_entities(self, entities: List[PIIEntity]) -> List[PIIEntity]:
        """Remove duplicate entities keeping the highest confidence scores."""
        entity_dict: Dict[str, PIIEntity] = {}
        
        for entity in entities:
            key = f"{entity.entity_type}:{entity.text}"
            if key not in entity_dict or entity.confidence_score > entity_dict[key].confidence_score:
                entity_dict[key] = entity
        
        return list(entity_dict.values())

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with cleanup."""
        await self.model_manager.release_model('spacy')
        await self.model_manager.release_model('gliner')
        self.memory_manager.cleanup()
        return False  # Don't suppress exceptions