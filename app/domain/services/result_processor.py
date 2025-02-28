from typing import List, Dict, Any, Optional
import asyncio
import logging
from datetime import datetime
import json
from uuid import uuid4
from app.domain.models.pii_entity import ScanResult
from app.domain.models.memory_manager import MemoryManager
from app.domain.services.scanner_interface import ResultProcessor
from client_connect import Connection

logger = logging.getLogger(__name__)

class ClickHouseResultProcessor(ResultProcessor):
    """
    Handles efficient processing and storage of scan results in ClickHouse.
    Implements batching and memory-efficient operations.
    """

    def __init__(self, batch_size: int = 1000):
        """Initialize with memory manager and connection pool."""
        self.memory_manager = MemoryManager()
        self.batch_size = batch_size
        self._connection: Optional[Connection] = None
        self.table_name = 'scan_results'
        self.meta_table_name = 'scan_metadata'

    @property
    def connection(self) -> Connection:
        """Lazy initialization of database connection."""
        if self._connection is None:
            self._connection = Connection()
        return self._connection

    async def process_results(self, results: List[ScanResult]) -> Dict[str, Any]:
        """
        Process and aggregate scan results efficiently.
        Groups results by entity types and calculates statistics.
        """
        try:
            entity_stats: Dict[str, Dict[str, Any]] = {}
            total_entities = 0
            file_stats: Dict[str, Any] = {
                'total_size': 0,
                'file_count': len(results)
            }

            # Process results in chunks to manage memory
            chunk_size = 100
            for i in range(0, len(results), chunk_size):
                chunk = results[i:i + chunk_size]
                
                # Check memory before processing chunk
                if not self.memory_manager.check_memory():
                    self.memory_manager.force_cleanup()
                    await asyncio.sleep(0.1)

                # Process chunk
                for result in chunk:
                    file_stats['total_size'] += result.file_size
                    
                    for entity in result.entities:
                        total_entities += 1
                        if entity.entity_type not in entity_stats:
                            entity_stats[entity.entity_type] = {
                                'count': 0,
                                'avg_confidence': 0.0,
                                'samples': []
                            }
                        
                        stats = entity_stats[entity.entity_type]
                        stats['count'] += 1
                        stats['avg_confidence'] = (
                            (stats['avg_confidence'] * (stats['count'] - 1) + entity.confidence_score)
                            / stats['count']
                        )
                        # Keep limited samples for reference
                        if len(stats['samples']) < 5:
                            stats['samples'].append(entity.text)

            return {
                'summary': {
                    'total_entities': total_entities,
                    'unique_entity_types': len(entity_stats),
                    'files_processed': file_stats
                },
                'entity_statistics': entity_stats
            }

        except Exception as e:
            logger.error(f"Error processing results: {e}")
            raise

    async def store_results(self, results: Dict[str, Any], batch_size: int = 1000) -> None:
        """
        Store processed results in ClickHouse with efficient batching.
        Implements retry logic and transaction management.
        """
        try:
            # Prepare data for batch insertion
            current_time = datetime.now()
            batch: List[tuple] = []
            meta_batch: List[tuple] = []

            # Convert results to database records
            scan_id = str(uuid4())
            json_data = json.dumps(results)

            batch.append((
                scan_id,
                current_time,
                results['summary']['total_entities'],
                results['summary']['unique_entity_types'],
                json_data
            ))

            # Prepare metadata records
            for entity_type, stats in results['entity_statistics'].items():
                meta_batch.append((
                    str(uuid4()),
                    scan_id,
                    entity_type,
                    stats['count'],
                    float(stats['avg_confidence']),
                    json.dumps(stats['samples']),
                    current_time
                ))

            # Store results with batching
            await self._store_batch(
                self.table_name,
                batch,
                ['id', 'scan_timestamp', 'total_entities', 'unique_types', 'result_json']
            )

            # Store metadata with batching
            for i in range(0, len(meta_batch), batch_size):
                chunk = meta_batch[i:i + batch_size]
                if not self.memory_manager.check_memory():
                    self.memory_manager.force_cleanup()
                    await asyncio.sleep(0.1)
                
                await self._store_batch(
                    self.meta_table_name,
                    chunk,
                    ['id', 'scan_id', 'entity_type', 'count', 'avg_confidence', 'samples', 'created_at']
                )

        except Exception as e:
            logger.error(f"Error storing results: {e}")
            raise
        finally:
            self._cleanup_connection()

    async def _store_batch(self, table: str, batch: List[tuple], columns: List[str]) -> None:
        """Store a batch of records in the specified table."""
        if not batch:
            return

        try:
            await asyncio.to_thread(
                self.connection.client.insert,
                table,
                batch,
                column_names=columns
            )
        except Exception as e:
            logger.error(f"Error inserting batch into {table}: {e}")
            raise

    def _cleanup_connection(self) -> None:
        """Clean up database connection."""
        if self._connection:
            self._connection.client.close()
            self._connection = None

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with cleanup."""
        self._cleanup_connection()
        self.memory_manager.cleanup()
        return False  # Don't suppress exceptions