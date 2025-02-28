from __future__ import annotations
from typing import Dict, Any, List, Optional
from pathlib import Path
import logging
import asyncio
from dataclasses import dataclass, field

from app.domain.models.pii_entity import ScanConfig
from app.domain.services.pii_scanner import PIIScanner
from app.domain.services.file_processor import StreamingFileProcessor
from app.domain.services.result_processor import ClickHouseResultProcessor
from app.domain.models.memory_manager import MemoryManager
from app.monitoring.metrics import MetricsCollector, MetricLabels
from app.interfaces.api.chunks import ChunkIterator
from app.interfaces.api.initializers import ManagedResource, InitializationError

logger = logging.getLogger(__name__)

@dataclass
class ScanResult:
    """Result of a scan operation."""
    file_name: str
    status: str
    summary: Dict[str, Any]
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert scan result to dictionary."""
        return {
            'file_name': self.file_name,
            'status': self.status,
            'summary': self.summary,
            'details': self.details
        }

class ScannerInitializationError(InitializationError):
    """Raised when scanner initialization fails."""
    pass

class Scanner(ManagedResource):
    """Core scanning logic encapsulation."""

    def __init__(self, memory_manager: MemoryManager, metrics: MetricsCollector) -> None:
        """Initialize scanner with required dependencies."""
        super().__init__()
        self.memory_manager = memory_manager
        self.metrics = metrics

    def initialize(self) -> bool:
        """Initialize scanner resources."""
        try:
            # Verify dependencies
            if not self.memory_manager or not self.metrics:
                self._mark_failed("Missing required dependencies")
                return False
            
            # Additional initialization logic here
            self._mark_initialized()
            return True

        except Exception as e:
            self._mark_failed(str(e))
            logger.error(f"Scanner initialization failed: {e}")
            return False

    async def process_file(
        self,
        file_path: Path,
        config: ScanConfig,
        customer_id: int
    ) -> ScanResult:
        """Process a single file with proper resource management."""
        if not self.is_initialized:
            if not self.initialize():
                raise ScannerInitializationError(
                    f"Scanner not initialized: {self.initialization_error}"
                )

        labels = MetricLabels(
            customer_id=str(customer_id),
            file_type=file_path.suffix.lower(),
            status="processing"
        )
        
        async with (
            StreamingFileProcessor() as file_processor,
            PIIScanner() as scanner,
            ClickHouseResultProcessor() as result_processor
        ):
            try:
                # Get file information
                file_info = await file_processor.get_file_info(file_path)
                self.metrics.track_memory_usage("file_processing")
                
                # Process file in chunks
                results: List[Dict[str, Any]] = []
                async for chunk in file_processor.process_file(file_path, config):
                    if not await self.memory_manager.check_memory():
                        self.memory_manager.cleanup()
                        await asyncio.sleep(0.1)
                    
                    # Create chunk iterator
                    chunk_iter = ChunkIterator(chunk)
                    
                    # Process chunk through scanner
                    async for scan_result in scanner.scan_stream(chunk_iter, config):
                        results.append(scan_result)

                # Process and store results
                processed_results = await result_processor.process_results(results)
                processed_results['customer_id'] = customer_id
                processed_results['file_info'] = file_info
                
                await result_processor.store_results(processed_results)
                
                labels.status = "success"
                self.metrics.track_file_processed(labels)
                
                return ScanResult(
                    file_name=file_path.name,
                    status='success',
                    summary=processed_results['summary'],
                    details={'file_info': file_info}
                )

            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")
                labels.status = "error"
                self.metrics.track_file_processed(labels)
                self.metrics.track_error("processing", "file_scanner")
                raise

    async def cleanup(self) -> None:
        """Clean up scanner resources."""
        self.memory_manager.cleanup()
        self._status = None

class ScannerManager(ManagedResource):
    """Factory for creating Scanner instances."""

    def __init__(self) -> None:
        """Initialize scanner manager."""
        super().__init__()
        self.memory_manager = MemoryManager()
        self.metrics = MetricsCollector()
        self._default_scanner: Optional[Scanner] = None

    def initialize(self) -> bool:
        """Initialize manager resources."""
        try:
            # Initialize dependencies
            if not self.memory_manager or not self.metrics:
                self._mark_failed("Missing required dependencies")
                return False
            
            self._mark_initialized()
            return True
        except Exception as e:
            self._mark_failed(str(e))
            return False

    def create_scanner(self) -> Scanner:
        """Create a new Scanner instance or return cached one."""
        if not self.is_initialized:
            if not self.initialize():
                raise ScannerInitializationError(
                    f"Manager not initialized: {self.initialization_error}"
                )

        if self._default_scanner is None:
            scanner = Scanner(
                memory_manager=self.memory_manager,
                metrics=self.metrics
            )
            if not scanner.initialize():
                raise ScannerInitializationError(
                    f"Failed to initialize scanner: {scanner.initialization_error}"
                )
            self._default_scanner = scanner

        return self._default_scanner

    async def cleanup(self) -> None:
        """Clean up all resources."""
        if self._default_scanner:
            await self._default_scanner.cleanup()
            self._default_scanner = None
        self.memory_manager.cleanup()
        self._mark_failed("Cleaned up")