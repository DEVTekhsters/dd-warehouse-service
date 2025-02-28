from abc import ABC, abstractmethod
from typing import List, Dict, Any, AsyncIterator, Awaitable
from pathlib import Path
from app.domain.models.pii_entity import ScanResult, ScanConfig

class PIIScannerInterface(ABC):
    """
    Interface defining the contract for PII scanning operations.
    Following Interface Segregation Principle from SOLID.
    """
    
    @abstractmethod
    def scan_stream(self, data_stream: AsyncIterator[bytes], config: ScanConfig) -> AsyncIterator[ScanResult]:
        """
        Scan a stream of data for PII information.
        Implements streaming to handle large files efficiently.
        """
        pass

    @abstractmethod
    async def scan_text(self, text: str, config: ScanConfig) -> ScanResult:
        """
        Scan a text string for PII information.
        """
        pass

class ModelManagerInterface(ABC):
    """
    Interface for ML model management operations.
    Separates model concerns from scanning logic (Single Responsibility Principle).
    """
    
    @abstractmethod
    async def get_model(self, model_type: str = 'spacy') -> Any:
        """
        Get or initialize the required ML model.
        Implements lazy loading for memory efficiency.
        """
        pass

    @abstractmethod
    async def release_model(self, model_type: str) -> None:
        """
        Release model resources when not needed.
        """
        pass

class FileProcessorInterface(ABC):
    """
    Interface for file processing operations.
    Separates file handling from scanning logic.
    """
    
    @abstractmethod
    def process_file(self, file_path: Path, config: ScanConfig) -> AsyncIterator[bytes]:
        """
        Process a file and yield chunks of data.
        """
        pass

    @abstractmethod
    async def get_file_info(self, file_path: Path) -> Dict[str, Any]:
        """
        Get file metadata and information.
        """
        pass

class ResultProcessor(ABC):
    """
    Interface for processing and storing scan results.
    Handles result aggregation and storage.
    """
    
    @abstractmethod
    async def process_results(self, results: List[ScanResult]) -> Dict[str, Any]:
        """
        Process and aggregate scan results.
        """
        pass

    @abstractmethod
    async def store_results(self, results: Dict[str, Any], batch_size: int = 1000) -> None:
        """
        Store processed results with batching support.
        """
        pass