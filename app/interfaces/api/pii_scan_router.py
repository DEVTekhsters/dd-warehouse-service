from __future__ import annotations
from dataclasses import dataclass
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, BackgroundTasks
from typing import List, Dict, Any, Optional, NoReturn, Literal
import logging
from pathlib import Path

from app.domain.models.pii_entity import ScanConfig
from app.monitoring.metrics import MetricsCollector
from app.interfaces.api.file_manager import TemporaryFileManager, FileError
from app.interfaces.api.validators import FileValidators, ValidationError
from app.interfaces.api.scanner import ScannerManager, Scanner, ScanResult
from app.interfaces.api.initializers import ManagedResource, InitializationError
from app.interfaces.api.route_setup import (
    RouteDefinition,
    setup_routes,
    create_route
)

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class APIResponse:
    """Standard API response structure."""
    message: str
    customer_id: int
    files_processed: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        """Convert response to dictionary format."""
        return {
            'message': self.message,
            'customer_id': self.customer_id,
            'files_processed': self.files_processed
        }

class PIIScanRouter(ManagedResource):
    """Router implementation with proper initialization handling."""

    def __init__(
        self,
        scanner_manager: ScannerManager,
        metrics: MetricsCollector,
        file_validator: FileValidators
    ) -> None:
        """Initialize router with dependencies."""
        super().__init__()
        self.scanner_manager = scanner_manager
        self.metrics = metrics
        self.file_validator = file_validator
        self.router = APIRouter()
        self._setup_routes()

    def initialize(self) -> bool:
        """Initialize router and its dependencies."""
        try:
            # Verify dependencies
            if not all([self.scanner_manager, self.metrics, self.file_validator]):
                self._mark_failed("Missing required dependencies")
                return False
            
            # Initialize scanner manager
            if not self.scanner_manager.initialize():
                self._mark_failed("Scanner manager initialization failed")
                return False
            
            self._mark_initialized()
            return True
        except Exception as e:
            self._mark_failed(f"Initialization error: {str(e)}")
            return False

    def _setup_routes(self) -> Literal[None]:
        """Set up router endpoints."""
        routes: List[RouteDefinition] = [
            create_route(
                path="/scan/",
                method="POST",
                handler=self.scan_files,
                response_model=Dict[str, Any],
                description="Process multiple files for PII scanning",
                tags=["PII Scanner"]
            ),
            create_route(
                path="/status/{task_id}",
                method="GET",
                handler=self.get_processing_status,
                response_model=Dict[str, Any],
                description="Get task processing status",
                tags=["PII Scanner"]
            )
        ]

        setup_routes(self.router, routes)
        return None

    def _create_config(self, sample_size: float, batch_size: int) -> ScanConfig:
        """Create scan configuration with validated parameters."""
        return ScanConfig(
            sample_size=max(0.1, min(1.0, sample_size)),
            batch_size=max(100, min(5000, batch_size)),
            chunk_size=1024 * 1024,  # 1MB chunks
            max_memory_usage=int(1.5 * 1024 * 1024 * 1024)  # 1.5GB limit
        )

    async def process_single_file(
        self,
        file: UploadFile,
        config: ScanConfig,
        customer_id: int,
        scanner: Scanner
    ) -> Dict[str, Any]:
        """Process a single file with validation and error handling."""
        if not file.filename:
            raise ValidationError("File has no name")

        try:
            # Validate file
            file_type = await self.file_validator.validate_file(file)
            if not file_type:
                raise ValidationError("Invalid file type")

            # Process file
            async with TemporaryFileManager(file.file, file.filename) as temp_path:
                result = await scanner.process_file(temp_path, config, customer_id)
                return result.to_dict()

        except ValidationError as e:
            logger.error(f"Validation error for file {file.filename}: {e}")
            raise HTTPException(status_code=400, detail=str(e))
        except FileError as e:
            logger.error(f"File operation error: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def scan_files(
        self,
        background_tasks: BackgroundTasks,
        customer_id: int = Form(...),
        files: List[UploadFile] = File(...),
        sample_size: float = Form(0.2),
        batch_size: int = Form(1000)
    ) -> Dict[str, Any]:
        """Process multiple files for PII scanning."""
        if not self.is_initialized and not self.initialize():
            raise InitializationError(f"Router not initialized: {self.initialization_error}")

        if not files:
            raise HTTPException(status_code=400, detail="No files provided")

        config = self._create_config(sample_size, batch_size)
        scanner = self.scanner_manager.create_scanner()
        processing_results: List[Dict[str, Any]] = []

        for file in files:
            if file.filename:
                result = await self.process_single_file(
                    file=file,
                    config=config,
                    customer_id=customer_id,
                    scanner=scanner
                )
                processing_results.append(result)

        response = APIResponse(
            message="Files processed successfully",
            customer_id=customer_id,
            files_processed=processing_results
        )
        return response.to_dict()

    async def get_processing_status(self, task_id: str) -> NoReturn:
        """Get the status of a processing task."""
        raise HTTPException(
            status_code=501,
            detail="Async task tracking not yet implemented"
        )

    async def cleanup(self) -> None:
        """Clean up router resources."""
        await self.scanner_manager.cleanup()
        self._mark_failed("Cleaned up")

# Create router instance with dependencies
router = APIRouter()
pii_router = PIIScanRouter(
    scanner_manager=ScannerManager(),
    metrics=MetricsCollector(),
    file_validator=FileValidators(
        max_size=100 * 1024 * 1024,  # 100MB
        allowed_extensions={
            'text': {'.txt', '.csv', '.json'},
            'document': {'.pdf', '.docx', '.xlsx'},
            'image': {'.jpg', '.jpeg', '.png'}
        }
    )
)

# Add routes to the main router
router.include_router(
    pii_router.router,
    prefix="/api/v2/pii",
    tags=["PII Scanner"]
)