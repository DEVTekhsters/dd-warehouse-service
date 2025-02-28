from __future__ import annotations
from typing import Dict, Any, Optional, Type
from dataclasses import dataclass
from fastapi import APIRouter

from app.interfaces.api.pii_scan_router import (
    PIIScanRouter, 
    RouterConfig,
    create_pii_router
)
from app.monitoring.metrics import MetricsCollector
from app.interfaces.api.scanner import ScannerManager
from app.interfaces.api.validators import FileValidators

@dataclass
class RouterOptions:
    """Configuration options for router initialization."""
    prefix: str
    tags: list[str]
    max_file_size: int
    allowed_extensions: Dict[str, set[str]]

class RouterFactory:
    """Factory for creating and configuring routers."""
    
    def __init__(self) -> None:
        """Initialize factory with shared dependencies."""
        self._metrics = MetricsCollector()
        self._scanner_manager = ScannerManager()
        self._routers: Dict[str, APIRouter] = {}

    def _create_file_validator(
        self,
        max_size: int,
        allowed_extensions: Dict[str, set[str]]
    ) -> FileValidators:
        """Create a configured file validator."""
        return FileValidators(
            max_size=max_size,
            allowed_extensions=allowed_extensions
        )

    def create_pii_router(self, options: RouterOptions) -> APIRouter:
        """Create and configure a PII scanning router."""
        if "pii" in self._routers:
            return self._routers["pii"]

        # Create base router
        router = APIRouter(
            prefix=options.prefix,
            tags=options.tags
        )

        # Create file validator
        validator = self._create_file_validator(
            max_size=options.max_file_size,
            allowed_extensions=options.allowed_extensions
        )

        # Create router config
        config = RouterConfig(
            scanner_manager=self._scanner_manager,
            metrics=self._metrics,
            file_validator=validator
        )

        # Create and configure PII router
        pii_router = create_pii_router(
            scanner_manager=self._scanner_manager,
            metrics=self._metrics,
            file_validator=validator
        )

        # Store router instance
        self._routers["pii"] = router
        return router

# Global factory instance
router_factory = RouterFactory()

def create_api_router(
    prefix: str = "/api/v2/pii",
    tags: Optional[list[str]] = None
) -> APIRouter:
    """Create a configured API router."""
    options = RouterOptions(
        prefix=prefix,
        tags=tags or ["PII Scanner"],
        max_file_size=100 * 1024 * 1024,  # 100MB
        allowed_extensions={
            'text': {'.txt', '.csv', '.json'},
            'document': {'.pdf', '.docx', '.xlsx'},
            'image': {'.jpg', '.jpeg', '.png'}
        }
    )
    return router_factory.create_pii_router(options)