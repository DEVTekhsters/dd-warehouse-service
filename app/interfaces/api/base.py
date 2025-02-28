from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from dataclasses import dataclass
from fastapi import APIRouter

@dataclass
class BaseConfig:
    """Base configuration for router components."""
    pass

@dataclass
class RouterMetadata:
    """Metadata for router configuration."""
    prefix: str
    tags: list[str]
    description: Optional[str] = None

class BaseRouter(ABC):
    """Base class for all routers."""
    
    @abstractmethod
    def get_router(self) -> APIRouter:
        """Get the configured router instance."""
        pass

    @abstractmethod
    def get_metadata(self) -> RouterMetadata:
        """Get router metadata."""
        pass

    @abstractmethod
    def initialize(self) -> None:
        """Initialize router dependencies."""
        pass

class RouterException(Exception):
    """Base exception for router errors."""
    pass

class RouterInitializationError(RouterException):
    """Raised when router initialization fails."""
    pass

class RouterConfigurationError(RouterException):
    """Raised when router configuration is invalid."""
    pass

@dataclass
class HealthResponse:
    """Standard health check response."""
    status: str
    version: str
    details: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "status": self.status,
            "version": self.version,
            "details": self.details
        }

class BaseRouterFactory(ABC):
    """Base factory for creating routers."""
    
    @abstractmethod
    def create_router(self, metadata: RouterMetadata) -> BaseRouter:
        """Create a new router instance."""
        pass

    @abstractmethod
    def get_router(self, router_type: str) -> Optional[BaseRouter]:
        """Get an existing router instance."""
        pass

    @abstractmethod
    def initialize_all(self) -> None:
        """Initialize all router instances."""
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """Clean up router resources."""
        pass

    @abstractmethod
    def health_check(self) -> HealthResponse:
        """Check health of all routers."""
        pass