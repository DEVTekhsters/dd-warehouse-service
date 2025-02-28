from __future__ import annotations
from typing import Protocol, Any, NoReturn, Optional
from abc import ABC, abstractmethod

class Initializable(Protocol):
    """Protocol for objects that require initialization."""
    
    def initialize(self) -> bool:
        """Initialize the object."""
        ...

class CleanupProtocol(Protocol):
    """Protocol for objects that require cleanup."""
    
    async def cleanup(self) -> None:
        """Clean up resources."""
        ...

class InitializationError(Exception):
    """Base class for initialization errors."""
    pass

class ResourceInitializer(ABC):
    """Base class for resource initialization."""
    
    @abstractmethod
    def initialize(self) -> bool:
        """
        Initialize resources.
        Returns True if successful, False otherwise.
        """
        pass

    @abstractmethod
    async def cleanup(self) -> None:
        """Clean up initialized resources."""
        pass

class InitializationStatus:
    """Track initialization status and errors."""

    def __init__(self) -> None:
        self.initialized: bool = False
        self.error: Optional[str] = None
        self.details: dict[str, Any] = {}

    def mark_success(self) -> None:
        """Mark initialization as successful."""
        self.initialized = True
        self.error = None

    def mark_failure(self, error: str) -> None:
        """Mark initialization as failed."""
        self.initialized = False
        self.error = error

    def add_details(self, key: str, value: Any) -> None:
        """Add initialization details."""
        self.details[key] = value

    def is_initialized(self) -> bool:
        """Check if initialization was successful."""
        return self.initialized

class InitializationManager:
    """Manage initialization of multiple components."""

    def __init__(self) -> None:
        self.components: dict[str, InitializationStatus] = {}

    def register(self, name: str) -> InitializationStatus:
        """Register a component for initialization tracking."""
        status = InitializationStatus()
        self.components[name] = status
        return status

    def get_status(self, name: str) -> Optional[InitializationStatus]:
        """Get initialization status for a component."""
        return self.components.get(name)

    def all_initialized(self) -> bool:
        """Check if all components are initialized."""
        return all(
            status.is_initialized() 
            for status in self.components.values()
        )

    def get_errors(self) -> dict[str, str]:
        """Get initialization errors for all components."""
        return {
            name: status.error
            for name, status in self.components.items()
            if status.error is not None
        }

class ManagedResource(ResourceInitializer):
    """Base class for resources with managed initialization."""

    def __init__(self) -> None:
        self._status = InitializationStatus()

    @property
    def is_initialized(self) -> bool:
        """Check if resource is initialized."""
        return self._status.is_initialized()

    @property
    def initialization_error(self) -> Optional[str]:
        """Get initialization error if any."""
        return self._status.error

    def _mark_initialized(self) -> None:
        """Mark resource as initialized."""
        self._status.mark_success()

    def _mark_failed(self, error: str) -> None:
        """Mark resource initialization as failed."""
        self._status.mark_failure(error)

    def initialize(self) -> bool:
        """
        Initialize the resource.
        Must be implemented by subclasses.
        """
        raise NotImplementedError

    async def cleanup(self) -> None:
        """
        Clean up the resource.
        Must be implemented by subclasses.
        """
        raise NotImplementedError