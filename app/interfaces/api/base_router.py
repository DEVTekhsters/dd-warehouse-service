from __future__ import annotations
from typing import (
    List, Optional, NoReturn, Dict, Any, TypeVar, Callable, 
    Awaitable, cast, Union, Protocol, runtime_checkable
)
import logging
import asyncio
from functools import wraps
from collections.abc import Awaitable as AwaitableABC
from fastapi import APIRouter, HTTPException
from app.interfaces.api.route_setup import RouteDefinition, setup_routes
from app.interfaces.api.initializers import ManagedResource

logger = logging.getLogger(__name__)

T = TypeVar('T')

@runtime_checkable
class SupportsInitialize(Protocol):
    """Protocol for objects that support initialization."""
    def initialize(self) -> bool: ...
    _initialized: bool
    initialization_error: Optional[str]

class NoImplementationError(NotImplementedError):
    """Raised when a required implementation is missing."""
    pass

class RouterSetupError(Exception):
    """Raised when router setup fails."""
    pass

def is_async_callable(func: Callable[..., Any]) -> bool:
    """Check if a callable is async."""
    return asyncio.iscoroutinefunction(func)

class BaseAPIRouter(ManagedResource):
    """
    Base class for API routers with proper route setup handling.
    """

    def __init__(self) -> None:
        """Initialize base router."""
        super().__init__()
        self.router = APIRouter()
        self._routes: List[RouteDefinition] = []
        self._initialized = False

    def add_route(self, route: RouteDefinition) -> None:
        """Add a route to the router."""
        self._routes.append(route)

    def initialize(self) -> bool:
        """
        Initialize the router.
        Returns True if initialization was successful.
        """
        if self._initialized:
            return True

        try:
            setup_routes(self.router, self._routes)
            self._initialized = True
            self._mark_initialized()
            return True
        except Exception as e:
            self._mark_failed(f"Route setup failed: {str(e)}")
            return False

    def include_router(
        self,
        router: APIRouter,
        *,
        prefix: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> None:
        """Include a sub-router."""
        if not self._initialized:
            self.initialize()
        self.router.include_router(
            router,
            prefix=prefix,
            tags=tags or []
        )

    def get_router(self) -> APIRouter:
        """Get the configured router."""
        if not self._initialized:
            self.initialize()
        return self.router

    @property
    def routes(self) -> List[RouteDefinition]:
        """Get registered routes."""
        return self._routes.copy()

    async def cleanup(self) -> None:
        """Clean up router resources."""
        self._routes.clear()
        self._initialized = False
        self._mark_failed("Router cleaned up")

    def _setup_routes(self) -> bool:
        """
        Internal method to set up routes.
        Returns True if setup was successful.
        """
        try:
            setup_routes(self.router, self._routes)
            return True
        except Exception as e:
            logger.error(f"Route setup failed: {e}")
            return False

def require_initialized(method: Callable[..., T]) -> Callable[..., Awaitable[T]]:
    """
    Decorator that ensures a method is called on an initialized router.
    Works with both sync and async methods.
    """
    @wraps(method)
    async def wrapper(self: SupportsInitialize, *args: Any, **kwargs: Any) -> T:
        if not self._initialized and not self.initialize():
            raise RouterSetupError(
                f"Router not initialized: {self.initialization_error}"
            )

        if is_async_callable(method):
            return await method(self, *args, **kwargs)  # type: ignore
        return cast(T, method(self, *args, **kwargs))

    return wrapper

class AsyncRouteHandler:
    """Base class for async route handlers with proper error handling."""

    @staticmethod
    async def handle_request(
        handler: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """Handle an async request with proper error handling."""
        try:
            return await handler(*args, **kwargs)
        except NoImplementationError:
            raise HTTPException(
                status_code=501,
                detail="Not implemented"
            )
        except RouterSetupError as e:
            raise HTTPException(
                status_code=500,
                detail=f"Router setup error: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Unexpected error in route handler: {e}")
            raise HTTPException(
                status_code=500,
                detail="Internal server error"
            )

async def not_implemented_endpoint(*args: Any, **kwargs: Any) -> NoReturn:
    """Default handler for not implemented endpoints."""
    raise NoImplementationError("Endpoint not implemented")