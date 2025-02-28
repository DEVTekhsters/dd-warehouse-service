from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, NoReturn, TypeVar, Callable, cast
from fastapi import APIRouter

T = TypeVar('T')

@dataclass
class RouteDefinition:
    """Definition of a route."""
    path: str
    method: str
    handler: Callable[..., Any]
    response_model: Optional[type] = None
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)

class RouteSetupError(Exception):
    """Base exception for route setup errors."""
    pass

class InvalidRouteError(RouteSetupError):
    """Raised when route configuration is invalid."""
    pass

class EndpointRegistrar:
    """Handles route registration with proper type checking."""

    HTTP_METHODS = {
        'GET', 'POST', 'PUT', 'DELETE', 'PATCH'
    }

    def __init__(self, router: APIRouter) -> None:
        self.router = router
        self._method_map: Dict[str, Callable] = {
            'GET': self.router.get,
            'POST': self.router.post,
            'PUT': self.router.put,
            'DELETE': self.router.delete,
            'PATCH': self.router.patch
        }

    def validate_route(self, route: RouteDefinition) -> None:
        """Validate route configuration."""
        if not route.path:
            raise InvalidRouteError("Route path cannot be empty")
        
        if not route.method:
            raise InvalidRouteError("HTTP method cannot be empty")
            
        if route.method.upper() not in self.HTTP_METHODS:
            raise InvalidRouteError(f"Unsupported HTTP method: {route.method}")
            
        if not callable(route.handler):
            raise InvalidRouteError("Route handler must be callable")

    def register_route(self, route: RouteDefinition) -> None:
        """Register a single route with proper error handling."""
        self.validate_route(route)
        
        method = self._method_map.get(route.method.upper())
        if not method:  # This should never happen due to validation
            raise InvalidRouteError(f"Unsupported HTTP method: {route.method}")

        kwargs: Dict[str, Any] = {}
        if route.response_model is not None:
            kwargs['response_model'] = route.response_model
        if route.description:
            kwargs['description'] = route.description
        if route.tags:
            kwargs['tags'] = route.tags

        # Register the route
        method(route.path, **kwargs)(route.handler)

def setup_routes(router: APIRouter, routes: List[RouteDefinition]) -> None:
    """
    Set up routes with proper error handling and type checking.
    Returns None explicitly for type checking.
    """
    try:
        registrar = EndpointRegistrar(router)
        for route in routes:
            registrar.register_route(route)
    except Exception as e:
        raise RouteSetupError(f"Failed to setup routes: {str(e)}")

def create_route(
    path: str,
    method: str,
    handler: Callable[..., Any],
    response_model: Optional[type] = None,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None
) -> RouteDefinition:
    """Create a route definition with proper type checking."""
    return RouteDefinition(
        path=path,
        method=method,
        handler=handler,
        response_model=response_model,
        description=description,
        tags=tags if tags is not None else []
    )

def create_router(
    routes: List[RouteDefinition],
    prefix: Optional[str] = None,
    tags: Optional[List[str]] = None
) -> APIRouter:
    """Create a new router with the specified routes."""
    router_tags = tags if tags is not None else []
    router = APIRouter(prefix=prefix, tags=router_tags)
    setup_routes(router, routes)
    return router