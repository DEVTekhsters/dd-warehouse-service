from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union
from fastapi import APIRouter

T = TypeVar('T')

@dataclass
class RouteConfig:
    """Configuration for a single route."""
    path: str
    method: str
    handler: Callable[..., Any]
    response_model: Optional[type] = None
    name: Optional[str] = None
    description: Optional[str] = None

class RouteManager:
    """Handles route setup and registration with proper type management."""

    def __init__(self) -> None:
        """Initialize route manager."""
        self.router = APIRouter()
        self._routes: List[RouteConfig] = []

    def add_route(self, config: RouteConfig) -> None:
        """Add a route configuration."""
        self._routes.append(config)

    def register_routes(self) -> None:
        """Register all routes with the router."""
        method_map: Dict[str, Callable] = {
            'GET': self.router.get,
            'POST': self.router.post,
            'PUT': self.router.put,
            'DELETE': self.router.delete,
            'PATCH': self.router.patch
        }

        for route in self._routes:
            method = method_map.get(route.method.upper())
            if method is None:
                raise ValueError(f"Unsupported HTTP method: {route.method}")

            kwargs: Dict[str, Any] = {
                'response_model': route.response_model
            }
            if route.name:
                kwargs['name'] = route.name
            if route.description:
                kwargs['description'] = route.description

            method(route.path, **kwargs)(route.handler)

    def get_router(self) -> APIRouter:
        """Get the configured router."""
        return self.router

class RouteBinder:
    """Helper class for binding routes to handlers."""

    @staticmethod
    def bind_routes(
        router: APIRouter,
        routes: List[RouteConfig]
    ) -> None:
        """Bind routes to the provided router."""
        manager = RouteManager()
        for route in routes:
            manager.add_route(route)
        manager.register_routes()
        
        # Copy routes to the provided router
        router.routes.extend(manager.get_router().routes)

    @staticmethod
    def create_route(
        path: str,
        method: str,
        handler: Callable[..., Any],
        response_model: Optional[type] = None,
        name: Optional[str] = None,
        description: Optional[str] = None
    ) -> RouteConfig:
        """Create a route configuration."""
        return RouteConfig(
            path=path,
            method=method,
            handler=handler,
            response_model=response_model,
            name=name,
            description=description
        )