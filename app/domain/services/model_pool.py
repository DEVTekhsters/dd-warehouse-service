from __future__ import annotations
import asyncio
import logging
from typing import Dict, Any, Optional, Callable, TypeVar, Generic, AsyncIterator
from contextlib import asynccontextmanager
from app.domain.models.memory_manager import MemoryManager

logger = logging.getLogger(__name__)

T = TypeVar('T')

class PoolExhaustedError(Exception):
    """Raised when the model pool is exhausted."""
    pass

class ModelPoolItem(Generic[T]):
    """Represents a model instance in the pool."""
    
    def __init__(self, model: T, last_used: float = 0):
        self.model = model
        self.last_used = last_used
        self.in_use = False
        self.load_count = 0
        self.error_count = 0

class ModelPool(Generic[T]):
    """
    Thread-safe pool of model instances with automatic resource management.
    """
    
    def __init__(
        self,
        model_loader: Callable[[], T],
        max_size: int = 2,
        max_waiting: int = 10,
        timeout: float = 30.0
    ):
        self.model_loader = model_loader
        self.max_size = max_size
        self.max_waiting = max_waiting
        self.timeout = timeout
        
        self.items: Dict[int, ModelPoolItem[T]] = {}
        self.available = asyncio.Queue[int](maxsize=max_size)
        self._lock = asyncio.Lock()
        self.memory_manager = MemoryManager()
        self._initialized = False
        self._shutting_down = False

    async def initialize(self) -> None:
        """Initialize the pool with models."""
        if self._initialized:
            return

        async with self._lock:
            if self._initialized:  # Double-check under lock
                return
            
            for i in range(self.max_size):
                try:
                    if await self.memory_manager.check_memory():
                        model = await self._load_model()
                        self.items[i] = ModelPoolItem(model)
                        await self.available.put(i)
                    else:
                        logger.warning("Memory limit reached during pool initialization")
                        break
                except Exception as e:
                    logger.error(f"Error initializing model {i}: {e}")
                    break
            
            self._initialized = True
            logger.info(f"Model pool initialized with {len(self.items)} models")

    async def _load_model(self) -> T:
        """Load a new model instance."""
        if asyncio.iscoroutinefunction(self.model_loader):
            return await self.model_loader()
        return self.model_loader()

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[T]:
        """
        Acquire a model from the pool.
        Uses context manager for automatic release.
        """
        if not self._initialized:
            await self.initialize()

        if self._shutting_down:
            raise RuntimeError("Pool is shutting down")

        model_id = None
        try:
            model_id = await asyncio.wait_for(
                self.available.get(),
                timeout=self.timeout
            )
            
            item = self.items[model_id]
            item.in_use = True
            item.load_count += 1
            
            yield item.model

        except asyncio.TimeoutError:
            raise PoolExhaustedError("Timeout waiting for available model")
        
        finally:
            if model_id is not None:
                item = self.items[model_id]
                item.in_use = False
                
                # Check if model needs cleanup
                if item.error_count > 3:  # Reset model after too many errors
                    try:
                        if await self.memory_manager.check_memory():
                            item.model = await self._load_model()
                            item.error_count = 0
                    except Exception as e:
                        logger.error(f"Error reloading model {model_id}: {e}")
                
                await self.available.put(model_id)

    async def shutdown(self) -> None:
        """Shutdown the pool and cleanup resources."""
        self._shutting_down = True
        
        # Wait for all models to be returned to pool
        try:
            async with asyncio.timeout(self.timeout):
                while self.available.qsize() < len(self.items):
                    await asyncio.sleep(0.1)
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for models to be returned during shutdown")

        # Cleanup models
        for item in self.items.values():
            if hasattr(item.model, 'cleanup'):
                try:
                    await item.model.cleanup()
                except Exception as e:
                    logger.error(f"Error cleaning up model: {e}")

        self.items.clear()
        self._initialized = False
        self.memory_manager.cleanup()

    @property
    def size(self) -> int:
        """Get current pool size."""
        return len(self.items)

    @property
    def available_count(self) -> int:
        """Get number of available models."""
        return self.available.qsize()

    async def health_check(self) -> Dict[str, Any]:
        """Get pool health metrics."""
        return {
            "total_size": self.size,
            "available": self.available_count,
            "initialized": self._initialized,
            "shutting_down": self._shutting_down,
            "models": [
                {
                    "id": id,
                    "in_use": item.in_use,
                    "load_count": item.load_count,
                    "error_count": item.error_count
                }
                for id, item in self.items.items()
            ]
        }