from __future__ import annotations

import resource
import gc
import torch
import logging
import psutil
import time
import asyncio
from typing import Dict, Any, Optional, Callable, List, cast
from dataclasses import dataclass
from functools import wraps

logger = logging.getLogger(__name__)

@dataclass
class MemoryThresholds:
    """Memory thresholds configuration."""
    warning: float = 0.75    # 75% of limit
    critical: float = 0.85   # 85% of limit
    circuit_break: float = 0.90  # 90% of limit

class MemoryCircuitBreaker:
    """Circuit breaker for memory protection."""
    
    def __init__(self, threshold_mb: int = 1400):  # 1.4GB threshold
        self.threshold: int = threshold_mb * 1024 * 1024
        self.tripped: bool = False
        self.cooldown_time: int = 60  # seconds
        self.last_trip_time: float = 0.0
        self.consecutive_trips: int = 0
        self.max_consecutive_trips: int = 3
        self._callbacks: Dict[str, Callable[[], None]] = {}

    def register_callback(self, name: str, callback: Callable[[], None]) -> None:
        """Register a callback for circuit breaker events."""
        self._callbacks[name] = callback

    async def check(self) -> bool:
        """
        Check if memory usage is within safe limits.
        Returns False if circuit breaker is tripped.
        """
        current_time = time.time()
        
        # Check if we're in cooldown
        if self.tripped and (current_time - self.last_trip_time) < self.cooldown_time:
            return False

        current_usage = psutil.Process().memory_info().rss
        if current_usage > self.threshold:
            await self.trip()
            return False
            
        # Reset trip count if we've recovered
        self.consecutive_trips = 0
        self.tripped = False
        return True

    async def trip(self) -> None:
        """Trip the circuit breaker and execute callbacks."""
        self.tripped = True
        self.last_trip_time = time.time()
        self.consecutive_trips += 1
        
        # Execute callbacks
        for name, callback in self._callbacks.items():
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback()
                else:
                    callback()
            except Exception as e:
                logger.error(f"Error in circuit breaker callback {name}: {e}")

        # Force cleanup if we're tripping too frequently
        if self.consecutive_trips >= self.max_consecutive_trips:
            logger.critical("Maximum consecutive trips reached - forcing aggressive cleanup")
            await self.force_cleanup()

    async def force_cleanup(self) -> None:
        """Aggressive memory cleanup."""
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        
        # Force Python to return memory to the system
        try:
            import ctypes
            libc = ctypes.CDLL('libc.so.6')
            cast(Any, libc.malloc_trim(0))
        except Exception as e:
            logger.error(f"Error in aggressive memory cleanup: {e}")

class MemoryManager:
    """
    Memory management service with circuit breaker protection.
    Implements singleton pattern to ensure consistent memory management.
    """
    _instance: Optional[MemoryManager] = None
    
    def __new__(cls) -> MemoryManager:
        if cls._instance is None:
            cls._instance = super(MemoryManager, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self) -> None:
        """Initialize memory management settings."""
        self._memory_limit: int = int(1.5 * 1024 * 1024 * 1024)  # 1.5GB in bytes
        self.thresholds = MemoryThresholds()
        self.circuit_breaker = MemoryCircuitBreaker()
        self._setup_callbacks()

    def _setup_callbacks(self) -> None:
        """Setup circuit breaker callbacks."""
        self.circuit_breaker.register_callback(
            "cleanup", 
            self.cleanup
        )
        self.circuit_breaker.register_callback(
            "log_memory", 
            lambda: logger.warning(f"Memory usage: {self.get_memory_usage() / (1024*1024):.2f}MB")
        )

    def set_resource_limits(self) -> None:
        """Set system resource limits."""
        try:
            resource.setrlimit(resource.RLIMIT_AS, (self._memory_limit, resource.RLIM_INFINITY))
            logger.info(f"Memory limit set to {self._memory_limit / (1024*1024*1024):.2f}GB")
        except (ValueError, resource.error) as e:
            logger.warning(f"Could not set memory limit: {e}")

    def get_memory_usage(self) -> int:
        """Get current memory usage in bytes."""
        return psutil.Process().memory_info().rss

    async def check_memory(self) -> bool:
        """
        Check if memory usage is within acceptable limits.
        Returns False if memory usage is critical.
        """
        current_usage = self.get_memory_usage()
        usage_ratio = current_usage / self._memory_limit

        if usage_ratio >= self.thresholds.circuit_break:
            logger.critical(f"Memory usage critical: {usage_ratio:.2%}")
            return await self.circuit_breaker.check()
            
        if usage_ratio >= self.thresholds.critical:
            logger.error(f"Memory usage high: {usage_ratio:.2%}")
            self.cleanup()
            
        elif usage_ratio >= self.thresholds.warning:
            logger.warning(f"Memory usage elevated: {usage_ratio:.2%}")

        return True

    def cleanup(self) -> None:
        """Perform memory cleanup."""
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

    @staticmethod
    async def memory_managed(func: Callable[..., Any]) -> Callable[..., Any]:
        """
        Decorator to manage memory for functions.
        Ensures cleanup after function execution.
        """
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            manager = MemoryManager()
            try:
                if not await manager.check_memory():
                    raise MemoryError("Insufficient memory to proceed")
                result = await func(*args, **kwargs)
                return result
            finally:
                manager.cleanup()
        return wrapper

    async def __aenter__(self) -> MemoryManager:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> bool:
        """Async context manager exit with cleanup."""
        self.cleanup()
        return False  # Don't suppress exceptions