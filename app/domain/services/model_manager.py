from __future__ import annotations
from typing import Dict, Optional, Any, Callable
import spacy
from gliner import GLiNER
import logging
import weakref
from pathlib import Path
from app.domain.models.memory_manager import MemoryManager
from app.domain.services.scanner_interface import ModelManagerInterface
from app.domain.services.model_pool import ModelPool, PoolExhaustedError

logger = logging.getLogger(__name__)

class ModelConfig:
    """Configuration for model loading and management."""
    SPACY_MODEL = "en_core_web_sm"
    GLINER_MODEL = "urchade/gliner_multi_pii-v1"
    GLINER_CACHE = "local_pii_model"
    MAX_POOL_SIZE = 2
    POOL_TIMEOUT = 30.0

class ModelLoadError(Exception):
    """Raised when model loading fails."""
    pass

class MLModelManager(ModelManagerInterface):
    """
    Enhanced model manager with pool management and memory protection.
    Implements Singleton pattern for consistent resource management.
    """
    _instance: Optional[MLModelManager] = None
    
    def __new__(cls) -> MLModelManager:
        if cls._instance is None:
            cls._instance = super(MLModelManager, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self) -> None:
        """Initialize model pools and memory management."""
        self.memory_manager = MemoryManager()
        self._setup_model_pools()

    def _setup_model_pools(self) -> None:
        """Setup model pools for different model types."""
        self.pools: Dict[str, ModelPool] = {
            'spacy': ModelPool(
                model_loader=self._load_spacy_model,
                max_size=ModelConfig.MAX_POOL_SIZE,
                timeout=ModelConfig.POOL_TIMEOUT
            ),
            'gliner': ModelPool(
                model_loader=self._load_gliner_model,
                max_size=ModelConfig.MAX_POOL_SIZE,
                timeout=ModelConfig.POOL_TIMEOUT
            )
        }

    async def _load_spacy_model(self) -> Any:
        """Load SpaCy model with memory check."""
        try:
            if not await self.memory_manager.check_memory():
                raise MemoryError("Insufficient memory to load SpaCy model")
            
            # Load with minimal components for memory efficiency
            return spacy.load(
                ModelConfig.SPACY_MODEL,
                disable=['parser', 'tagger']
            )
        except Exception as e:
            logger.error(f"Error loading SpaCy model: {e}")
            raise ModelLoadError(f"Failed to load SpaCy model: {e}")

    async def _load_gliner_model(self) -> Any:
        """Load GLiNER model with memory check."""
        try:
            if not await self.memory_manager.check_memory():
                raise MemoryError("Insufficient memory to load GLiNER model")
            
            cache_dir = Path(ModelConfig.GLINER_CACHE)
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            return GLiNER.from_pretrained(
                ModelConfig.GLINER_MODEL,
                cache_dir=str(cache_dir)
            )
        except Exception as e:
            logger.error(f"Error loading GLiNER model: {e}")
            raise ModelLoadError(f"Failed to load GLiNER model: {e}")

    async def get_model(self, model_type: str = 'spacy') -> Any:
        """
        Get a model from the appropriate pool.
        Implements retry logic for pool exhaustion.
        """
        if model_type not in self.pools:
            raise ValueError(f"Unsupported model type: {model_type}")

        pool = self.pools[model_type]
        
        try:
            async with pool.acquire() as model:
                return model
        except PoolExhaustedError:
            logger.error(f"Model pool exhausted for {model_type}")
            # Force cleanup and retry once
            self.memory_manager.cleanup()
            async with pool.acquire() as model:
                return model

    async def release_model(self, model_type: str) -> None:
        """Release all models of specified type."""
        if model_type in self.pools:
            await self.pools[model_type].shutdown()

    async def cleanup(self) -> None:
        """Cleanup all model resources."""
        for pool in self.pools.values():
            await pool.shutdown()
        self.memory_manager.cleanup()

    async def health_check(self) -> Dict[str, Any]:
        """Get health status of all model pools."""
        return {
            model_type: await pool.health_check()
            for model_type, pool in self.pools.items()
        }

    async def __aenter__(self) -> MLModelManager:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> bool:
        """Async context manager exit."""
        await self.cleanup()
        return False  # Don't suppress exceptions