from __future__ import annotations
import os
import asyncio
import aiofiles
from typing import Dict, Any, AsyncIterator, Optional
from pathlib import Path
import logging
from app.domain.models.pii_entity import ScanConfig
from app.domain.services.scanner_interface import FileProcessorInterface
from app.domain.models.memory_manager import MemoryManager

logger = logging.getLogger(__name__)

class StreamingFileProcessor(FileProcessorInterface):
    """
    Handles file operations with streaming to manage memory usage.
    Implements memory-efficient file processing strategies.
    """

    def __init__(self):
        """Initialize with memory manager."""
        self.memory_manager = MemoryManager()
        self.supported_extensions: Dict[str, set[str]] = {
            'text': {'.txt', '.csv', '.json'},
            'document': {'.pdf', '.docx', '.xlsx'},
            'image': {'.jpg', '.jpeg', '.png'}
        }

    def process_file(self, file_path: Path, config: ScanConfig) -> AsyncIterator[bytes]:
        """
        Process file in chunks to maintain memory efficiency.
        Yields chunks of file data for streaming processing.
        """
        return self._process_file_impl(file_path, config)

    async def _process_file_impl(self, file_path: Path, config: ScanConfig) -> AsyncIterator[bytes]:
        """
        Internal implementation of file processing.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Validate file size before processing
        file_size = os.path.getsize(file_path)
        if file_size > config.max_memory_usage:
            raise ValueError(
                f"File size ({file_size} bytes) exceeds maximum allowed size "
                f"({config.max_memory_usage} bytes)"
            )

        file_type = self._get_file_type(file_path)
        
        try:
            async for chunk in self._stream_file(file_path, file_type, config):
                # Check memory usage before yielding each chunk
                if not self.memory_manager.check_memory():
                    logger.warning("Memory usage high, forcing cleanup")
                    self.memory_manager.force_cleanup()
                yield chunk
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            raise

    async def _stream_file(
        self, 
        file_path: Path, 
        file_type: str, 
        config: ScanConfig
    ) -> AsyncIterator[bytes]:
        """
        Stream file content based on file type.
        Implements different streaming strategies for different file types.
        """
        if file_type == 'text':
            async with aiofiles.open(file_path, 'rb') as file:
                while True:
                    chunk = await file.read(config.chunk_size)
                    if not chunk:
                        break
                    yield chunk
        else:
            # For binary files, use specialized streaming
            async for chunk in self._stream_binary_file(file_path, file_type, config):
                yield chunk

    async def _stream_binary_file(
        self, 
        file_path: Path, 
        file_type: str, 
        config: ScanConfig
    ) -> AsyncIterator[bytes]:
        """
        Stream binary files with specialized handling per type.
        Uses appropriate chunking strategies for different file types.
        """
        # Implement specialized streaming for different file types
        if file_type == 'document':
            async for chunk in self._stream_document(file_path, config):
                yield chunk
        elif file_type == 'image':
            async for chunk in self._stream_image(file_path, config):
                yield chunk
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    async def _stream_document(self, file_path: Path, config: ScanConfig) -> AsyncIterator[bytes]:
        """
        Stream document files in chunks.
        Specialized handling for document formats.
        """
        async with aiofiles.open(file_path, 'rb') as file:
            while True:
                chunk = await file.read(config.chunk_size)
                if not chunk:
                    break
                yield chunk

    async def _stream_image(self, file_path: Path, config: ScanConfig) -> AsyncIterator[bytes]:
        """
        Stream image files in chunks.
        Specialized handling for image formats.
        """
        async with aiofiles.open(file_path, 'rb') as file:
            while True:
                chunk = await file.read(config.chunk_size)
                if not chunk:
                    break
                yield chunk

    def _get_file_type(self, file_path: Path) -> str:
        """
        Determine file type based on extension.
        """
        extension = file_path.suffix.lower()
        for file_type, extensions in self.supported_extensions.items():
            if extension in extensions:
                return file_type
        raise ValueError(f"Unsupported file extension: {extension}")

    async def get_file_info(self, file_path: Path) -> Dict[str, Any]:
        """
        Get file metadata and information.
        Returns file statistics and type information.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        stats = os.stat(file_path)
        return {
            'name': file_path.name,
            'size': stats.st_size,
            'created': stats.st_ctime,
            'modified': stats.st_mtime,
            'type': self._get_file_type(file_path),
            'extension': file_path.suffix.lower()
        }

    async def __aenter__(self) -> StreamingFileProcessor:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> bool:
        """Async context manager exit with cleanup."""
        self.memory_manager.cleanup()
        return False  # Don't suppress exceptions