from __future__ import annotations
from contextlib import AbstractAsyncContextManager
from typing import Optional, Any
from pathlib import Path
import logging
from tempfile import SpooledTemporaryFile
from datetime import datetime
from fastapi import HTTPException

logger = logging.getLogger(__name__)

class FileError(Exception):
    """Base class for file operation errors."""
    pass

class FileWriteError(FileError):
    """Error writing temporary file."""
    pass

class FileCleanupError(FileError):
    """Error cleaning up temporary file."""
    pass

class TemporaryFileManager(AbstractAsyncContextManager[Path]):
    """
    Context manager for handling temporary files with proper cleanup.
    Guarantees cleanup of temporary files even in error cases.
    """
    
    def __init__(self, spooled_file: SpooledTemporaryFile, filename: str) -> None:
        """Initialize with spooled file and target filename."""
        self.spooled_file = spooled_file
        self.filename = filename
        timestamp = datetime.now().timestamp()
        self.temp_path = Path(f"/tmp/pii_scanner_{timestamp}_{filename}")
        self._cleanup_attempted = False

    async def __aenter__(self) -> Path:
        """
        Save spooled file to temporary location.
        Always returns a Path or raises an exception.
        """
        try:
            # Ensure parent directory exists
            self.temp_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write file contents
            with open(self.temp_path, 'wb') as f:
                self.spooled_file.seek(0)
                f.write(self.spooled_file.read())
            
            # Verify file was written
            if not self.temp_path.exists():
                raise FileWriteError("Failed to create temporary file")
            
            return self.temp_path
            
        except Exception as e:
            logger.error(f"Error saving temporary file: {e}")
            # Clean up if file was partially written
            await self._cleanup()
            if isinstance(e, FileError):
                raise HTTPException(status_code=500, detail=str(e))
            raise HTTPException(status_code=500, detail="Error processing uploaded file")

    async def __aexit__(
        self, 
        exc_type: Optional[type], 
        exc_val: Optional[BaseException], 
        exc_tb: Optional[Any]
    ) -> bool:
        """
        Ensure cleanup of temporary file.
        Returns False to not suppress exceptions.
        """
        try:
            await self._cleanup()
            return False  # Don't suppress exceptions
        except Exception as e:
            logger.error(f"Error in __aexit__ cleanup: {e}")
            return False

    async def _cleanup(self) -> None:
        """
        Perform actual file cleanup.
        Ensures cleanup is only attempted once.
        """
        if self._cleanup_attempted:
            return

        self._cleanup_attempted = True
        try:
            if self.temp_path.exists():
                self.temp_path.unlink()
        except Exception as e:
            logger.error(f"Error cleaning up temporary file: {e}")
            raise FileCleanupError(f"Failed to clean up temporary file: {e}")