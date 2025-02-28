from __future__ import annotations
from fastapi import HTTPException, UploadFile
from typing import Set, Dict, Optional
from pathlib import Path

class ValidationError(Exception):
    """Base class for validation errors."""
    pass

class FileValidators:
    """File validation utilities with proper type checking."""

    def __init__(
        self,
        max_size: int,
        allowed_extensions: Dict[str, Set[str]]
    ) -> None:
        self.max_size = max_size
        self.allowed_extensions = allowed_extensions

    def validate_extension(self, filename: str) -> str:
        """
        Validate file extension and return file type.
        Raises HTTPException if extension is not allowed.
        """
        if not filename:
            raise ValidationError("Filename cannot be empty")

        ext = Path(filename).suffix.lower()
        for file_type, extensions in self.allowed_extensions.items():
            if ext in extensions:
                return file_type

        allowed = ', '.join(
            ext for exts in self.allowed_extensions.values() 
            for ext in exts
        )
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type. Allowed extensions: {allowed}"
        )

    async def validate_size(self, file: UploadFile) -> None:
        """
        Validate file size with efficient streaming.
        Raises HTTPException if file size exceeds limit.
        """
        if not file.filename:
            raise ValidationError("File has no filename")

        try:
            file_size = 0
            while True:
                chunk = await file.read(8192)  # 8KB chunks for efficiency
                if not chunk:
                    break
                file_size += len(chunk)
                if file_size > self.max_size:
                    raise ValidationError(
                        f"File {file.filename} exceeds maximum size of "
                        f"{self.max_size/1024/1024:.1f}MB"
                    )
        except Exception as e:
            if not isinstance(e, ValidationError):
                raise ValidationError(f"Error reading file: {e}")
            raise
        finally:
            await file.seek(0)  # Always reset file position

    async def validate_file(
        self, 
        file: UploadFile,
        required: bool = True
    ) -> Optional[str]:
        """
        Perform all validations on a file.
        Returns file type if valid, raises HTTPException otherwise.
        """
        if not file.filename:
            if required:
                raise HTTPException(
                    status_code=400,
                    detail="File is required"
                )
            return None

        try:
            await self.validate_size(file)
            return self.validate_extension(file.filename)
        except ValidationError as e:
            raise HTTPException(
                status_code=400,
                detail=str(e)
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Validation error: {str(e)}"
            )