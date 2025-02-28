from __future__ import annotations
from typing import AsyncIterator

class ChunkIterator(AsyncIterator[bytes]):
    """
    Memory-efficient async iterator for byte chunks.
    Implements proper async iteration protocol.
    """
    
    def __init__(self, data: bytes) -> None:
        self.data = data
        self._consumed = False

    async def __anext__(self) -> bytes:
        """Return the chunk and mark as consumed."""
        if self._consumed:
            raise StopAsyncIteration
        self._consumed = True
        return self.data