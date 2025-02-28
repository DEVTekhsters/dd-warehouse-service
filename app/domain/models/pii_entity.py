from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import datetime

@dataclass
class PIIEntity:
    """
    Domain model representing a PII entity found in text.
    """
    entity_type: str
    text: str
    confidence_score: float
    start_position: Optional[int] = None
    end_position: Optional[int] = None

@dataclass
class ScanResult:
    """
    Domain model representing the result of a PII scan.
    """
    file_name: str
    file_size: int
    scan_timestamp: datetime
    entities: List[PIIEntity]
    metadata: Dict[str, str]

@dataclass
class ScanConfig:
    """
    Domain model for scan configuration parameters.
    """
    sample_size: float = 0.2
    chunk_size: int = 1024 * 1024  # 1MB default chunk size
    max_memory_usage: int = 1024 * 1024 * 1024  # 1GB default max memory
    batch_size: int = 1000