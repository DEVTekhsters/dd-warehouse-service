from __future__ import annotations
import logging
from prometheus_client import Counter, Gauge, Histogram
from typing import Dict, Optional, Any, cast
import time
from dataclasses import dataclass
from app.domain.models.memory_manager import MemoryManager

logger = logging.getLogger(__name__)

@dataclass
class MetricLabels:
    """Standard labels for metrics."""
    customer_id: str = "unknown"
    model_type: str = "unknown"
    file_type: str = "unknown"
    status: str = "unknown"

class MetricsCollector:
    """
    Centralized metrics collection for PII scanning service.
    Implements singleton pattern for consistent metrics tracking.
    """
    _instance: Optional[MetricsCollector] = None

    def __new__(cls) -> MetricsCollector:
        if cls._instance is None:
            cls._instance = super(MetricsCollector, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self) -> None:
        """Initialize metrics collectors."""
        # Memory metrics
        self.memory_usage: Gauge = Gauge(
            'pii_scanner_memory_usage_bytes',
            'Current memory usage in bytes',
            ['component']
        )
        self.memory_limit: Gauge = Gauge(
            'pii_scanner_memory_limit_bytes',
            'Memory limit in bytes'
        )

        # Performance metrics
        self.scan_duration: Histogram = Histogram(
            'pii_scanner_scan_duration_seconds',
            'Time taken for PII scanning',
            ['customer_id', 'file_type'],
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0)
        )
        self.model_load_time: Histogram = Histogram(
            'pii_scanner_model_load_duration_seconds',
            'Time taken to load models',
            ['model_type'],
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0)
        )
        
        # Operational metrics
        self.files_processed: Counter = Counter(
            'pii_scanner_files_processed_total',
            'Total number of files processed',
            ['customer_id', 'file_type', 'status']
        )
        self.errors_total: Counter = Counter(
            'pii_scanner_errors_total',
            'Total number of errors',
            ['type', 'component']
        )

        # Resource metrics
        self.model_pool_size: Gauge = Gauge(
            'pii_scanner_model_pool_size',
            'Current size of model pools',
            ['model_type']
        )
        self.model_pool_utilization: Gauge = Gauge(
            'pii_scanner_model_pool_utilization',
            'Current utilization of model pools',
            ['model_type']
        )

        # Initialize memory manager for monitoring
        self.memory_manager = MemoryManager()
        self.memory_limit.set(float(self.memory_manager._memory_limit))

    def track_memory_usage(self, component: str = "main") -> None:
        """Track current memory usage."""
        try:
            usage = self.memory_manager.get_memory_usage()
            self.memory_usage.labels(component=component).set(float(usage))
        except Exception as e:
            logger.error(f"Error tracking memory usage: {e}")
            self.errors_total.labels(type="monitoring", component="memory").inc()

    def track_scan_duration(self, duration: float, labels: MetricLabels) -> None:
        """Track scan duration with labels."""
        self.scan_duration.labels(
            customer_id=labels.customer_id,
            file_type=labels.file_type
        ).observe(duration)

    def track_file_processed(self, labels: MetricLabels) -> None:
        """Track processed file with status."""
        self.files_processed.labels(
            customer_id=labels.customer_id,
            file_type=labels.file_type,
            status=labels.status
        ).inc()

    def track_error(self, error_type: str, component: str) -> None:
        """Track error occurrence."""
        self.errors_total.labels(
            type=error_type,
            component=component
        ).inc()

    def track_model_pool(self, model_type: str, size: int, in_use: int) -> None:
        """Track model pool metrics."""
        self.model_pool_size.labels(model_type=model_type).set(float(size))
        if size > 0:
            utilization = float(in_use) / float(size)
            self.model_pool_utilization.labels(model_type=model_type).set(utilization)

    async def collect_all_metrics(self) -> Dict[str, Any]:
        """
        Collect all current metrics.
        Useful for health checks and monitoring.
        """
        self.track_memory_usage()
        
        files_processed = cast(float, self.files_processed._value.sum())
        errors_total = cast(float, self.errors_total._value.sum())
        scan_duration_sum = cast(float, self.scan_duration._sum.sum())
        scan_duration_count = max(cast(float, self.scan_duration._count.sum()), 1.0)
        
        return {
            "memory": {
                "current_usage": self.memory_manager.get_memory_usage(),
                "limit": self.memory_manager._memory_limit,
                "usage_percent": (float(self.memory_manager.get_memory_usage()) / 
                                float(self.memory_manager._memory_limit) * 100.0)
            },
            "processing": {
                "files_processed": {
                    "success": files_processed,
                    "error": errors_total
                },
                "average_duration": scan_duration_sum / scan_duration_count
            }
        }

class MetricsTimer:
    """Context manager for timing operations."""
    
    def __init__(self, labels: MetricLabels):
        self.labels = labels
        self.start_time: float = 0.0
        self.metrics = MetricsCollector()

    async def __aenter__(self) -> MetricsTimer:
        self.start_time = time.time()
        return self

    async def __aexit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> None:
        duration = time.time() - self.start_time
        if exc_type is None:
            self.labels.status = "success"
        else:
            self.labels.status = "error"
            self.metrics.track_error(str(exc_type), "scanning")
        
        self.metrics.track_scan_duration(duration, self.labels)
        self.metrics.track_file_processed(self.labels)