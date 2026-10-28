from __future__ import annotations

from dataclasses import dataclass

@dataclass(frozen=True)
class Config:
    ''
    num_urls: int = 200
    producer_interval_min: float = 0.01
    producer_interval_max: float = 0.1
    num_downloaders: int = 5
    num_analyzers: int = 3
    num_savers: int = 2
    queue_size: int = 20
    max_connections: int = 5
    timeout_seconds: float = 2.0
    max_retry: int = 3
    backoff_base: float = 0.5
    backoff_multiplier: float = 2.0
    backoff_max: float = 10.0
    backoff_jitter: float = 0.3
    monitor_interval: float = 2.0
    download_fail_rate: float = 0.4
    analysis_fail_rate: float = 0.4
    save_fail_rate: float = 0.4
    download_latency_min: float = 0.05
    download_latency_max: float = 1.5
    analysis_latency_min: float = 0.01
    analysis_latency_max: float = 0.3
    save_latency_min: float = 0.02
    save_latency_max: float = 0.5
    log_level: str = "INFO"