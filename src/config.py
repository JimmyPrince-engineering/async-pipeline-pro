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

    def __post_init__(self):
        
        positif_ints = {
            "num_urls" : self.num_urls,
            "num_downloaders" : self.num_downloaders,
            "num_analyzers" : self.num_analyzers,
            "num_savers" : self.num_savers,
            "queue_size" : self.queue_size,
            "max_connections" : self.max_connections,
        }

        for field_name, value in positif_ints.items():
            if value <= 0 or type(value) is not int:
                raise ValueError(
                    f"{field_name} doit être un entier supérieur à 0,"
                    f" réçu {value!r}"
                )
        
        positif_floats ={
            "timeout_seconds" : self.timeout_seconds,
            "monitor_interval" : self.monitor_interval,
            "backoff_base" : self.backoff_base
        }

        for field_name, value in positif_floats.items():
            if value <= 0 :
                raise ValueError(f"{field_name} doit être un entier strictement positif, réçu {value!r} ")
            
        if self.max_retry < 0 or type(self.max_retry) is not int :
            raise ValueError("max_retry doit être un entier positif")

        rates = {
            "download_fail_rate" : self.download_fail_rate,
            "analysis_fail_rate" : self.analysis_fail_rate,
            "save_fail_rate" : self.save_fail_rate
        }

        for field_name, value in rates.items():
            if not 0.0 <= value <= 1.0 :
                raise ValueError(f"{field_name} doit être entre 0.0 et 1.0, réçu {value!r} ")

        if self.backoff_multiplier < 1.0:
            raise ValueError(f"backoff_multiplier doit ≥ 1.0, réçu {value!r} ")


        if self.backoff_max < self.backoff_base:
            raise ValueError(f"backoff_max {self.backoff_max!r} doit être supérieur à  backoff_base {self.backoff_base!r} ")

        if self.backoff_jitter <= 0:
            raise ValueError(f"backoff_jitter doit être strictement positif, réçu {self.backoff_jitter!r} ")

        intervals = {
            "producer" : (self.producer_interval_min, self.producer_interval_max),
            "download_latency" : (self.download_latency_min, self.download_latency_max),
            "analysis_latency" : (self.analysis_latency_min, self.analysis_latency_max),
            "save_latency" : (self.save_latency_min, self.save_latency_max)
        }

        for field_name, (low, high) in intervals.items():
            if low < 0:
                raise ValueError(f"{field_name}_min doit être positif, réçu {low!r} ")
            if low > high :
                raise ValueError(f"{field_name}_min {low!r} ne peut pas dépasser {field_name}_max {high!r} ")

        valid_levels = {"DEBUG", "INFO", "WARMING", "ERROR", "CRITICAL"}
        
        if self.log_level not in valid_levels:
            raise ValueError(f"log_level doit être l'un de {sorted(valid_levels)}, réçu {self.log_level!r} ")