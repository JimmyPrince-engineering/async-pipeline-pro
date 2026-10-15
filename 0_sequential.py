from __future__ import annotations

from pathlib import Path
import logging
from dataclasses import asdict, dataclass


LOGGER = logging.getLogger("Pipeline.m0")
USER_AGENT = "AsyncPipelinePro/0.2 (personnal site monitor)"
CSV_COLUMNS = (
    "url", "final_url", "status_code", "succes", "error_kind", "duration_s"
)


@dataclass(frozen=True)
class Result:
    ""
    url : str,
    final_url : str|None,
    status_code : int|None,
    succes : bool,
    error_kind : str|None,
    duration_s : float

def load_urls(path : Path) -> list[str]:
    ""
    with path.open(encoding="utf-8") as source:
        urls = [
            line.strip() for line in source if line and not line.lstrip().startswith("#")
        ]
    if not urls:
        raise ValueError(f"There is no URL to test in {path}.")
    return urls