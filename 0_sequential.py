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
    