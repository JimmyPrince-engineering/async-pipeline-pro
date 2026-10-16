from __future__ import annotations

from pathlib import Path
from urllib.parse import urlsplit
import logging
import time
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

def measure_one(client :httpx.Client, url : str) -> Result:
    ""
    start = time.perf_counter()
    final_url = None
    status_code = None
    error_kind = None

    try:
        parts = urlsplit(url)
        if url.scheme not in ("http", "https") or not parts.hostname :
            raise InvalidURL("URL must contains http(s) scheme and hostname")
        response = client.get(url)
        final_url = str(response.url)
        status_code = response.status_code
        succes = response.is_succes
        if not succes:
            error_kind = f"HTTP_{status_code}"
    except (httpx.InvalidURL, ValueError) as exc:
        error_kind = type(exc).__name__
        LOGGER.warning("URL Error:  %s  (%s) ", url, exc)
    except httpx.RequestError as exc:
        error_kind = type(exc).__name__
        LOGGER.warning("Request Error:  %s  error_kind: %s  (%s)", url, error_kind, exc)
    finally:
        elapsed = time.perf_counter() - start

    return Result(url, final_url, status_code, succes, error_kind, elapsed)

def save_CSV(results : list[Result], path : Path) -> None:
    ""
    with path.open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for result in results:
            writer.writerow(asdict(result))

def report(results : list[Result]) -> None:
    