from __future__ import annotations

from pathlib import Path
from urllib.parse import urlsplit
from collections import Counter
from datetime import timezone, datetime
from typing import Any
import logging
import argparse
import time
import statistics
import csv

import httpx

from dataclasses import asdict, dataclass


LOGGER = logging.getLogger("Pipeline.m0")
USER_AGENT = "AsyncPipelinePro/0.2 (personnal site monitor)"
CSV_RESULT_COLUMNS = (
    "url", "final_url", "status_code", "success", "error_kind", "error_detail", "root_cause", "duration_s"
)
CSV_RESUME_COLUMNS = (
    "date_time", "request_number", "OK", "KO", "total_duration", "duration_sum", "mean", "median", "interrupted"
)


@dataclass(frozen=True)
class Result:
    ""
    url : str
    final_url : str|None
    status_code : int|None
    success : bool
    error_kind : str|None
    error_detail : str|None
    root_cause : str|None
    duration_s : float

@dataclass(frozen=True)
class Resume:
    ""
    date_time: str
    request_number: int
    OK : int
    KO : int
    total_duration : float
    duration_sum : float
    mean : float
    median : float
    interrupted : bool


def get_root_cause(exc: Exception) -> str:
    ""
    current= exc
    seen = set()
    while current.__cause__ is not None:
        if id(current) in seen:
            break
        seen.add(id(current))
        current = current.__cause__
    return type(current).__name__

def load_urls(path : Path) -> list[str]:
    ""
    urls = []
    with path.open(encoding="utf-8") as source:
        for line_index, line in enumerate(source, start=1):
            url = line.strip()
            if not url or url.startswith("#"):
                continue
            if not url_is_valid(url):
                LOGGER.warning("Line %d: Invalid URL ignored : %s", line_index, url)
                continue
                
            urls.append(url)
    if not urls:
        raise ValueError(f"There is no valid URL to test in {path}.")
    return urls


def url_is_valid(url: str) -> bool:
    ""
    try:
        parts = urlsplit(url)
        return (parts.scheme in ("http", "https") and bool(parts.hostname))
    except ValueError:
        return False


def measure_one(client :httpx.Client, url : str) -> Result:
    ""
    start = time.perf_counter()
    final_url = None
    status_code = None
    success = False
    error_kind = None
    error_detail = None
    root_cause = None

    try:
        response = client.get(url)
        final_url = str(response.url)
        status_code = response.status_code
        success = response.is_success
        if not success:
            error_kind = f"HTTP_{status_code}"
    
    except httpx.InvalidURL as exc:
        error_kind = type(exc).__name__
        error_detail = str(exc)[:150]
        root_cause = get_root_cause(exc)
        LOGGER.warning("URL Error:  %s  (%s) ", url, exc)
    
    except httpx.RequestError as exc:
        error_kind = type(exc).__name__
        error_detail = str(exc)[:150]
        root_cause = get_root_cause(exc)
        LOGGER.warning("Request Error:  %s  error_kind: %s  (%s)", url, error_kind, exc)

    except Exception as exc:
        error_kind = "Unexpected"
        error_detail = str(exc)[:150]
        root_cause = get_root_cause(exc)
        LOGGER.exception("Unexpected error for %s", url)
    finally:
        elapsed = time.perf_counter() - start

    return Result(url, final_url, status_code, success, error_kind, error_detail, root_cause, elapsed)


def make_resume(report: dict[str, Any], interrupted : bool) -> Resume:
    ""
    return Resume (
        date_time = datetime.now(timezone.utc).strftime("%Y/%m/%d %H:%M:%S"),
        request_number = report["requests"],
        OK = report["successes"],
        KO = report["faillures"],
        total_duration = report["total_duration"],
        duration_sum = report["duration_sum"],
        mean = report["mean"],
        median = report["median"],
        interrupted = interrupted,
    )


def save_CSV_result(results : list[Result], path : Path) -> None:
    ""
    with path.open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=CSV_RESULT_COLUMNS)
        writer.writeheader()
        for result in results:
            writer.writerow(asdict(result))


def save_CSV_resume(resume : Resume, path : Path) -> None :
    ""
    needs_header = not path.exists() or path.starts().st_size == 0
            
    with path.open("a", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=CSV_RESUME_COLUMNS)
        if needs_header:
            writer.writeheader()
        writer.writerow(asdict(resume))



def compute_report(results : list[Result], wall_time_s :float) -> dict[Any]:
    ""
    duration = [result.duration_s for result in results]
    successes = sum(result.success for result in results)
    faillures = len(results) - successes
    errors = Counter(result.error_kind for result in results if result.error_kind)
    
    five_very_slow = []
    for result in sorted(results, key= lambda item : item.duration_s, reverse=True)[:5]:
        outcome = result.status_code if result.status_code is not None else result.error_kind
        five_very_slow.append((result.duration_s, outcome, result.url))

    return {
        "requests" : len(results) ,
        "successes" : successes,
        "faillures" : faillures,
        "total_duration" : wall_time_s,
        "duration_sum" : sum(duration),
        "mean" : statistics.mean(duration),
        "median" : statistics.median(duration),
        "five_very_slow" : five_very_slow,
        "errors_count" : errors
    }


def print_report(report : dict[str, Any]) -> None:
    ""
    print("==M0 - SEQUENTIAL BASELINE==")
    print(f"Requests: {report["requests"]} | OK (2xx): {report["successes"]} | KO: {report["faillures"]}")
    print(f"Total duration (gross): {report["total_duration"]:.3f}")
    print(f"Request duration sum : {report["duration_sum"]:.3f}")
    print(f"Mean/Request: {report["mean"]:.3f} ")
    print(f"Median/Request: {report["median"]:.3f}")

    print("\nTop 5 very slow (include faillures)\n")
    for duration_s, outcome, url in report["five_very_slow"]:
        print(f"{duration_s:.3f} | {outcome}| {url}")

    print("\nFaillure per type\n")

    if report["errors_count"]:
        for kind, count in sorted(report["errors_count"].items(), key = lambda item : item[1], reverse=True):
            print(f"{kind} : {count}")
    else:
        print("There is not error.")


def main():
    ""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    
    parser = argparse.ArgumentParser(description="M0 sequential HTTP Baseline")
    parser.add_argument("--urls", type=Path, default=Path("urls.txt"))
    parser.add_argument("--output", type=Path, default=Path(f"m0_results_{timestamp}.csv"))
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s : %(message)s")

    try:
        urls = load_urls(args.urls)
    except (OSError, UnicodeError, ValueError) as exc :
        parser.error(str(exc))

    results : list[Result] = []
    start = time.perf_counter()

    interrupted = False 
    try:
        with httpx.Client(
            timeout = 10.0,
            follow_redirects = True,
            headers = {"User-Agent" : USER_AGENT}
        ) as client:
            for index, url in enumerate(urls, start=1):
                result = measure_one(client, url)
                outcome = result.status_code if result.status_code is not None else result.error_kind
                print(f"[{index}/{len(urls)}]  | {outcome}  | {result.duration_s:.3f}")
                results.append(result)
    except KeyboardInterrupt:
        interrupted = True
        LOGGER.error("Interrupted: Saving the results already acquired.")
    wall_time_s = time.perf_counter() - start

    if not results:
        LOGGER.error("There is not resultto save.")
        return 1

    report = compute_report(results, wall_time_s)
    resume = make_resume(report, interrupted)

    try:
        type = "results"
        path = args.output
        save_CSV_result(results, path)
        print(f"\nResults successfully saved.\n")
        type = "resume"
        path = Path("m0_runs.csv")
        save_CSV_resume(resume, path)
        print(f"\nResume successfully saved.\n")
    
    except OSError as exc:
        logging.error("Impossible to write CSV %s %s: %s", type, path, exc)
        return 1
    finally:
        print_report(report)
    
    return 0 if not interrupted else 130
    
    
if __name__ == "__main__":
    raise SystemExit(main())