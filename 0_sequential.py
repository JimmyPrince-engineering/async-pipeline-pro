from __future__ import annotations

from pathlib import Path
from urllib.parse import urlsplit
from collections import Counter
import logging
import argparse
import time
import statistics

from dataclasses import asdict, dataclass


LOGGER = logging.getLogger("Pipeline.m0")
USER_AGENT = "AsyncPipelinePro/0.2 (personnal site monitor)"
CSV_COLUMNS = (
    "url", "final_url", "status_code", "succes", "error_kind", "duration_s"
)


@dataclass(frozen=True)
class Result:
    ""
    url : str
    final_url : str|None
    status_code : int|None
    succes : bool
    error_kind : str|None
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

def report(results : list[Result], wall_time_s :float) -> None:
    ""
    duration = [result.duration_s for result in results]
    successes = sum(result.succes for result in results)
    faillures = len(results) - successes
    errors = Counter(result.error_kind for result in results if result.error_kind)

    print("==M0 - SEQUENTIAL BASELINE==")
    print(f"Requests: {len(results)} | OK (2xx): {successes} | KO: {faillures}")
    print(f"Total duration (gross): {wall_time_s}")
    print(f"Request duration sum : {sum(duration)}")
    print(f"Mean/Request: {statistics.mean(duration)} ")
    print(f"Median/Request: {statistics.median(duration)}")

    print("\nTop 5 very slow (include faillures)\n")
    for result in sorted(results, key= lambda item : item.duration_s, reversed=True)[:5]:
        outcome = result.status_code if status_code is not None else result.error_kind
        print(f"{result.duration_s:.3f} | {outcome}| {result.url}")

    print("\nFaillure per type\n")

    if errors:
        for kind, count in sorted(errors.items()):
            print(f"{kind} : {count}")
    else:
        print("There is not error.")


def main():
    ""
    parser = argparse.ArgumentParser(description="M0 sequential HTTP Baseline")
    parser.add_argument("--urls", type=Path, default="urls.txt")
    parser.add_argument("--output", type=Path, default="m0_results.csv")
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
            for index, url in enumerate(urls):
                result = measure_one(client, url)
                outcome = result.status_code if result.status_code is not None else result.error_kind
                print(f"[{index}/{len(urls)}]  | {outcome}  | {result.duration_s:.3f}")
                results.append(result)
    except KeyboardInterrupt:
        interrupted = True
        LOGGER.error("")
    wall_time_s = time.perf_counter() - start

    if not results:
        LOGGER.error("There is not resultto save.")
        return 1

    try:
        save_CSV(results, args.output)
        print(f"Results successfully saved.")
    except OSError as exc:
        logging.error("Impossible to write CSV %s: %s", pars.output, exc)
        return 1
    finally:
        report(results, wall_time_s)
    
    return 0 if interrupted else 130
    
    
if __name__ == "__main__":
    raise SystemExit(main())