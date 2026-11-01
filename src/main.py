from __future__  import annotations

import asyncio
import logging
import argparse

from config import Config
from pipeline import Pipeline 


def setup_logging(level : str) -> logging.Logger :
    ""
    logger = logging.getLogger("asynciopipeline")
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt = "%(asctime)s %(name)-30s %(levelname)-8s %(message)s",
        datefmt = "%H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def _parse_args() -> argparse.Namespace :
    ""
    parser = argparse.ArgumentParser(
        description = "AsyncPipeline Pro — simulate pipeline producer/consumer."
    )

    parser.add_argument(
        "--log-level", type = str, default = None,
        choices = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help = "Log level (default : Config.log_level)" ,
    )
    parser.add_argument(
        "--num-urls", type = int, default = None,
        help = "Number of URLs to process (default : Config.num_urls)" ,
    )
    return parser.parse_args()


def main() -> None :
    args = _parse_args()
    overrides = {}
    if args.num_urls is not None:
        overrides["num_urls"] = args.num_urls
    if args.log_level is not None :
        overrides["log_level"] = args.log_level

    config = Config(**overrides)
    logger = setup_logging(config.log_level)
    pipeline = Pipeline (config, logger)
    asyncio.run(pipeline.run())

if __name__ == "__main__":
    main()