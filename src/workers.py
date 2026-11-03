from __future__ import annotations

import asyncio
import logging
import random
import time


from typing import Any, Generic, TypeVar


from models import URLTask, HTMLPage, AnalysisResult
from config import Config
from exceptions import (
PipelineError,
DownloadTimeoutError,
AnalysisError,
DatabaseError,
RetryExhaustedError,
)
from simulations import simulate_download, analyze_text, simulate_save

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


class Producer:
    ""

    def __init__(
        self,
        stats : dict[str, int],
        queue_out : asyncio.Queue[URLTask],
        logger : logging.Logger,
        config : Config
    ) -> None :
        self.stats = stats
        self.queue_out = queue_out
        self.logger = logger
        self.config = config

    async def run(self) -> None:
        ""

        for task_id in range(1, self.config.num_urls+1):
            task=URLTask(
                task_id=task_id,
                url=f"https://example/page/{task_id}",
                created_at=time.monotonic()
            )

            self.logger.debug("Producer: task %d issued, URL: %s", task_id, task.url)

            await self.queue_out.put(task)
            self.stats["produced"] += 1
            if task_id < self.config.num_urls:
                delay = random.uniform(self.config.producer_interval_min, self.config.producer_interval_max)
                await asyncio.sleep(delay)
        self.logger.info("Producer: %d URL issued, work completed", task_id)



class Worker(Generic[InputT, OutputT]):
    ""
    stat_key : str
    def __init__(
        self,
        name : str ,
        queue_in : asyncio.Queue[InputT],
        queue_out : asyncio.Queue[OutputT]|None,
        stats : dict[str, int],
        config : Config,
        logger : logging.Logger
    ) -> None:
        self.name = name
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.stats = stats
        self.config = config
        self.logger = logger

    async def run(self) -> None :
        while True:
            item = await self.queue_in.get()
            try:
                if item is None:
                    self.logger.info("%s sentinel received, task completed", self.name)
                    return
                try:
                    result = await self._process_with_retry(item)
                except RetryExhaustedError as exc:
                    self.stats["errors"] += 1
                    self.logger.error("%s: definitive abbandonment - %s", self.name, exc)
                else:
                    self.stats[self.stat_key] += 1
                    if self.queue_out is not None:
                        await self.queue_out.put(result)
            finally:
                self.queue_in.task_done()

    async def _process_with_retry(self, item: InputT) -> OutputT:
        ""
        last_error : Exception|None = None
        total_attempts : int = self.config.max_retry + 1

        for attempt in range(1, total_attempts +1):
            try:
                return await self.process_item(item)

            except PipelineError as exc:
                self.logger.warning(
                    "%s: %d/%d attempt failed for %r -%s",
                    self.name, attempt, total_attempts, item, exc
                )
                last_error = exc
            if attempt < total_attempts:
                await asyncio.sleep(self._computer_backoff(attempt))

        assert last_error is not None
        raise RetryExhaustedError(item=item, attempt=attempt, last_error=last_error)

    def _computer_backoff(self, attempt : int) -> float:

        delay = self.config.backoff_base * (self.config.backoff_multiplier** (attempt - 1))
        delay = min(delay, self.config.backoff_max)
        return delay + random.uniform(0, self.config.backoff_jitter)

    
    def process_item(self, item: InputT) -> OutputT|None:
        ""
        raise NotImplementedError()



class Downloader(Worker[URLTask, HTMLPage]):
    ""
    stat_key = "downloaded"

    def __init__(self, *args : Any, semaphore : asyncio.Semaphore, **kwargs : Any) -> None :
        super().__init__(*args, **kwargs)
        self.semaphore = semaphore
    
    async def process_item(self, item : URLTask) -> HTMLPage :
        async with self.semaphore :
            try:
                return await asyncio.wait_for(simulate_download(item, self.config), timeout = self.config.timeout_seconds)
            except asyncio.TimeoutError as exc:
                raise DownloadTimeoutError(
                  f"{item.url!r} download beyond {self.config.timeout_seconds} s."  
                )

class Analyzer(Worker[HTMLPage, AnalysisResult]):
    stat_key = "analyzed"

    async def process_item(self, item: HTMLPage) -> AnalysisResult:
        ""
        loop = asyncio.get_running_loop()
        word_count = await loop.run_in_executor(None, analyze_text, item, self.config)
        return AnalysisResult (
            task_id = item.task_id,
            url = item.url,
            len_content = word_count,
            analyzed_at = time.monotonic()
        )

class Saver(Worker[AnalysisResult, None]):
    stat_key = "saved"

    def __init__(self, *args : Any, database : list[AnalysisResult],
        db_lock : asyncio.Lock, **kwargs : Any
    ) -> None :
        super().__init__(*args, **kwargs)
        self.database = database
        self.db_lock = db_lock

    async def process_item(self, item : AnalysisResult) -> None :
        ""
        async with self.db_lock:
            await simulate_save(item, self.database, self.config)