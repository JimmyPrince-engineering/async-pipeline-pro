from __future__ import annotations

import asyncio
import logging
import random
import time


from models import URLTask, HTMLPage
from config import Config
from exceptions import (
PipelineError,
DownloadTimeoutError,
)
from simulations import simulate_download

class Producer:
    ""

    def __init__(
        self,
        stats : dict[str, int],
        queue_out : asyncio.Queue[URLTask],
        logger : logging.logger,
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



class Downloader:
    ""
    stat_key : str = "downloaded"
    def __init__(
        self,
        name : str ,
        queue_in : asyncio.Queue[URLTask],
        queue_out : asyncio.Queue[HTMLPage],
        stats : dict[str, int],
        config : Config,
        logger : logging.Logger,
        semaphore : asyncio.Semaphore
    ) -> None:
        self.name = name
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.stats = stats
        self.config = config
        self.logger = logger
        self.semaphore = semaphore

    async def run(self) -> None :
        while True:
            try:
                item = await self.queue_in.get()
                if item is None:
                    self.logger.info("%s sentinel received, task completed", self.name)
                    return
                try:
                    result = await process_item(item)
                except DownloadTimeoutError as exc:
                    self.stats["errors"] += 1
                    self.logger.error("%s: DownloadTimeoutError - %s", self.name, exc)
                else:
                    self.stats[self.stat_key] += 1
                    await self.queue_out.put(result)
            finally:
                self.queue_in.task_done()

    async def process_item(self, item : URLTask) -> HTMLPage :
        async with self.semaphore :
            return await simulate_download(item, self.config)