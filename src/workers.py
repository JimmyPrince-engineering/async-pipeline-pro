from __future__ import annotations

import asyncio
import logging
import random
import time


from models import URLTask
from config import Config

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