from __future__ import annotations

import asyncio
import logging

from config import Config
from models import URLTask, HTMLPage
from workers import Producer, Downloader

class Pipeline:
    ""
    def __init__(self, config: Config, logger : logging.Logger) -> None:
        ""
        self.config = config
        self.logger = logger.getChild("Pipeline")

        self.url_queue : asyncio.Queue[URLTask|None] = asyncio.Queue(maxsize=config.queue_size)
        self.html_queue : asyncio.Queue[HTMLPage|None] = asyncio.Queue(maxsize=config.queue_size)

        self.stats : dict[str, int] = {
            "produced" : 0,
            "downloaded" : 0,
            "analyzed" : 0,
            "saved" : 0,
            "errors" : 0,
        }


        self._producer = Producer(
            self.stats,
            self.url_queue,
            logger.getChild("Producer"),
            config,
        )
        self._downloaders = Downloader(
                f"Downloader",
                self.url_queue,
                self.html_queue,
                self.stats,
                config,
                logger.getChild(f"Downloader"),
        )

    async def run(self) -> None :
        ""
        producer_task = asyncio.create_task(self._producer.run())
        downloader_task = asyncio.create_task(self._downloader.run())

        self.logger.info("Pipeline started")

        await producer_task
        self.url_queue.join()
        self.url_queue.put(None)

        await downloader_task
        self.html_queue.join()
        self.html_queue.put(None)