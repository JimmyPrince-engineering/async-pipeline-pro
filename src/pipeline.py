from __future__ import annotations

import asyncio
import logging
import signal

from config import Config
from models import URLTask, HTMLPage, AnalysisResult
from workers import Producer, Downloader, Analyzer, Saver

class Pipeline:
    ""
    def __init__(self, config: Config, logger : logging.Logger) -> None:
        ""
        self.config = config
        self.logger = logger.getChild("Pipeline")

        self.url_queue : asyncio.Queue[URLTask|None] = asyncio.Queue(maxsize=config.queue_size)
        self.html_queue : asyncio.Queue[HTMLPage|None] = asyncio.Queue(maxsize=config.queue_size)
        self.result_queue : asyncio.Queue[AnalysisResult|None] = asyncio.Queue(maxsize=config.queue_size)

        self.semaphore : asyncio.Semaphore = asyncio.Semaphore(config.max_connections)
        self.db_lock : asyncio.Lock = asyncio.Lock()

        self.stats : dict[str, int] = {
            "produced" : 0,
            "downloaded" : 0,
            "analyzed" : 0,
            "saved" : 0,
            "errors" : 0,
        }

        self.databse : list[AnalysisResult] = []


        self._producer = Producer(
            self.stats,
            self.url_queue,
            logger.getChild("Producer"),
            config,
        )
        self._downloaders = [
        Downloader(
                f"Downloader_{i}",
                self.url_queue,
                self.html_queue,
                self.stats,
                config,
                logger.getChild(f"Downloader.{i}"),
                semaphore = self.semaphore,
        ) for i in range(1, config.num_downloaders)
        ]

        self._analyzers = [
            Analyzer(
                f"Analyzer_{i}",
                self.html_queue,
                self.result_queue,
                self.stats,
                config,
                logger.getChild(f"Analyzer.{i}"),
            )
            for i in range(1, config.num_analyzers + 1)
        ]

        self._savers = [
            Saver(f"Saver_{i}",
            self.result_queue,
            None,
            self.stats,
            config,
            logger.getChild(f"Saver.{i}"),
            database = self.database,
            db_lock = self.db_lock,) for i in range(1, config.num_savers + 1)
        ]

    async def run(self) -> None :
        ""
        self._install_signal_handler()
        
        producer_task = asyncio.create_task(self._producer.run())
        downloader_tasks = [
            asyncio.create_task(d.run())
            for d in self._downloaders
        ]
        analyzer_tasks = [
            asyncio.create_task(an.run())
            for an in self._analyzers
        ]
        saver_tasks = [
            asyncio.create_task(s.run())
            for s in self._savers
        ]

        self.logger.info("Pipeline started")

        try:
            await producer_task
        except asyncio.CancelledError :
            self.logger.warning(
                    "Pipeline: Producer interrupted no more new URLs, "
                    "drainage of tasks already issued."
            )
        
        await self._drain_stage(self.url_queue, self.config.num_downloaders, downloader_tasks)
        await self._drain_stage(self.html_queue, self.config.num_analyzers, analyzer_tasks)
        await self._drain_stage(self.result_queue, self.config.num_savers, saver_tasks)

    async def _drain_stage(
        self,
        queue : asyncio.Queue,
        num_sentinels : int,
        task : list[asyncio.Task],
    ) -> None :
        ""
        await queue.join()

        for _ in range(num_sentinels):
            await queue.put(None)

        await asyncio.gather(*task)


    def _install_signal_handler(self) -> None:
        ""
        loop = asyncio.get_running_loop()

        def _sigint_handler() -> None :
            ""
            self.logger.warning(
                "Pipeline : SIGINT signal received — graceful shutdown, no more new URLs will be issued."
            )

            if self._producer_task is not None and not self._producer_task.done():
                self._producer_task.cancel()

        try:
            loop.add_signal_handler(signal.SIGINT, _sigint_handler)
        except NotImplementedError :
            self.logger.debug(
                "Pipeline : add_signal_handler isn\'t available on this platform (eg : Window)."
            )