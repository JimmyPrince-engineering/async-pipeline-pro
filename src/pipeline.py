from __future__ import annotations

import asyncio
import logging
import signal
import time

from typing import Optional

from config import Config
from models import URLTask, HTMLPage, AnalysisResult
from workers import Producer, Downloader, Analyzer, Saver
from monitor import Monitor

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
        self.ready_event : asyncio.Event = asyncio.Event()
        self.shutdown_event : asyncio.Event = asyncio.Event()

        self.stats : dict[str, int] = {
            "produced" : 0,
            "downloaded" : 0,
            "analyzed" : 0,
            "saved" : 0,
            "errors" : 0,
        }

        self.database : list[AnalysisResult] = []


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
        ) for i in range(1, config.num_downloaders + 1)
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

        self._monitor = Monitor(
            self.stats,
            {"url" : self.url_queue, "html" : self.html_queue, "result" : self.result_queue},
            self.ready_event,
            self.shutdown_event,
            logger.getChild("Monitor"),
            config,
        )

        self._producer_task : Optional[asyncio.Task] = None
        self._monitor_task : Optional[asyncio.Task] = None
        self._started_time : float = 0.0
        self._duration : float = 0.0
        

    async def run(self) -> None :
        ""
        self._install_signal_handler()
        self._started_time = time.monotonic()
        
        self._producer_task = asyncio.create_task(self._producer.run())
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

        self._monitor_task = asyncio.create_task(self._monitor.run())

        self.ready_event.set()
        self.logger.info(
            "Pipeline : started — %d urls, %d Downloaders, %d Analyzers, %d Savers.",
            self.config.num_urls, self.config.num_downloaders, self.config.num_analyzers,
            self.config.num_savers
        )

        try:
            try:
                await self._producer_task
            except asyncio.CancelledError :
                self.logger.warning(
                        "Pipeline: Producer interrupted no more new URLs, "
                        "drainage of tasks already issued."
                )
            
        
            await self._drain_stage(self.url_queue, self.config.num_downloaders, downloader_tasks)
            await self._drain_stage(self.html_queue, self.config.num_analyzers, analyzer_tasks)
            await self._drain_stage(self.result_queue, self.config.num_savers, saver_tasks)
        
        finally:
            self.shutdown_event.set()
            if self._monitor_task is not None:
                await self._monitor_task
        self._duration = time.monotonic() - self._started_time
        self._print_summary()
    

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
    
    def _print_summary(self) -> None :
        ""
        saved = self.stats.get("saved", 0)
        errors = self.stats.get("errors", 0)
        total = saved + errors
        error_rate = (errors/total * 100) if total >0 else 0.0 
        throughput = total/self._duration if self._duration > 0 else 0.0

        self.logger.info(
            "Pipeline : FINAL SUMMARING — delay = : %.2fs | issued = %d  downloaded = %d\
            analysed = %d  saved = %d  failed = %d |"
            "total processed = %d/%d | fail rate = %.1fs%% | average flow = %.1fs items/sec ",
            self._duration, self.stats.get("produced", 0), self.stats.get("downloaded", 0),
            self.stats.get("analyzed", 0), self.stats.get("saved", 0), self.stats.get("errors", 0),
            total, self.config.num_urls, error_rate, throughput
        )