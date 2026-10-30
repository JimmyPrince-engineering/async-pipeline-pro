from __future__ import annotations

import asyncio
import time
import logging

from typing import Any

from config import Config

class Monitor:
    ""
    def __init__(
        self,
        stats : dict[str, int],
        queues : dict[str, asyncio.Queue],
        ready_event : asyncio.Event,
        shutdown_event : asyncio.Event,
        logger : logging.Logger,
        config : Config,
    ) -> None :
        ""
        self.stats = stats
        self.queues = queues
        self.ready_event = ready_event
        self.shutdown_event = shutdown_event
        self.logger = logger
        self.config = config
        self._last_snapshoot_time : float = time.monotonic()
        self._last_saved : int = 0
    
    async def run (self) -> None :
        ""
        await self.ready_event.wait()
        self._last_snapshoot_time = time.monotonic()
        self._last_saved = self.stats.get("saved", 0)
        self.logger.info("Monitor : started (interval : %.1f s)", self.config.monitor_interval)
        
        while not self.shutdown_event.is_set() :
            try:
                self.logger.info("Monitor : %s", self._snapshoot())
                await asyncio.wait_for(
                    self.shutdown_event.wait(),
                    timeout = self.config.monitor_interval
                )
            except asyncio.TimeoutError :
                pass

        self.logger.info("Monitor stopped (shutdown_event lifted). ")
        
    def _snapshoot(self) -> dict [str, int|float] :
        ""
        now = time.monotonic()
        saved_now = self.stats.get("saved", 0)
        elapsed = now - self._last_snapshoot_time
        throuput = (saved_now - self._last_saved)/ elapsed if elapsed > 0 else 0.0

        self._last_snapshoot_time = now
        self._last_saved = saved_now

        snapshoot : dict[str, int|float] = {f"{name}_waiting" : q.qsize() for name, q in self.queues.items()}
        snapshoot["flow_items_saved_per_sec"] = round(throuput, 2)
        snapshoot.update(self.stats)

        return snapshoot