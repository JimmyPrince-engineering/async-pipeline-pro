from __future__ import annotations

import asyncio
import random
import time


from config import Config
from exceptions import DownloadError, AnalysisError, DatabaseError
from models import URLTask, HTMLPage, AnalysisResult


def _roll_failure(rate : float) -> bool :
    return random.random() < rate

async def simulate_download(task : URLTask, config : Config ) -> HTMLPage:
    ""
    latency = random.uniform(config.download_latency_min, config.download_latency_max)
    await asyncio.sleep(latency)

    if _roll_failure(config.download_fail_rate):
        raise DownloadError(f"Download simulate faillure of {task.url!r}")
    
    content = f"<html><body>Content simulate for {task.url} (task_id={task.task_id})</body></html>"
    
    return HTMLPage(
        task_id = task.task_id,
        url = task.url,
        content = content,
        status_code = 200,
        download_duration = latency,
    )

def analyze_text(page : HTMLPage, config : Config) -> int :
    ""
    latency = random.uniform(config.analysis_latency_min, config.analysis_latency_max)
    time.sleep(latency)

    if _roll_failure(config.analysis_fail_rate):
        raise AnalysisError(f"Analysis simulate faillure for task_id={page.task_id}")

    return len(page.content.split())

async def simulate_save(result : AnalysisResult, database : list[AnalysisResult], config : Config) -> None:
    ""
    latency = random.uniform(config.save_latency_min, config.save_latency_max)
    await asyncio.sleep(latency)
    

    if _roll_failure(config.save_fail_rate):
        raise DatabaseError(f"Save simulate faillure for task_id={result.task_id}")
    
    database.append(result)