from __future__ import annotations

import asyncio
import random
import time


from config import Config
from exceptions import DownloadError, AnalysisError
from models import URLTask, HTMLPage


def _roll_failure(rate : float) -> bool :
    return random.random() < rate

async def simulate_download(task : URLTask, config : Config ) -> HTMLPage:
    ""
    latency = random.uniform(config.download_latency_min, config.download_latency_max)
    await asyncio.sleep(latency)

    if _roll_failure(config.download_fail_rate):
        raise DownloadError(f"Échec simulé du téléchargement de {task.url!r}")
    
    content = f"<html><body>Contenu simulé pour {task.url} (task_id={task.task_id})</body></html>"
    
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
        raise AnalysisError(f"Échec simulé de l'analyse pour task_id={page.task_id}")

    return len(page.content.split())