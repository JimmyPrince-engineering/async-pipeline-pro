from dataclasses import dataclass, field

@dataclass(frozen=True)
class URLTask:
    task_id : int
    url : str
    created_at : float


@dataclass(frozen=True)
class HTMLPage:
    task_id : int
    url : str
    content : str = field(repr = False)
    status_code : int
    download_duration : float


@dataclass(frozen=True)
class AnalysisResult:
    task_id : int
    url : str
    len_content : int
    analyzed_at : float