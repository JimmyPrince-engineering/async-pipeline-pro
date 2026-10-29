"application exceptions module"

class PipelineError(Exception):
    "main application exceptions"

class DownloadError(PipelineError):
    "download failed exception"

class DownloadTimeoutError(DownloadError):
    "timeout exception"

class AnalysisError(PipelineError):
    "data analysis error exception"

class DatabaseError(PipelineError):
    "save error exception"

class RetryExhaustedError(PipelineError):
    "exceeding max retry error exception"

    def __init__(
        self,
        item,
        max_retry,
        last_error,
    ):
        self.item : object = item
        self.attempts : int = attempts
        self.last_error : PipelineError = last_error 
        
        super().init(
            f"Abandonment after {self.attempts} of {self.item!r} \
            {type(self.last_error).__name__} : {self.last_error!r} "
        )