import os
import json
import datetime
import threading
from typing import Any, Optional
from abc import ABC, abstractmethod
import logging
import anyio

from src.constants.config import Config 

LOGGER_SERVICE=Config.LOGGER_SERVICE

class LoggerBase(ABC):
    """Abstract base class for all loggers."""

    @abstractmethod
    def log(self, data: Any, level: str = "info") -> None:
        pass

class JsonFileLogger(LoggerBase):
    """Logs messages to a JSON file."""

    def __init__(self, file_path: str = "app.log"):
        self.file_path = file_path
        self._lock = threading.Lock()

    def log(self, data: Any, level: str = "info") -> None:
        entry = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "level": level.upper(),
            "message": str(data),
        }
        with self._lock, open(self.file_path, "a") as f:
            f.write(json.dumps(entry)+ "\n")


class StdoutLogger(LoggerBase):
    """Uses Python logging (FastAPI/Uvicorn integrated)."""

    def __init__(self):
        self._logger = logging.getLogger("app")

    def log(self, data: Any, level: str = "info") -> None:
        msg =data
        getattr(self._logger, level.lower(), self._logger.info)(msg)




class GlobalLogger:
    _instance: Optional["GlobalLogger"] = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        service = (LOGGER_SERVICE or "json").lower()
        if service == "stdout":
            self._logger = StdoutLogger()
        else:
            self._logger = JsonFileLogger("app.log")  # default

    async def log_event(self, data: Any, level: str = "info") -> None:
        await anyio.to_thread.run_sync(self._logger.log, data, level)

# Global access
global_logger = GlobalLogger()
