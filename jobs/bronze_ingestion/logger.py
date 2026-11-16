"""Shared stdlib logger (get_logger) - one stream handler, LOG_LEVEL from env."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

# GMT+3:30
# pattern (JVM-side logs) and postgres's TZ=Asia/Tehran, so every log in the
# stack reads the same wall-clock time.
_LOG_TZ = timezone(timedelta(hours=3, minutes=30))


class _TzFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: "str | None" = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=_LOG_TZ)
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S")


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            _TzFormatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
        logger.propagate = False
    return logger
