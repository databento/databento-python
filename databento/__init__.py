from typing import Optional

from databento.common.bento import Bento, FileBento, MemoryBento
from databento.historical.api import API_VERSION
from databento.historical.client import Historical
from databento.historical.error import (
    BentoClientError,
    BentoError,
    BentoHttpError,
    BentoServerError,
)
from databento.version import __version__  # noqa


__all__ = [
    "API_VERSION",
    "Bento",
    "BentoClientError",
    "BentoError",
    "BentoHttpError",
    "BentoServerError",
    "FileBento",
    "Historical",
    "MemoryBento",
]

# Set to either 'DEBUG' or 'INFO', controls console logging
log: Optional[str] = None
