from typing import Optional

from databento.common.bento import Bento, FileBento, MemoryBento
from databento.common.enums import (
    Compression,
    Dataset,
    Delivery,
    Encoding,
    FeedMode,
    HistoricalGateway,
    LiveGateway,
    Packaging,
    RecordFlags,
    RollRule,
    Schema,
    SplitDuration,
    SType,
    SymbologyResolution,
)
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
    "Compression",
    "Dataset",
    "Delivery",
    "Encoding",
    "FeedMode",
    "FileBento",
    "RecordFlags",
    "Historical",
    "HistoricalGateway",
    "LiveGateway",
    "MemoryBento",
    "Packaging",
    "RollRule",
    "Schema",
    "SplitDuration",
    "SType",
    "SymbologyResolution",
]

# Set to either 'DEBUG' or 'INFO', controls console logging
log: Optional[str] = None
