import logging

from databento.common import utility
from databento.common.dbnstore import DBNStore
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
from databento.common.error import (
    BentoClientError,
    BentoError,
    BentoHttpError,
    BentoServerError,
)
from databento.historical.api import API_VERSION
from databento.historical.client import Historical
from databento.version import __version__  # noqa


__all__ = [
    "API_VERSION",
    "DBNStore",
    "BentoClientError",
    "BentoError",
    "BentoHttpError",
    "BentoServerError",
    "Compression",
    "Dataset",
    "Delivery",
    "Encoding",
    "FeedMode",
    "RecordFlags",
    "Historical",
    "HistoricalGateway",
    "LiveGateway",
    "Packaging",
    "RollRule",
    "Schema",
    "SplitDuration",
    "SType",
    "SymbologyResolution",
]

# Setup logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Convenience imports
enable_logging = utility.enable_logging
from_dbn = DBNStore.from_file
