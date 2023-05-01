import logging
import warnings

from databento.common import bentologging
from databento.common.dbnstore import DBNStore
from databento.common.enums import (
    Compression,
    Dataset,
    Delivery,
    Encoding,
    FeedMode,
    HistoricalGateway,
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
from databento.live.client import Live
from databento.live.dbn import DBNRecord, DBNStruct
from databento.version import __version__  # noqa
from databento_dbn import (
    ErrorMsg,
    ImbalanceMsg,
    InstrumentDefMsg,
    MBOMsg,
    MBP1Msg,
    MBP10Msg,
    Metadata,
    OHLCVMsg,
    SymbolMappingMsg,
    SystemMsg,
    TradeMsg,
)


__all__ = [
    "API_VERSION",
    "DBNStore",
    "DBNRecord",
    "DBNStruct",
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
    "Live",
    "Packaging",
    "RollRule",
    "Schema",
    "SplitDuration",
    "SType",
    "SymbologyResolution",
    # DBN Record Types
    "Metadata",
    "MBOMsg",
    "MBP1Msg",
    "MBP10Msg",
    "TradeMsg",
    "OHLCVMsg",
    "InstrumentDefMsg",
    "ImbalanceMsg",
    "ErrorMsg",
    "SystemMsg",
    "SymbolMappingMsg",
]

# Setup logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Setup deprecation warnings
warnings.simplefilter("always", DeprecationWarning)

# Convenience imports
enable_logging = bentologging.enable_logging
from_dbn = DBNStore.from_file
