import logging
import warnings

from databento_dbn import Compression
from databento_dbn import Encoding
from databento_dbn import ErrorMsg
from databento_dbn import ImbalanceMsg
from databento_dbn import InstrumentDefMsg
from databento_dbn import MBOMsg
from databento_dbn import MBP1Msg
from databento_dbn import MBP10Msg
from databento_dbn import Metadata
from databento_dbn import OHLCVMsg
from databento_dbn import Schema
from databento_dbn import StatMsg
from databento_dbn import SType
from databento_dbn import SymbolMappingMsg
from databento_dbn import SystemMsg
from databento_dbn import TradeMsg

from databento.common import bentologging
from databento.common import symbology
from databento.common.dbnstore import DBNStore
from databento.common.enums import Delivery
from databento.common.enums import FeedMode
from databento.common.enums import HistoricalGateway
from databento.common.enums import Packaging
from databento.common.enums import RecordFlags
from databento.common.enums import RollRule
from databento.common.enums import SplitDuration
from databento.common.enums import SymbologyResolution
from databento.common.error import BentoClientError
from databento.common.error import BentoError
from databento.common.error import BentoHttpError
from databento.common.error import BentoServerError
from databento.common.publishers import Dataset
from databento.common.publishers import Publisher
from databento.common.publishers import Venue
from databento.common.symbology import InstrumentMap
from databento.common.types import DBNRecord
from databento.historical.api import API_VERSION
from databento.historical.client import Historical
from databento.live.client import Live
from databento.version import __version__  # noqa


__all__ = [
    "API_VERSION",
    "DBNStore",
    "DBNRecord",
    "BentoClientError",
    "BentoError",
    "BentoHttpError",
    "BentoServerError",
    "Compression",
    "Publisher",
    "Dataset",
    "Venue",
    "Delivery",
    "Encoding",
    "FeedMode",
    "RecordFlags",
    "Historical",
    "HistoricalGateway",
    "InstrumentMap",
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
    "StatMsg",
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
map_symbols_csv = symbology.map_symbols_csv
map_symbols_json = symbology.map_symbols_json
