import logging
import warnings

from databento_dbn import CMBP1Msg
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
from databento_dbn import StatType
from databento_dbn import StatusMsg
from databento_dbn import SType
from databento_dbn import SymbolMappingMsg
from databento_dbn import SystemMsg
from databento_dbn import TradeMsg
from databento_dbn.v2 import BBO1MMsg
from databento_dbn.v2 import BBO1SMsg
from databento_dbn.v2 import CBBO1MMsg
from databento_dbn.v2 import CBBO1SMsg
from databento_dbn.v2 import TBBOMsg
from databento_dbn.v2 import TCBBOMsg

from databento.common import API_VERSION
from databento.common import bentologging
from databento.common import symbology
from databento.common.dbnstore import DBNStore
from databento.common.enums import Delivery
from databento.common.enums import FeedMode
from databento.common.enums import HistoricalGateway
from databento.common.enums import Packaging
from databento.common.enums import ReconnectPolicy
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
from databento.historical.client import Historical
from databento.live.client import Live
from databento.reference.client import Reference
from databento.version import __version__  # noqa


__all__ = [
    "API_VERSION",
    "BBO1MMsg",
    "BBO1SMsg",
    "BentoClientError",
    "BentoError",
    "BentoHttpError",
    "BentoServerError",
    "CBBO1MMsg",
    "CBBO1SMsg",
    "CMBP1Msg",
    "Compression",
    "DBNRecord",
    "DBNStore",
    "Dataset",
    "Delivery",
    "Encoding",
    "ErrorMsg",
    "FeedMode",
    "Historical",
    "HistoricalGateway",
    "ImbalanceMsg",
    "InstrumentDefMsg",
    "InstrumentMap",
    "Live",
    "MBOMsg",
    "MBP1Msg",
    "MBP10Msg",
    "Metadata",
    "OHLCVMsg",
    "Packaging",
    "Publisher",
    "ReconnectPolicy",
    "RecordFlags",
    "Reference",
    "RollRule",
    "SType",
    "Schema",
    "SplitDuration",
    "StatMsg",
    "StatType",
    "StatusMsg",
    "SymbolMappingMsg",
    "SymbologyResolution",
    "SystemMsg",
    "TBBOMsg",
    "TBBOMsg",
    "TCBBOMsg",
    "TradeMsg",
    "Venue",
]

# Setup logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Setup deprecation warnings
warnings.simplefilter("always", DeprecationWarning)

# Convenience imports
enable_logging = bentologging.enable_logging
read_dbn = DBNStore.from_file
map_symbols_csv = symbology.map_symbols_csv
map_symbols_json = symbology.map_symbols_json
