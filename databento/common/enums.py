from enum import Enum, unique


@unique
class HistoricalGateway(Enum):
    """Represents a historical data center gateway location."""

    BO1 = "bo1"


@unique
class LiveGateway(Enum):
    """Represents a live data center gateway location."""

    ORIGIN = "origin"
    NY4 = "ny4"
    DC3 = "dc3"


@unique
class FeedMode(Enum):
    """Represents a data feed mode."""

    HISTORICAL = "historical"
    HISTORICAL_STREAMING = "historical-streaming"
    LIVE = "live"


@unique
class Dataset(Enum):
    """Represents a dataset code (string identifier)."""

    GLBX_MDP3 = "GLBX.MDP3"
    XNAS_ITCH = "XNAS.ITCH"


@unique
class Schema(Enum):
    """Represents a data record schema."""

    MBO = "mbo"
    MBP_1 = "mbp-1"
    MBP_10 = "mbp-10"
    TBBO = "tbbo"
    TRADES = "trades"
    OHLCV_1S = "ohlcv-1s"
    OHLCV_1M = "ohlcv-1m"
    OHLCV_1H = "ohlcv-1h"
    OHLCV_1D = "ohlcv-1d"
    DEFINITION = "definition"
    STATISTICS = "statistics"
    STATUS = "status"
    GATEWAY_ERROR = "gateway_error"
    SYMBOL_MAPPING = "symbol_mapping"


@unique
class Encoding(Enum):
    """Represents a data output encoding."""

    DBZ = "dbz"
    CSV = "csv"
    JSON = "json"


@unique
class Compression(Enum):
    """Represents a data compression format (if any)."""

    NONE = "none"
    ZSTD = "zstd"


@unique
class SplitDuration(Enum):
    """Represents the duration before splitting for each batched data file."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    NONE = "none"


@unique
class Packaging(Enum):
    """Represents the packaging method for batched data files."""

    NONE = "none"
    ZIP = "zip"
    TAR = "tar"


@unique
class Delivery(Enum):
    """Represents the delivery mechanism for batched data."""

    DOWNLOAD = "download"
    S3 = "s3"
    DISK = "disk"


@unique
class SType(Enum):
    """Represents a symbology type."""

    PRODUCT_ID = "product_id"
    NATIVE = "native"
    SMART = "smart"


@unique
class RollRule(Enum):
    """Represents a smart symbology roll rule."""

    VOLUME = 0
    OPEN_INTEREST = 1
    CALENDAR = 2


@unique
class SymbologyResolution(Enum):
    """
    Status code of symbology resolution.

    - OK: All symbol mappings resolved.
    - PARTIAL: One or more symbols did not resolve on at least one date.
    - NOT_FOUND: One or more symbols where not found on any date in range.
    """

    OK = 0
    PARTIAL = 1
    NOT_FOUND = 2


@unique
class Flags(Enum):
    """Represents record flags."""

    # Last message in the packet from the venue for a given `product_id`
    F_LAST = 1 << 7
    # Message sourced from a replay, such as a snapshot server
    F_SNAPSHOT = 1 << 5
    # Aggregated price level message, not an individual order
    F_MBP = 1 << 4
    # The `ts_recv` value is inaccurate (clock issues or reordering)
    F_BAD_TS_RECV = 1 << 3
