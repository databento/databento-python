from __future__ import annotations

from enum import Enum
from enum import Flag
from enum import IntFlag
from enum import unique
from typing import Callable, TypeVar

from databento_dbn import ImbalanceMsg
from databento_dbn import InstrumentDefMsg
from databento_dbn import MBOMsg
from databento_dbn import MBP1Msg
from databento_dbn import MBP10Msg
from databento_dbn import OHLCVMsg
from databento_dbn import StatMsg
from databento_dbn import TradeMsg

from databento.live import DBNRecord


M = TypeVar("M", bound=Enum)


def coercible(enum_type: type[M]) -> type[M]:
    """Decorate coercible enumerations.

    Decorating an Enum class with this function will intercept calls to
    __new__ and perform a type coercion for the passed value. The type conversion
    function is chosen based on the subclass of the Enum type.

    Currently supported subclasses types:
        int
            values are passed to int()
        str
            values are passed to str(), the result is also lowercased

    Parameters
    ----------
    enum_type : EnumMeta
        The deocrated Enum type.

    Returns
    -------
    EnumMeta

    Raises
    ------
    ValueError
        If an invalid value of the Enum is given.

    Notes
    -----
    This decorator makes some assuptions about your Enum class.
        1. Your attribute names are all UPPERCASE
        2. Your attribute values are all lowercase

    """
    _new: Callable[[type[M], object], M] = enum_type.__new__

    def _cast_str(value: object) -> str:
        return str(value).lower()

    coerce_fn: Callable[[object], str | int]
    if issubclass(enum_type, int):
        coerce_fn = int
    elif issubclass(enum_type, str):
        coerce_fn = _cast_str
    else:
        raise TypeError(f"{enum_type} does not a subclass a coercible type.")

    def coerced_new(enum: type[M], value: object) -> M:
        if value is None:
            raise TypeError(
                f"value `{value}` is not coercible to {enum_type.__name__}.",
            )
        try:
            return _new(enum, coerce_fn(value))
        except ValueError as ve:
            name_to_try = str(value).replace(".", "_").replace("-", "_").upper()
            named = enum._member_map_.get(name_to_try)
            if named is not None:
                return named
            enum_values = tuple(value for value in enum._value2member_map_)

            raise ValueError(
                f"value `{value}` is not a member of {enum_type.__name__}. "
                f"use one of {enum_values}.",
            ) from ve

    setattr(enum_type, "__new__", coerced_new)

    return enum_type


class StringyMixin:
    """
    Mixin class for overloading __str__ on Enum types.
    This will use the Enumerations subclass, if any, to modify
    the behavior of str().

    For subclasses of enum.Flag a comma separated string of names is returned.
    For integer enumerations, the lowercase member name is returned.
    For string enumerations, the value is returned.

    """

    def __str__(self) -> str:
        if isinstance(self, Flag):
            return ", ".join(f.name.lower() for f in self.__class__ if f in self)
        if isinstance(self, int):
            return getattr(self, "name").lower()
        return getattr(self, "value")


@unique
@coercible
class HistoricalGateway(StringyMixin, str, Enum):
    """Represents a historical data center gateway location."""

    BO1 = "https://hist.databento.com"


@unique
@coercible
class FeedMode(StringyMixin, str, Enum):
    """Represents a data feed mode."""

    HISTORICAL = "historical"
    HISTORICAL_STREAMING = "historical-streaming"
    LIVE = "live"


@unique
@coercible
class Dataset(StringyMixin, str, Enum):
    """Represents a dataset code (string identifier)."""

    GLBX_MDP3 = "GLBX.MDP3"
    XNAS_ITCH = "XNAS.ITCH"


@unique
@coercible
class Schema(StringyMixin, str, Enum):
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
    IMBALANCE = "imbalance"
    STATISTICS = "statistics"

    def get_record_type(self) -> type[DBNRecord]:
        if self == Schema.MBO:
            return MBOMsg
        if self == Schema.MBP_1:
            return MBP1Msg
        if self == Schema.MBP_10:
            return MBP10Msg
        if self == Schema.TBBO:
            return MBP1Msg
        if self == Schema.TRADES:
            return TradeMsg
        if self == Schema.OHLCV_1S:
            return OHLCVMsg
        if self == Schema.OHLCV_1M:
            return OHLCVMsg
        if self == Schema.OHLCV_1H:
            return OHLCVMsg
        if self == Schema.OHLCV_1D:
            return OHLCVMsg
        if self == Schema.DEFINITION:
            return InstrumentDefMsg
        if self == Schema.IMBALANCE:
            return ImbalanceMsg
        if self == Schema.STATISTICS:
            return StatMsg
        raise NotImplementedError(f"No message type for {self}")


@unique
@coercible
class Encoding(StringyMixin, str, Enum):
    """Represents a data output encoding."""

    DBN = "dbn"
    CSV = "csv"
    JSON = "json"


@unique
@coercible
class Compression(StringyMixin, str, Enum):
    """Represents a data compression format (if any)."""

    NONE = "none"
    ZSTD = "zstd"


@unique
@coercible
class SplitDuration(StringyMixin, str, Enum):
    """Represents the duration before splitting for each batched data file."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    NONE = "none"


@unique
@coercible
class Packaging(StringyMixin, str, Enum):
    """Represents the packaging method for batched data files."""

    NONE = "none"
    ZIP = "zip"
    TAR = "tar"


@unique
@coercible
class Delivery(StringyMixin, str, Enum):
    """Represents the delivery mechanism for batched data."""

    DOWNLOAD = "download"
    S3 = "s3"
    DISK = "disk"


@unique
@coercible
class SType(StringyMixin, str, Enum):
    """Represents a symbology type."""

    INSTRUMENT_ID = "instrument_id"
    RAW_SYMBOL = "raw_symbol"
    PARENT = "parent"
    CONTINUOUS = "continuous"


@unique
@coercible
class RollRule(StringyMixin, str, Enum):
    """Represents a smart symbology roll rule."""

    VOLUME = "volume"
    OPEN_INTEREST = "open_interst"
    CALENDAR = "calendar"


@unique
@coercible
class SymbologyResolution(StringyMixin, str, Enum):
    """
    Status code of symbology resolution.

    - OK: All symbol mappings resolved.
    - PARTIAL: One or more symbols did not resolve on at least one date.
    - NOT_FOUND: One or more symbols where not found on any date in range.
    """

    OK = "ok"
    PARTIAL = "partial"
    NOT_FOUND = "not_found"


@unique
@coercible
# Ignore type to work around mypy bug https://github.com/python/mypy/issues/9319
class RecordFlags(StringyMixin, IntFlag):  # type: ignore
    """Represents record flags.

    F_LAST
        Last message in the packet from the venue for a given `instrument_id`.
    F_SNAPSHOT
        Message sourced from a replay, such as a snapshot server.
    F_MBP
        Aggregated price level message, not an individual order.
    F_BAD_TS_RECV
        The `ts_recv` value is inaccurate (clock issues or reordering).

    Other bits are reserved and have no current meaning.
    """

    F_LAST = 128
    F_SNAPSHOT = 32
    F_MBP = 16
    F_BAD_TS_RECV = 8
