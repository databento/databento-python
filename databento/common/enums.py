from __future__ import annotations

from enum import Enum
from enum import Flag
from enum import IntFlag
from enum import unique
from typing import Callable
from typing import TypeVar


M = TypeVar("M", bound=Enum)


def coercible(enum_type: type[M]) -> type[M]:
    """
    Decorate coercible enumerations.

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
        The decorated Enum type.

    Returns
    -------
    EnumMeta

    Raises
    ------
    ValueError
        If an invalid value of the Enum is given.

    Notes
    -----
    This decorator makes some assumptions about your Enum class.
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
            raise ValueError(
                f"value `{value}` is not coercible to {enum_type.__name__}.",
            )
        try:
            return _new(enum, coerce_fn(value))
        except ValueError:
            name_to_try = str(value).replace(".", "_").replace("-", "_").upper()
            named = enum._member_map_.get(name_to_try)
            if named is not None:
                return named
            enum_values = list(value for value in enum._value2member_map_)

            raise ValueError(
                f"The `{value}` was not a valid value of {enum_type.__name__}"
                f", was '{value}'. Use any of {enum_values}.",
            ) from None

    setattr(enum_type, "__new__", coerced_new)

    return enum_type


class StringyMixin:
    """
    Mixin class for overloading __str__ on Enum types. This will use the
    Enumerations subclass, if any, to modify the behavior of str().

    For subclasses of enum.Flag a comma separated string of names is
    returned. For integer enumerations, the lowercase member name is
    returned. For string enumerations, the value is returned.

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
    """
    Represents a historical data center gateway location.
    """

    BO1 = "https://hist.databento.com"


@unique
@coercible
class FeedMode(StringyMixin, str, Enum):
    """
    Represents a data feed mode.
    """

    HISTORICAL = "historical"
    HISTORICAL_STREAMING = "historical-streaming"
    LIVE = "live"


@unique
@coercible
class SplitDuration(StringyMixin, str, Enum):
    """
    Represents the duration before splitting for each batched data file.
    """

    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    NONE = "none"


@unique
@coercible
class Packaging(StringyMixin, str, Enum):
    """
    Represents the packaging method for batched data files.
    """

    NONE = "none"
    ZIP = "zip"
    TAR = "tar"


@unique
@coercible
class Delivery(StringyMixin, str, Enum):
    """
    Represents the delivery mechanism for batched data.
    """

    DOWNLOAD = "download"


@unique
@coercible
class RollRule(StringyMixin, str, Enum):
    """
    Represents a smart symbology roll rule.
    """

    VOLUME = "volume"
    OPEN_INTEREST = "open_interest"
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
    """
    Represents record flags.

    F_LAST
        Marks the last record in a single event for a given `instrument_id`.
    F_TOB
        Indicates a top-of-book message, not an individual order.
    F_SNAPSHOT
        Message sourced from a replay, such as a snapshot server.
    F_MBP
        Aggregated price level message, not an individual order.
    F_BAD_TS_RECV
        The `ts_recv` value is inaccurate (clock issues or reordering).
    F_MAYBE_BAD_BOOK
        Indicates an unrecoverable gap was detected in the channel.
    F_PUBLISHER_SPECIFIC
        Indicates a publisher-specific event.

    Other bits are reserved and have no current meaning.

    """

    F_LAST = 128
    F_TOB = 64
    F_SNAPSHOT = 32
    F_MBP = 16
    F_BAD_TS_RECV = 8
    F_MAYBE_BAD_BOOK = 4
    F_PUBLISHER_SPECIFIC = 2


@unique
@coercible
class ReconnectPolicy(StringyMixin, str, Enum):
    """
    Live session reconnection policy.
    """

    NONE = "none"
    RECONNECT = "reconnect"


@unique
@coercible
class PriceType(StringyMixin, str, Enum):
    """
    Price type for DataFrame price fields.
    """

    FIXED = "fixed"
    FLOAT = "float"
    DECIMAL = "decimal"


@unique
@coercible
class JobState(StringyMixin, str, Enum):
    """
    Represents the different states for batch jobs.
    """

    QUEUED = "queued"
    PROCESSING = "processing"
    DONE = "done"
    EXPIRED = "expired"
