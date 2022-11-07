from datetime import date
from enum import Enum
from typing import Iterable, List, Optional, Union

import pandas as pd
from databento.common.enums import Compression, Encoding, Flags, Schema, SType


def enum_or_str_lowercase(
    value: Union[Enum, str],
    param: str,
) -> str:
    """
    Return the given value parsed to a lowercase string if possible.

    Parameters
    ----------
    value : Enum or str
        The value to parse.
    param : str
        The name of the parameter being validated (for any error message).

    Returns
    -------
    str

    Raises
    ------
    TypeError
        If value is not of type NoneType, Enum or str.

    """
    if isinstance(value, Enum):
        return value.value.lower()
    elif isinstance(value, str):
        if not value.isspace():
            return value.lower()

    raise TypeError(f"invalid `{param}` type, was {type(value)}.")


def maybe_enum_or_str_lowercase(
    value: Optional[Union[Enum, str]],
    param: str,
) -> Optional[str]:
    """
    Return the given value parsed to a lowercase string if possible.

    Parameters
    ----------
    value : Enum or str, optional
        The value to parse.
    param : str
        The name of the parameter being validated (for any error message).

    Returns
    -------
    str or ``None``

    Raises
    ------
    TypeError
        If value is not of type NoneType, Enum or str.

    """
    if value is None:
        return value
    return enum_or_str_lowercase(value, param)


def values_list_to_string(
    values: Union[Iterable[str], str],
) -> str:
    """
    Concatenate a values string or iterable of string values.

    Parameters
    ----------
    values : iterable of str or str
        The values to concatenate.

    Returns
    -------
    str

    """
    if isinstance(values, str):
        return values.strip().rstrip(",").lower()
    elif isinstance(values, Iterable):
        return ",".join(values).strip().lower()
    else:
        raise TypeError(f"invalid values type, was {type(values)}")


def maybe_values_list_to_string(
    values: Optional[Union[Iterable[str], str]],
) -> Optional[str]:
    """
    Concatenate a values string or iterable of string values (if not None).

    Parameters
    ----------
    values : iterable of str or str, optional
        The values to concatenate.

    Returns
    -------
    str or ``None``

    """
    if values is None:
        return None

    return values_list_to_string(values)


def maybe_symbols_list_to_string(
    symbols: Optional[Union[Iterable[str], str]],
) -> Optional[str]:
    """
    Concatenate a symbols string or iterable of symbol strings (if not None).

    Parameters
    ----------
    symbols : iterable of str or str, optional
        The symbols to concatenate.

    Returns
    -------
    str or ``None``

    """
    if symbols is None:
        return None  # All symbols

    if isinstance(symbols, str):
        return symbols.strip().rstrip(",").upper()
    elif isinstance(symbols, Iterable):
        return ",".join(symbols).strip().upper()
    else:
        raise TypeError(f"invalid symbols type, was {type(symbols)}")


def maybe_date_to_string(value: Optional[Union[date, str]]) -> Optional[str]:
    """
    Return a valid date string from the given value (if not None).

    Parameters
    ----------
    value : date or str, optional
        The value to parse.

    Returns
    -------
    str or ``None``

    """
    if value is None:
        return None

    return str(pd.to_datetime(value).date())


def datetime_to_string(value: Union[pd.Timestamp, date, str, int]) -> str:
    """
    Return a valid datetime string from the given value.

    Parameters
    ----------
    value : pd.Timestamp or date or str
        The value to parse.

    Returns
    -------
    str

    """
    return str(pd.to_datetime(value)).replace(" ", "T")


def maybe_datetime_to_string(
    value: Optional[Union[pd.Timestamp, date, str, int]],
) -> Optional[str]:
    """
    Return a valid datetime string from the given value (if not None).

    Parameters
    ----------
    value : pd.Timestamp or date or str, optional
        The value to parse.

    Returns
    -------
    str or ``None``

    """
    if value is None:
        return None

    return datetime_to_string(value)


def parse_flags(value: int, apply_bitmask: bool = False) -> List[str]:
    """
    Return an array of flag strings from the given flags value.

    This convenience function parses the values from the flags field.
    The flags value is a combination of event packet end and matching engine
    status. Using a technique involving bit shifts, there can be more than one
    flag represented by a single integer.

    Possible values include:
     - F_LAST: Last msg in packet (flags < 0).
     - F_HALT: Exchange-independent HALT signal.
     - F_RESET: Drop book, reset symbol for this exchange.
     - F_DUPID: This OrderID has valid fresh duplicate (Iceberg, etc).
     - F_MBP: This is SIP/MBP ADD message, single per price level.
     - F_RESERVED2: Reserved for future use (no current meaning).
     - F_RESERVED1: Reserved for future use (no current meaning).
     - F_RESERVED0: Reserved for future use (no current meaning).

    Parameters
    ----------
    value : int
        The 'flags' field value.
    apply_bitmask : bool, default False
        If the AND 0xff bitmask should be applied to remove the values sign.
        This would only be required when parsing raw binary data.

    Returns
    -------
    list[str]

    """
    if apply_bitmask:
        value = value & 0xFF
    return [f.name for f in Flags if value & f.value != 0]


# TODO(cs): We should probably change the enum values to ints so as to avoid
#  all of these conversion functions. If this implementation remains then will
#  add exhaustive unit tests (27/6/22)


def schema_to_int(schema: Schema) -> int:
    assert isinstance(schema, Schema)

    if schema == Schema.MBO:
        return 0
    elif schema == Schema.MBP_1:
        return 1
    elif schema == Schema.MBP_10:
        return 2
    elif schema == Schema.TBBO:
        return 3
    elif schema == Schema.TRADES:
        return 4
    elif schema == Schema.OHLCV_1S:
        return 5
    elif schema == Schema.OHLCV_1M:
        return 6
    elif schema == Schema.OHLCV_1H:
        return 7
    elif schema == Schema.OHLCV_1D:
        return 8
    elif schema == Schema.DEFINITION:
        return 9
    elif schema == Schema.STATISTICS:
        return 10
    elif schema == Schema.STATUS:
        return 11
    else:
        raise NotImplementedError(
            f"The enum value '{schema.value}' "
            f"has not been implemented for conversion",
        )


def int_to_schema(value: int) -> Schema:
    if value == 0:
        return Schema.MBO
    elif value == 1:
        return Schema.MBP_1
    elif value == 2:
        return Schema.MBP_10
    elif value == 3:
        return Schema.TBBO
    elif value == 4:
        return Schema.TRADES
    elif value == 5:
        return Schema.OHLCV_1S
    elif value == 6:
        return Schema.OHLCV_1M
    elif value == 7:
        return Schema.OHLCV_1H
    elif value == 8:
        return Schema.OHLCV_1D
    elif value == 9:
        return Schema.DEFINITION
    elif value == 10:
        return Schema.STATISTICS
    elif value == 11:
        return Schema.STATUS
    else:
        raise NotImplementedError(
            f"The int value '{value}' " f"cannot be represented with the enum",
        )


def stype_to_int(stype: SType) -> int:
    assert isinstance(stype, SType)

    if stype == SType.PRODUCT_ID:
        return 0
    elif stype == SType.NATIVE:
        return 1
    elif stype == SType.SMART:
        return 2
    else:
        raise NotImplementedError(
            f"The enum value '{stype.value}' "
            f"has not been implemented for conversion",
        )


def int_to_stype(value: int) -> SType:
    if value == 0:
        return SType.PRODUCT_ID
    elif value == 1:
        return SType.NATIVE
    elif value == 2:
        return SType.SMART
    else:
        raise NotImplementedError(
            f"The int value '{value}' " f"cannot be represented with the enum",
        )


def encoding_to_int(encoding: Encoding) -> int:
    assert isinstance(encoding, Encoding)

    if encoding == Encoding.DBZ:
        return 0
    elif encoding == Encoding.CSV:
        return 1
    elif encoding == Encoding.JSON:
        return 2
    else:
        raise NotImplementedError(
            f"The enum value '{encoding.value}' "
            f"has not been implemented for conversion",
        )


def int_to_encoding(value: int) -> Encoding:
    if value == 0:
        return Encoding.DBZ
    elif value == 1:
        return Encoding.CSV
    elif value == 2:
        return Encoding.JSON
    else:
        raise NotImplementedError(
            f"The int value '{value}' " f"cannot be represented with the enum",
        )


def compression_to_int(compression: Compression) -> int:
    assert isinstance(compression, Compression)

    if compression == Compression.NONE:
        return 0
    elif compression == Compression.ZSTD:
        return 1
    else:
        raise NotImplementedError(
            f"The enum value '{compression.value}' "
            f"has not been implemented for conversion",
        )


def int_to_compression(value: int) -> Compression:
    if value == 0:
        return Compression.NONE
    elif value == 1:
        return Compression.ZSTD
    else:
        raise NotImplementedError(
            f"The int value '{value}' " f"cannot be represented with the enum",
        )
