from datetime import date
from enum import Enum
from typing import Iterable, List, Optional, Union

import pandas as pd
from databento.common.enums import Flags


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


def maybe_symbols_list_to_string(
    symbols: Optional[Iterable[str]],
) -> Optional[str]:
    """
    Concatenate a symbols string (if not None).

    Parameters
    ----------
    symbols : iterable of str, optional
        The symbols to concatenate.

    Returns
    -------
    str or ``None``

    """
    if symbols is None:
        return None  # ALL

    if isinstance(symbols, str):
        return symbols.rstrip(",").upper()
    if isinstance(symbols, (tuple, list)):
        return ",".join(symbols).upper()
    else:
        raise TypeError(f"invalid symbols type, was {type(symbols)}")


def maybe_datetime_to_string(
    value: Optional[Union[pd.Timestamp, date, str]],
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

    return str(pd.to_datetime(value)).replace(" ", "T")


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
