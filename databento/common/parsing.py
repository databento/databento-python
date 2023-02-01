from datetime import date
from typing import Iterable, List, Optional, Union

import pandas as pd
from databento.common.enums import SType
from databento.common.symbology import ALL_SYMBOLS


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
    if isinstance(values, Iterable):
        return ",".join(values).strip().lower()
    raise TypeError(f"invalid values type, was {type(values)}")


def optional_values_list_to_string(
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
    str or `None`

    """
    if values is None:
        return None
    return values_list_to_string(values)


def optional_symbols_list_to_string(
    symbols: Optional[Union[Iterable[str], str]],
    stype_in: SType,
) -> str:
    """
    Concatenate a symbols string or iterable of symbol strings (if not None).

    Parameters
    ----------
    symbols : iterable of str or str, optional
        The symbols to concatenate.
    stype_in : SType
        The input symbology type for the request.

    Returns
    -------
    str

    """
    if symbols is None:
        return ALL_SYMBOLS

    symbols_list = symbols.split(",") if isinstance(symbols, str) else list(symbols)
    cleaned_symbols: List[str] = []
    for symbol in symbols_list:
        if not symbol:
            continue
        symbol = symbol.strip().upper()
        if stype_in == SType.SMART:
            pieces: List[str] = symbol.split(".")
            if len(pieces) == 3:
                symbol = f"{pieces[0]}.{pieces[1].lower()}.{pieces[2]}"

        cleaned_symbols.append(symbol)

    return ",".join(cleaned_symbols)


def optional_date_to_string(value: Optional[Union[date, str]]) -> Optional[str]:
    """
    Return a valid date string from the given value (if not None).

    Parameters
    ----------
    value : date or str, optional
        The value to parse.

    Returns
    -------
    str or `None`

    """
    if value is None:
        return None

    return datetime_to_date_string(value)


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


def datetime_to_date_string(value: Union[pd.Timestamp, date, str, int]) -> str:
    """
    Return a valid date string from the given value.

    Parameters
    ----------
    value : pd.Timestamp or date or str
        The value to parse.

    Returns
    -------
    str

    """
    return str(pd.to_datetime(value).date())


def optional_datetime_to_string(
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
    str or `None`

    """
    if value is None:
        return None

    return datetime_to_string(value)
