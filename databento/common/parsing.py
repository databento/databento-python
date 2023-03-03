from collections.abc import Iterable as IterableABC
from datetime import date
from functools import partial, singledispatch
from typing import Iterable, Optional, Union

import pandas as pd
from databento.common.enums import SType
from databento.common.symbology import ALL_SYMBOLS
from databento.common.validation import validate_smart_symbol


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


@singledispatch
def optional_symbols_list_to_string(
    symbols: Optional[Union[Iterable[str], Iterable[int], str, int]],
    stype_in: SType,
) -> str:
    """
    Concatenate a symbols string or iterable of symbol strings (if not None).

    Parameters
    ----------
    symbols : iterable of str, iterable of int, str, or int optional
        The symbols to concatenate.
    stype_in : SType
        The input symbology type for the request.

    Returns
    -------
    str

    Notes
    -----
    If None is given, ALL_SYMBOLS is returned.

    """
    raise TypeError(
        f"`{symbols}` is not a valid type for symbol input; "
        "allowed types are Iterable[str], Iterable[int], str, int, and None.",
    )


@optional_symbols_list_to_string.register
def _(_: None, __: SType) -> str:
    """
    Dispatch method for optional_symbols_list_to_string.
    Handles None which defaults to ALL_SYMBOLS.

    See Also
    --------
    optional_symbols_list_to_string

    """
    return ALL_SYMBOLS


@optional_symbols_list_to_string.register
def _(symbols: int, stype_in: SType) -> str:
    """
    Dispatch method for optional_symbols_list_to_string.
    Handles int, alerting when an integer is given for
    STypes that expect strings.

    See Also
    --------
    optional_symbols_list_to_string

    """
    if stype_in == SType.PRODUCT_ID:
        return str(symbols)
    raise ValueError(
        f"value `{symbols}` is not a valid symbol for stype {stype_in}; "
        "did you mean to use `product_id`?",
    )


@optional_symbols_list_to_string.register
def _(symbols: str, stype_in: SType) -> str:
    """
    Dispatch method for optional_symbols_list_to_string.
    Handles str, splitting on commas and validating smart symbology.

    See Also
    --------
    optional_symbols_list_to_string

    """
    if not symbols:
        raise ValueError(
            f"value `{symbols}` is not a valid symbol for {stype_in}; "
            "an empty string is not allowed",
        )

    if "," in symbols:
        symbol_to_string = partial(
            optional_symbols_list_to_string,
            stype_in=stype_in,
        )
        symbol_list = symbols.strip().strip(",").split(",")
        return ",".join(map(symbol_to_string, symbol_list))

    if stype_in == SType.SMART:
        return validate_smart_symbol(symbols)
    return symbols.strip().upper()


@optional_symbols_list_to_string.register(cls=IterableABC)
def _(symbols: Union[Iterable[str], Iterable[int]], stype_in: SType) -> str:
    """
    Dispatch method for optional_symbols_list_to_string.
    Handles Iterables by dispatching the individual members.

    See Also
    --------
    optional_symbols_list_to_string

    """
    symbol_to_string = partial(
        optional_symbols_list_to_string,
        stype_in=stype_in,
    )
    return ",".join(map(symbol_to_string, symbols))


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
