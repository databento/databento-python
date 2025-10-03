from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from datetime import datetime
from functools import partial
from functools import singledispatch
from io import BytesIO
from io import TextIOWrapper
from numbers import Integral
from typing import IO
from typing import Any

import pandas as pd
import zstandard
from databento_dbn import SType

from databento.common.constants import ALL_SYMBOLS
from databento.common.enums import JobState
from databento.common.validation import validate_enum
from databento.common.validation import validate_smart_symbol


def values_list_to_string(
    values: Iterable[str] | str,
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
    values: Iterable[str] | str | None,
) -> str | None:
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


def optional_states_list_to_string(
    states: Iterable[JobState | str] | JobState | str | None,
) -> str | None:
    """
    Concatenate a states string or iterable of string states (if not None).

    Parameters
    ----------
    states : Iterable[JobState | str] | JobState | str | None
        The states to concatenate.

    Returns
    -------
    str or `None`

    """
    if states is None:
        return None
    elif isinstance(states, (JobState, str)):
        return str(states)
    else:
        states_list = [validate_enum(state, JobState, "state").value for state in states]
        return ",".join(states_list)


def optional_string_to_list(
    value: Iterable[str] | str | None,
) -> Iterable[str] | list[str] | None:
    """
    Convert a comma-separated string into a list of strings, or return the
    original input if not a string.

    Parameters
    ----------
    value : iterable of str or str, optional
        The input value to be parsed.

    Returns
    -------
    Iterable[str] | list[str] | `None`

    """
    return value.strip().strip(",").split(",") if isinstance(value, str) else value


def optional_symbols_list_to_list(
    symbols: Iterable[str | int | Integral] | str | int | Integral | None,
    stype_in: SType,
) -> list[str]:
    """
    Create a list from an optional symbols string or iterable of symbol
    strings. If symbols is `None`, this function returns `[ALL_SYMBOLS]`.

    Parameters
    ----------
    symbols : Iterable of str or int or Number, or str or int or Number, optional
        The symbols to concatenate; or `None`.
    stype_in : SType
        The input symbology type for the request.

    Returns
    -------
    list[str]

    See Also
    --------
    symbols_list_to_list

    """
    if symbols is None:
        return [ALL_SYMBOLS]
    return symbols_list_to_list(symbols, stype_in)


@singledispatch
def symbols_list_to_list(
    symbols: Iterable[str | int | Integral] | str | int | Integral,
    stype_in: SType,
) -> list[str]:
    """
    Create a list from a symbols string or iterable of symbol strings.

    Parameters
    ----------
    symbols : Iterable of str or int or Number, or str or int or Number
        The symbols to concatenate.
    stype_in : SType
        The input symbology type for the request.

    Returns
    -------
    list[str]

    """
    raise TypeError(
        f"`{symbols}` is not a valid type for symbol input; "
        "allowed types are Iterable[str | int], str, and int.",
    )


@symbols_list_to_list.register(cls=Integral)
def _(symbols: Integral, stype_in: SType) -> list[str]:
    """
    Dispatch method for optional_symbols_list_to_list. Handles integral types,
    alerting when an integer is given for STypes that expect strings.

    See Also
    --------
    symbols_list_to_list

    """
    if stype_in == SType.INSTRUMENT_ID:
        return [str(symbols)]
    raise ValueError(
        f"value `{symbols}` is not a valid symbol for stype {stype_in}; "
        "did you mean to use `instrument_id`?",
    )


@symbols_list_to_list.register(cls=str)
def _(symbols: str, stype_in: SType) -> list[str]:
    """
    Dispatch method for optional_symbols_list_to_list. Handles str, splitting
    on commas and validating smart symbology.

    See Also
    --------
    symbols_list_to_list

    """
    if not symbols:
        raise ValueError(
            f"value `{symbols}` is not a valid symbol for {stype_in}; "
            "an empty string is not allowed",
        )

    symbol_list = symbols.strip().strip(",").split(",")

    if stype_in in (SType.PARENT, SType.CONTINUOUS):
        return list(map(str.strip, map(validate_smart_symbol, symbol_list)))

    return list(map(str.upper, map(str.strip, symbol_list)))


@symbols_list_to_list.register(cls=Iterable)
def _(symbols: Iterable[Any], stype_in: SType) -> list[str]:
    """
    Dispatch method for optional_symbols_list_to_list. Handles Iterables by
    dispatching the individual members.

    See Also
    --------
    symbols_list_to_list

    """
    symbol_to_list = partial(
        symbols_list_to_list,
        stype_in=stype_in,
    )
    aggregated: list[str] = []
    for sym in map(symbol_to_list, symbols):
        aggregated.extend(sym)
    return aggregated


def optional_date_to_string(value: date | str | None) -> str | None:
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

    return date_to_string(value)


def datetime_to_string(value: pd.Timestamp | datetime | date | str | int) -> str:
    """
    Return a valid datetime string from the given value.

    Parameters
    ----------
    value : pd.Timestamp, datetime, date, str, or int
        The value to parse.

    Returns
    -------
    str

    """
    if isinstance(value, str):
        return value
    elif isinstance(value, int):
        return str(value)
    elif isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, datetime):
        return value.isoformat()
    else:
        return pd.to_datetime(value).isoformat()


def date_to_string(value: date | str) -> str:
    """
    Return a valid date string from the given value.

    Parameters
    ----------
    value : date or str
        The value to parse.

    Returns
    -------
    str

    """
    if isinstance(value, str):
        return value
    elif type(value) is date:
        return value.isoformat()
    else:
        raise TypeError(f"`{type(value)} is not supported. Only `date` and `str` are supported.")


def optional_datetime_to_string(
    value: pd.Timestamp | datetime | date | str | int | None,
) -> str | None:
    """
    Return a valid datetime string from the given value (if not None).

    Parameters
    ----------
    value : pd.Timestamp, datetime, date, str, or int, optional
        The value to parse.

    Returns
    -------
    str or `None`

    """
    if value is None:
        return None

    return datetime_to_string(value)


def datetime_to_unix_nanoseconds(
    value: pd.Timestamp | datetime | date | str | int,
) -> int:
    """
    Return a valid UNIX nanosecond timestamp from the given value.

    Parameters
    ----------
    value : pd.Timestamp, datetime, date, str, or int
        The value to parse.

    Returns
    -------
    int

    """
    if isinstance(value, int):
        return value  # no checking on integer values
    elif isinstance(value, date):
        return pd.to_datetime(value, utc=True).value
    elif isinstance(value, pd.Timestamp):
        return value.value
    else:
        try:
            nanoseconds = pd.to_datetime(value, utc=True).value
        except Exception:  # different versions of pandas raise different exceptions
            nanoseconds = pd.to_datetime(
                int(value),
                utc=True,
            ).value

        return nanoseconds


def optional_datetime_to_unix_nanoseconds(
    value: pd.Timestamp | datetime | date | str | int | None,
) -> int | None:
    """
    Return a valid UNIX nanosecond timestamp from the given value (if not
    None).

    Parameters
    ----------
    value : pd.Timestamp, datetime, date, str, or int
        The value to parse.

    Returns
    -------
    int | None

    """
    if value is None:
        return None
    return datetime_to_unix_nanoseconds(value)


def convert_to_date(value: str) -> date | None:
    """
    Convert the given `value` to a date (or None).

    Parameters
    ----------
    value : str
        The date string value to convert.

    Returns
    -------
    datetime.date or `None`
        The corresponding `date` object if the conversion succeeds,
        or `None` if the input value cannot be converted.

    """
    # Calling `.date()` on a NaT value will retain the NaT value
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    return timestamp.date() if pd.notna(timestamp) else None


def convert_to_datetime(value: str) -> pd.Timestamp | None:
    """
    Convert the given `value` to a pandas Timestamp (or None).

    Parameters
    ----------
    value : str
        The datetime string value to convert.

    Returns
    -------
    pandas.Timestamp or None
        The corresponding `Timestamp` object if the conversion succeeds,
        or `None` if the input value cannot be converted.

    """
    return pd.to_datetime(value, utc=True, errors="coerce")


def convert_date_columns(df: pd.DataFrame, columns: list[str]) -> None:
    """
    Convert the specified columns in a DataFrame to date objects.

    The function modifies the input DataFrame in place.

    Parameters
    ----------
    df : pandas.DataFrame
        The pandas DataFrame to modify.
    columns : List[str]
        The column names to convert.

    """
    for column in columns:
        if column not in df:
            continue
        df[column] = df[column].apply(convert_to_date)


def convert_datetime_columns(df: pd.DataFrame, columns: list[str]) -> None:
    """
    Convert the specified columns in a DataFrame to pandas Timestamp objects.

    Parameters
    ----------
    df : pandas.DataFrame
        The pandas DataFrame to modify.
    columns : List[str]
        The column names to convert.

    """
    for column in columns:
        if column not in df:
            continue
        df[column] = df[column].apply(convert_to_datetime)


def convert_jsonl_to_df(data: bytes, compressed: bool) -> pd.DataFrame:
    """
    Convert the given JSON lines bytes `data` to a pandas DataFrame.

    Parameters
    ----------
    data : bytes
        The JSON lines data as bytes to be converted.
    compressed : bool
        If the content is zstd compressed.

    Returns
    -------
    pandas.DataFrame

    """
    if compressed:
        decompressor = zstandard.ZstdDecompressor()
        reader: IO[bytes] = decompressor.stream_reader(data)
    else:
        reader = BytesIO(data)

    return pd.read_json(TextIOWrapper(reader), lines=True)
