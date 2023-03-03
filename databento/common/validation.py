from __future__ import annotations

from enum import Enum
from os import PathLike
from pathlib import Path
from typing import Optional, Type, TypeVar, Union
from urllib.parse import urlsplit, urlunsplit


E = TypeVar("E", bound=Enum)


def validate_path(value: Union[PathLike[str], str], param: str) -> Path:
    """
    Validate whether the given value is a valid path.

    Parameters
    ----------
    value: PathLike or str
        The value to validate.
    param : str
        The name of the parameter being validated (for any error message).

    Returns
    -------
    Path
        A valid path.

    """
    try:
        return Path(value)
    except TypeError as e:
        raise TypeError(
            f"The `{param}` was not a valid path type. "
            "Use any of [str, bytes, os.PathLike].",
        ) from e


def validate_enum(
    value: object,
    enum: Type[E],
    param: str,
) -> E:
    """
    Validate whether the given value is either the correct Enum type, or a valid
    value of that enum.

    Parameters
    ----------
    value : Enum or str, optional
        The value to validate.
    enum : Type[Enum]
        The valid enum type.
    param : str
        The name of the parameter being validated (for any error message).

    Returns
    -------
    Enum
        A valid member of Enum.

    Raises
    ------
    ValueError
        If value is not valid for the given enum.

    """
    try:
        return enum(value)
    except ValueError as e:
        valid = list(map(str, enum))
        raise ValueError(
            f"The `{param}` was not a valid value of {enum}, was '{value}'. "
            f"Use any of {valid}.",
        ) from e


def validate_maybe_enum(
    value: Optional[Union[E, str]],
    enum: Type[E],
    param: str,
) -> Optional[E]:
    """
    Validate whether the given value is either the correct Enum type, a valid
    value of that enum, or None.

    Parameters
    ----------
    value : Enum or str, optional
        The value to validate.
    enum : Type[Enum]
        The valid enum type.
    param : str
        The name of the parameter being validated (for any error message).

    Returns
    -------
    Enum or None
        A valid member of Enum or None.

    Raises
    ------
    ValueError
        If value is not valid for the given enum.

    """
    if value is None:
        return None
    return validate_enum(value, enum, param)


def validate_gateway(
    url: str,
) -> str:
    """
    Validate that the given value is a valid gateway URL.

    Parameters
    ----------
    url : str
        The URL to validate

    Returns
    -------
    str
        The gateway URL if it is valid.

    Raises
    ------
    ValueError
        If the URL is invalid.

    """
    url_chunks = urlsplit(url)

    if not any([url_chunks.netloc, url_chunks.path]):
        raise ValueError(f"`{url}` is not a valid URL")

    if url_chunks.netloc:
        return urlunsplit(
            components=("https", url_chunks.netloc, url_chunks.path, "", ""),
        )
    return urlunsplit(components=("https", url_chunks.path, "", "", ""))


def validate_smart_symbol(symbol: str) -> str:
    """
    Validate whether symbol has a valid smart symbol format.

    Parameters
    ----------
    symbol: str
        The smart symbol to validate.

    Raises
    ------
    ValueError
        If symbol is not a valid smart symbol format.

    Notes
    -----
    Valid smart symbols can have the following formats:
        [ROOT]
        [ROOT].[ASSET_CLASS]
        [ROOT].[ROLL_RULE].[RANK]

    e.x
        ES
        ES.OPT
        ES.c.0

    """
    tokens = symbol.upper().split(".")

    if len(tokens) > 3 or not all(tokens):
        raise ValueError(
            f"value `{symbol}` is not a valid smart symbol format; ",
            "valid formats are [ROOT], [ROOT].[ASSET_CLASS], and "
            "[ROOT].[ROLL_RULE].[RANK].",
        )

    if len(tokens) == 3:
        # [ROOT].[ROLL_RULE].[RANK]
        tokens[1] = tokens[1].lower()  # api expects lower case

    return ".".join(tokens)
