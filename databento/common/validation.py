from __future__ import annotations

import os
from enum import Enum
from os import PathLike
from pathlib import Path
from typing import TypeVar
from urllib.parse import urlsplit
from urllib.parse import urlunsplit


E = TypeVar("E", bound=Enum)


def validate_path(value: PathLike[str] | str, param: str) -> Path:
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

    Raises
    ------
    TypeError
        If value is not a valid path.

    """
    try:
        return Path(value)
    except TypeError:
        raise TypeError(
            f"The `{param}` was not a valid path type. "
            "Use any of [str, bytes, os.PathLike].",
        ) from None


def validate_file_write_path(value: PathLike[str] | str, param: str) -> Path:
    """
    Validate whether the given value is a valid path to a writable file.

    Parameters
    ----------
    value: PathLike or str
        The value to validate.
    param : str
        The name of the parameter being validated (for any error message).

    Returns
    -------
    Path
        A valid path to a writable file.

    Raises
    ------
    IsADirectoryError
        If path is a directory.
    FileExistsError
        If path exists.
    PermissionError
        If path is not writable.

    """
    path_valid = validate_path(value, param)
    if not os.access(path_valid.parent, os.W_OK):
        raise PermissionError(f"The file `{value}` is not writable.")
    if path_valid.is_dir():
        raise IsADirectoryError(f"The `{param}` was not a path to a file.")
    if path_valid.is_file():
        raise FileExistsError(f"The file `{value}` already exists.")
    return path_valid


def validate_enum(
    value: object,
    enum: type[E],
    param: str,
) -> E:
    """
    Validate whether the given value is either the correct Enum type, or a
    valid value of that enum.

    Parameters
    ----------
    value : Enum or str, optional
        The value to validate.
    enum : type[Enum]
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
    except ValueError:
        if hasattr(enum, "variants"):
            valid = list(map(str, enum.variants()))  # type: ignore [attr-defined]
        else:
            valid = list(map(str, enum))

        raise ValueError(
            f"The `{param}` was not a valid value of {enum.__name__}"
            f", was '{value}'. Use any of {valid}.",
        ) from None


def validate_maybe_enum(
    value: E | str | None,
    enum: type[E],
    param: str,
) -> E | None:
    """
    Validate whether the given value is either the correct Enum type, a valid
    value of that enum, or None.

    Parameters
    ----------
    value : Enum or str, optional
        The value to validate.
    enum : type[Enum]
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


def validate_semantic_string(value: str, param: str) -> str:
    """
    Validate whether a string contains a semantic value.

    A string is considered absent of meaning if:
        - It is empty.
        - It contains only whitespace.
        - It contains unprintable characters.

    Parameters
    ----------
    value: str
        The string to validate.
    param : str
        The name of the parameter being validated (for any error message).

    Raises
    ------
    ValueError
        If the string is not meaningful.

    """
    if not value:
        raise ValueError(f"The `{param}` cannot be an empty string.")
    if str.isspace(value):
        raise ValueError(f"The `{param}` cannot contain only whitepsace.")
    if not str.isprintable(value):
        raise ValueError(f"The `{param}` cannot contain unprintable characters.")
    return value


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
