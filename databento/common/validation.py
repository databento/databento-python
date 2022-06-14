from enum import Enum, EnumMeta
from typing import List, Optional, Union


def validate_enum(
    value: Union[Enum, str],
    enum: EnumMeta,
    param: str,
    lower: bool = True,
) -> Union[Enum, str]:
    """
    Validate whether the given value is either the correct Enum type, or a valid
    value of that enum.

    Parameters
    ----------
    value : Enum or str
        The value to validate.
    enum : EnumMeta
        The valid enum type.
    param : str
        The name of the parameter being validated (for any error message).
    lower : str, default True
        If the str value should be lower-cased.

    Returns
    -------
    Union[Enum, str]
        The valid enum or lower-cased string.

    Raises
    ------
    ValueError
        If value is not valid for the given enum.
    TypeError
        If value is an Enum and not of type enum_type.

    """
    if isinstance(value, str):
        if lower:
            value = value.lower()
        valid: List[str] = [x.value for x in enum]  # type: ignore
        if value not in valid:
            raise ValueError(
                f"The `{param}` was not a valid value of {enum}, was '{value}'. "
                f"Use any of {valid}.",
            )
    elif not isinstance(value, enum):
        raise TypeError(
            f"The `{param}` value was not of type {enum}, was {type(value)}.",
        )

    return value


def validate_maybe_enum(
    value: Optional[Union[Enum, str]],
    enum: EnumMeta,
    param: str,
    lower: bool = True,
) -> Optional[Union[Enum, str]]:
    """
    Validate whether the given value is either the correct Enum type, a valid
    value of that enum, or None.

    Parameters
    ----------
    value : Enum or str, optional
        The value to validate.
    enum : EnumMeta
        The valid enum type.
    param : str
        The name of the parameter being validated (for any error message).
    lower : str, default True
        If the str value should be lower-cased.

    Returns
    -------
    Union[Enum, str] or None
        The valid enum or lower-cased string.

    Raises
    ------
    ValueError
        If value is not valid for the given enum.
    TypeError
        If value is an Enum and not of type enum_type.

    """
    if value is None:
        return None
    return validate_enum(value, enum, param, lower)
