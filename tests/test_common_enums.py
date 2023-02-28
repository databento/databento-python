"""Unit tests for databento.common.enums"""
from enum import Enum, Flag
from itertools import combinations
from typing import Type

import pytest
from databento.common.enums import (
    Compression,
    Dataset,
    Delivery,
    Encoding,
    FeedMode,
    HistoricalGateway,
    LiveGateway,
    Packaging,
    RecordFlags,
    RollRule,
    Schema,
    SplitDuration,
    StringyMixin,
    SType,
    SymbologyResolution,
)


DATABENTO_ENUMS = (
    Compression,
    Dataset,
    Encoding,
    FeedMode,
    HistoricalGateway,
    LiveGateway,
    Packaging,
    Delivery,
    RecordFlags,
    RollRule,
    Schema,
    SplitDuration,
    SType,
    SymbologyResolution,
)


@pytest.mark.parametrize(
    "enum_type",
    (pytest.param(enum) for enum in DATABENTO_ENUMS if issubclass(enum, int)),
)
def test_int_enum_string_coercion(enum_type: Type[Enum]) -> None:
    """
    Test the int coercion for integer enumerations.
    See: databento.common.enums.coercible
    """
    for enum in enum_type:
        assert enum == enum_type(str(enum.value))
        with pytest.raises(ValueError):
            enum_type("NaN")  # sanity


@pytest.mark.parametrize(
    "enum_type",
    (pytest.param(enum) for enum in DATABENTO_ENUMS if issubclass(enum, str)),
)
def test_str_enum_case_coercion(enum_type: Type[Enum]) -> None:
    """
    Test the lowercase name coercion for string enumerations.
    See: databento.common.enums.coercible
    """
    for enum in enum_type:
        assert enum == enum_type(enum.value.lower())
        assert enum == enum_type(enum.value.upper())
        with pytest.raises(ValueError):
            enum_type("foo")  # sanity


@pytest.mark.parametrize(
    "enum_type",
    DATABENTO_ENUMS,
)
def test_enum_name_coercion(enum_type: Type[Enum]) -> None:
    """
    Test that enums can be coerced from the member names.
    This includes case and dash conversion to underscores.
    See: databento.common.enums.coercible
    """
    for enum in enum_type:
        assert enum == enum_type(enum.name)
        assert enum == enum_type(enum.name.replace("_", "-"))
        assert enum == enum_type(enum.name.lower())
        assert enum == enum_type(enum.name.upper())
        with pytest.raises(ValueError):
            enum_type("bar")  # sanity


@pytest.mark.parametrize(
    "enum_type",
    (pytest.param(enum) for enum in DATABENTO_ENUMS),
)
def test_enum_none_not_coercible(enum_type: Type[Enum]) -> None:
    """
    Test that None type is not coercible and raises a TypeError.
    See: databento.common.enum.coercible
    """
    with pytest.raises(TypeError):
        enum_type(None)


@pytest.mark.parametrize(
    "enum_type",
    (pytest.param(enum) for enum in DATABENTO_ENUMS if issubclass(enum, int)),
)
def test_int_enum_stringy_mixin(enum_type: Type[Enum]) -> None:
    """
    Test the StringyMixin for integer enumerations.
    See: databento.common.enum.StringyMixin
    """
    if not issubclass(enum_type, StringyMixin):
        pytest.skip(f"{type(enum_type)} is not a subclass of StringyMixin")
    for enum in enum_type:
        assert str(enum) == enum.name.lower()


@pytest.mark.parametrize(
    "enum_type",
    (pytest.param(enum) for enum in DATABENTO_ENUMS if issubclass(enum, str)),
)
def test_str_enum_stringy_mixin(enum_type: Type[Enum]) -> None:
    """
    Test the StringyMixin for string enumerations.
    See: databento.common.enum.StringyMixin
    """
    if not issubclass(enum_type, StringyMixin):
        pytest.skip(f"{type(enum_type)} is not a subclass of StringyMixin")
    for enum in enum_type:
        assert str(enum) == enum.value


@pytest.mark.parametrize(
    "enum_type",
    (pytest.param(enum) for enum in DATABENTO_ENUMS if issubclass(enum, Flag)),
)
def test_int_flags_stringy_mixin(enum_type: Type[Flag]) -> None:
    """
    Test that combinations of int flags are displayed properly.
    """
    for value in map(sum, combinations(enum_type, 2)):
        record_flags = enum_type(value)
        assert str(record_flags) == ", ".join(
            f.name.lower() for f in enum_type if f in record_flags
        )
