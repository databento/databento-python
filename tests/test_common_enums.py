"""
Unit tests for databento.common.enums.
"""

from enum import Enum
from enum import Flag
from itertools import combinations
from typing import Final

import pytest
from databento.common.enums import Delivery
from databento.common.enums import FeedMode
from databento.common.enums import HistoricalGateway
from databento.common.enums import Packaging
from databento.common.enums import RecordFlags
from databento.common.enums import RollRule
from databento.common.enums import SplitDuration
from databento.common.enums import StringyMixin
from databento.common.enums import SymbologyResolution
from databento.common.publishers import Dataset
from databento_dbn import Compression
from databento_dbn import DBNError
from databento_dbn import Encoding
from databento_dbn import Schema
from databento_dbn import SType


NATIVE_ENUMS: Final = (
    Dataset,
    FeedMode,
    HistoricalGateway,
    Packaging,
    Delivery,
    RecordFlags,
    RollRule,
    SplitDuration,
    SymbologyResolution,
)

DBN_ENUMS: Final = (
    Compression,
    Encoding,
    Schema,
    SType,
)


@pytest.mark.parametrize(
    "enum_type",
    (pytest.param(enum) for enum in NATIVE_ENUMS if issubclass(enum, int)),
)
def test_int_enum_string_coercion(enum_type: type[Enum]) -> None:
    """
    Test the int coercion for integer enumerations.

    See: databento.common.enums.coercible

    """
    # Arrange, Act, Assert
    for variant in enum_type:
        assert variant == enum_type(str(variant.value))
        with pytest.raises(ValueError):
            enum_type("NaN")  # sanity


@pytest.mark.parametrize(
    "enum_type",
    (pytest.param(enum) for enum in NATIVE_ENUMS if issubclass(enum, str)),
)
def test_str_enum_case_coercion(enum_type: type[Enum]) -> None:
    """
    Test the lowercase name coercion for string enumerations.

    See: databento.common.enums.coercible

    """
    # Arrange, Act, Assert
    for enum in enum_type:
        assert enum == enum_type(enum.value.lower())
        assert enum == enum_type(enum.value.upper())
        with pytest.raises(ValueError):
            enum_type("foo")  # sanity


@pytest.mark.parametrize(
    "enum_type",
    NATIVE_ENUMS,
)
def test_enum_name_coercion(enum_type: type[Enum]) -> None:
    """
    Test that enums can be coerced from the member names.

    This includes case and dash conversion to underscores.
    See: databento.common.enums.coercible

    """
    # Arrange, Act
    if enum_type in (Compression, Encoding, Schema, SType):
        enum_it = iter(enum_type.variants())  # type: ignore [attr-defined]
    else:
        enum_it = iter(enum_type)

    # Assert
    for enum in enum_it:
        assert enum == enum_type(enum.name)
        assert enum == enum_type(enum.name.replace("_", "-"))
        assert enum == enum_type(enum.name.lower())
        assert enum == enum_type(enum.name.upper())
        with pytest.raises(ValueError):
            enum_type("bar")  # sanity


@pytest.mark.parametrize(
    "enum_type",
    DBN_ENUMS,
)
def test_dbn_enum_name_coercion(enum_type: type[Enum]) -> None:
    """
    Test that DBN enums can be coerced from the member names.

    This includes case and dash conversion to underscores.

    """
    # Arrange, Act
    if enum_type in (Compression, Encoding, Schema, SType):
        enum_it = iter(enum_type.variants())  # type: ignore [attr-defined]
    else:
        enum_it = iter(enum_type)

    # Assert
    for enum in enum_it:
        assert enum == enum_type(enum.name)
        assert enum == enum_type(enum.name.replace("_", "-"))
        assert enum == enum_type(enum.name.lower())
        assert enum == enum_type(enum.name.upper())
        with pytest.raises(DBNError):
            enum_type("bar")  # sanity


@pytest.mark.parametrize(
    "enum_type",
    (pytest.param(enum) for enum in NATIVE_ENUMS),
)
def test_enum_none_not_coercible(enum_type: type[Enum]) -> None:
    """
    Test that None type is not coercible and raises a TypeError.

    See: databento.common.enum.coercible

    """
    # Arrange, Act
    if enum_type == Compression:
        enum_type(None)
    else:
        # Assert
        with pytest.raises(ValueError):
            enum_type(None)


@pytest.mark.parametrize(
    "enum_type",
    (pytest.param(enum) for enum in DBN_ENUMS),
)
def test_dbn_enum_none_not_coercible(enum_type: type[Enum]) -> None:
    """
    Test that None type is not coercible and raises a TypeError.
    """
    # Arrange, Act
    if enum_type == Compression:
        enum_type(None)
    else:
        # Assert
        with pytest.raises(DBNError):
            enum_type(None)


@pytest.mark.parametrize(
    "enum_type",
    (pytest.param(enum) for enum in NATIVE_ENUMS if issubclass(enum, int)),
)
def test_int_enum_stringy_mixin(enum_type: type[Enum]) -> None:
    """
    Test the StringyMixin for integer enumerations.

    See: databento.common.enum.StringyMixin

    """
    # Arrange, Act
    if not issubclass(enum_type, StringyMixin):
        pytest.skip(f"{type(enum_type)} is not a subclass of StringyMixin")

    # Assert
    for enum in enum_type:
        assert str(enum) == enum.name.lower()


@pytest.mark.parametrize(
    "enum_type",
    (pytest.param(enum) for enum in NATIVE_ENUMS if issubclass(enum, str)),
)
def test_str_enum_stringy_mixin(enum_type: type[Enum]) -> None:
    """
    Test the StringyMixin for string enumerations.

    See: databento.common.enum.StringyMixin

    """
    # Arrange, Act
    if not issubclass(enum_type, StringyMixin):
        pytest.skip(f"{type(enum_type)} is not a subclass of StringyMixin")

    # Assert
    for enum in enum_type:
        assert str(enum) == enum.value


@pytest.mark.parametrize(
    "enum_type",
    (pytest.param(enum) for enum in NATIVE_ENUMS if issubclass(enum, Flag)),
)
def test_int_flags_stringy_mixin(enum_type: type[Flag]) -> None:
    """
    Test that combinations of int flags are displayed properly.
    """
    # Arrange, Act
    for value in map(sum, combinations(enum_type, 2)):  # type: ignore [arg-type]
        record_flags = enum_type(value)

        # Assert
        assert str(record_flags) == ", ".join(
            f.name.lower() for f in enum_type if f in record_flags
        )
