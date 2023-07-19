from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any

import pytest
from databento.common.validation import validate_enum
from databento.common.validation import validate_file_write_path
from databento.common.validation import validate_gateway
from databento.common.validation import validate_maybe_enum
from databento.common.validation import validate_path
from databento.common.validation import validate_semantic_string
from databento.common.validation import validate_smart_symbol
from databento_dbn import Encoding


@pytest.mark.parametrize(
    "value",
    [
        [None, 0],
    ],
)
def test_validate_path_given_wrong_types_raises_type_error(
    value: Any,
) -> None:
    # Arrange, Act, Assert
    with pytest.raises(TypeError):
        validate_path(value, "param")

def test_validate_file_write_path(
    tmp_path: Path,
) -> None:
    # Arrange, Act, Assert
    test_file = tmp_path / "test.file"
    validate_file_write_path(test_file, "param")

def test_validate_file_write_path_is_dir(
    tmp_path: Path,
) -> None:
    # Arrange, Act, Assert
    with pytest.raises(IsADirectoryError):
        validate_file_write_path(tmp_path, "param")

def test_validate_file_write_path_exists(
    tmp_path: Path,
) -> None:
    # Arrange, Act, Assert
    test_file = tmp_path / "test.file"
    test_file.touch()
    with pytest.raises(FileExistsError):
        validate_file_write_path(test_file, "param")

@pytest.mark.parametrize(
    "value, enum",
    [
        [None, Encoding],
    ],
)
def test_validate_enum_given_wrong_types_raises_type_error(
    value: Any,
    enum: type[Enum],
) -> None:
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        validate_enum(value, enum, "param")

def test_validate_enum_given_invalid_value_raises_value_error() -> None:
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        validate_enum("invalid", Encoding, "encoding")

@pytest.mark.parametrize(
    "value, enum, expected",
    [
        ["dbn", Encoding, "dbn"],
        ["DBN", Encoding, "dbn"],
        [Encoding.DBN, Encoding, "dbn"],
    ],
)
def test_validate_enum_given_valid_value_returns_expected_output(
    value: str | Enum,
    enum: type[Enum],
    expected: str | Enum,
) -> None:
    # Arrange, Act, Assert
    assert validate_enum(value, enum, "param") == expected

def test_validate_maybe_enum_give_none_returns_none() -> None:
    # Arrange, Act, Assert
    assert validate_maybe_enum(None, Encoding, "encoding") is None

@pytest.mark.parametrize(
    "url, expected",
    [
        pytest.param("databento.com", "https://databento.com"),
        pytest.param("hist.databento.com", "https://hist.databento.com"),
        pytest.param("http://databento.com", "https://databento.com"),
        pytest.param("http://hist.databento.com", "https://hist.databento.com"),
        pytest.param("//", ValueError),
        pytest.param("", ValueError),
    ],
)
def test_validate_gateway(
    url: str,
    expected: str | type[Exception],
) -> None:
    """
    Tests several correct and malformed URLs.
    """
    if isinstance(expected, str):
        assert validate_gateway(url) == expected
    else:
        with pytest.raises(expected):
            validate_gateway(url)

@pytest.mark.parametrize(
    "symbol, expected",
    [
        pytest.param("ES", "ES"),
        pytest.param("es", "ES"),
        pytest.param("ES.FUT", "ES.FUT"),
        pytest.param("es.opt", "ES.OPT"),
        pytest.param("ES.C.0", "ES.c.0"),
        pytest.param("es.c.5", "ES.c.5"),
        pytest.param(".v.2", ValueError),
        pytest.param("es..9", ValueError),
        pytest.param("es.n.", ValueError),
        pytest.param("es.c.5.0", ValueError),
        pytest.param("", ValueError),
    ],
)
def test_validate_smart_symbol(
    symbol: str,
    expected: str | type[Exception],
) -> None:
    """
    Test several correct smart symbols and invalid syntax.
    """
    if isinstance(expected, str):
        assert validate_smart_symbol(symbol) == expected
    else:
        with pytest.raises(expected):
            validate_smart_symbol(symbol)


@pytest.mark.parametrize(
    "value,expected",
    [
        pytest.param("nick", "nick"),
        pytest.param("", ValueError, id="empty"),
        pytest.param(" ", ValueError, id="whitespace"),
        pytest.param("foo\x00", ValueError, id="unprintable"),
    ],
)
def test_validate_semantic_string(
    value: str,
    expected: str | type[Exception],
) -> None:
    """
    Test that validate_semantic_string rejects string which are:
        - empty
        - whitespace
        - contain unprintable characters
    """
    if isinstance(expected, str):
        assert validate_semantic_string(value, "unittest") == expected
    else:
        with pytest.raises(expected):
            assert validate_semantic_string(value, "")
