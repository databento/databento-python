from enum import Enum
from typing import Any, Type, Union

import pytest
from databento.common.enums import Encoding
from databento.common.validation import (
    validate_enum,
    validate_gateway,
    validate_maybe_enum,
    validate_path,
    validate_smart_symbol,
)


class TestValidation:
    @pytest.mark.parametrize(
        "value",
        [
            [None, 0],
        ],
    )
    def test_validate_path_given_wrong_types_raises_type_error(
        self,
        value: Any,
    ) -> None:
        # Arrange, Act, Assert
        with pytest.raises(TypeError):
            validate_path(value, "param")

    @pytest.mark.parametrize(
        "value, enum",
        [
            [None, Encoding],
        ],
    )
    def test_validate_enum_given_wrong_types_raises_type_error(
        self,
        value: Any,
        enum: Type[Enum],
    ) -> None:
        # Arrange, Act, Assert
        with pytest.raises(TypeError):
            validate_enum(value, enum, "param")

    def test_validate_enum_given_invalid_value_raises_value_error(self) -> None:
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
        self,
        value: Union[str, Enum],
        enum: Type[Enum],
        expected: Union[str, Enum],
    ) -> None:
        # Arrange, Act, Assert
        assert validate_enum(value, enum, "param") == expected

    def test_validate_maybe_enum_give_none_returns_none(self) -> None:
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
        self,
        url: str,
        expected: Union[str, Type[Exception]],
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
        self,
        symbol: str,
        expected: Union[str, Type[Exception]],
    ) -> None:
        """ """
        if isinstance(expected, str):
            assert validate_smart_symbol(symbol) == expected
        else:
            with pytest.raises(expected):
                validate_smart_symbol(symbol)
