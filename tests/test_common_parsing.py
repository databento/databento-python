import datetime as dt
from typing import Any, List, Optional, Type, Union

import numpy as np
import pandas as pd
import pytest
from databento.common.enums import SType
from databento.common.parsing import (
    optional_date_to_string,
    optional_datetime_to_string,
    optional_symbols_list_to_string,
    optional_values_list_to_string,
)


# Set the type to `Any` to disable mypy type checking. Used to test if functions
# will raise a `TypeError` when passed an incorrectly-typed argument.
INCORRECT_TYPE: Any = type


class TestParsing:
    def test_maybe_values_list_to_string_given_invalid_input_raises_type_error(
        self,
    ) -> None:
        # Arrange, Act, Assert
        with pytest.raises(TypeError):
            optional_values_list_to_string(INCORRECT_TYPE)

    @pytest.mark.parametrize(
        "values, expected",
        [
            [None, None],
            ["ABC,DEF", "abc,def"],
            [" ABC, DEF ", "abc, def"],
            [[" ABC", "DEF "], "abc,def"],
            [np.asarray([" ABC", "DEF "]), "abc,def"],
            [("1", "2"), "1,2"],
        ],
    )
    def test_maybe_values_list_to_string_given_valid_inputs_returns_expected(
        self,
        values: Optional[List[str]],
        expected: str,
    ) -> None:
        # Arrange, Act
        result: Optional[str] = optional_values_list_to_string(values)

        # Assert
        assert result == expected

    def test_maybe_symbols_list_to_string_given_invalid_input_raises_type_error(
        self,
    ) -> None:
        # Arrange, Act, Assert
        with pytest.raises(TypeError):
            optional_symbols_list_to_string(INCORRECT_TYPE, SType.RAW_SYMBOL)

    @pytest.mark.parametrize(
        "symbols, expected",
        [
            pytest.param(None, "ALL_SYMBOLS"),
            pytest.param("ES.fut", "ES.FUT"),
            pytest.param("ES,CL", "ES,CL"),
            pytest.param("ES,CL,", "ES,CL"),
            pytest.param("es,cl,", "ES,CL"),
            pytest.param(["ES", "CL"], "ES,CL"),
            pytest.param(["es", "cl"], "ES,CL"),
            pytest.param(["ES.N.0", "CL.n.0"], "ES.n.0,CL.n.0"),
            pytest.param(["ES.N.0", ["ES,cl"]], "ES.n.0,ES,CL"),
            pytest.param(["ES.N.0", "ES,cl"], "ES.n.0,ES,CL"),
            pytest.param("", ValueError),
            pytest.param([""], ValueError),
            pytest.param(["ES.N.0", ""], ValueError),
            pytest.param(["ES.N.0", "CL..0"], ValueError),
            pytest.param(123458, ValueError),
        ],
    )
    def test_optional_symbols_list_to_string_given_valid_inputs_returns_expected(
        self,
        symbols: Optional[List[str]],
        expected: Union[str, Type[Exception]],
    ) -> None:
        # Arrange, Act, Assert
        if isinstance(expected, str):
            assert optional_symbols_list_to_string(symbols, SType.SMART) == expected
        else:
            with pytest.raises(expected):
                optional_symbols_list_to_string(symbols, SType.SMART)

    @pytest.mark.parametrize(
        "symbols, stype, expected",
        [
            pytest.param(12345, SType.INSTRUMENT_ID, "12345"),
            pytest.param("67890", SType.INSTRUMENT_ID, "67890"),
            pytest.param([12345, "  67890"], SType.INSTRUMENT_ID, "12345,67890"),
            pytest.param([12345, [67890, 66]], SType.INSTRUMENT_ID, "12345,67890,66"),
            pytest.param([12345, "67890,66"], SType.INSTRUMENT_ID, "12345,67890,66"),
            pytest.param("", SType.INSTRUMENT_ID, ValueError),
            pytest.param([12345, ""], SType.INSTRUMENT_ID, ValueError),
            pytest.param([12345, [""]], SType.INSTRUMENT_ID, ValueError),
            pytest.param(12345, SType.RAW_SYMBOL, ValueError),
            pytest.param(12345, SType.SMART, ValueError),
        ],
    )
    def test_optional_symbols_list_to_string_int(
        self,
        symbols: Optional[Union[List[int], int]],
        stype: SType,
        expected: Union[str, Type[Exception]],
    ) -> None:
        """
        Test that integers are allowed for SType.INSTRUMENT_ID.
        If integers are given for a different SType we expect
        a ValueError.
        """
        if isinstance(expected, str):
            assert optional_symbols_list_to_string(symbols, stype) == expected
        else:
            with pytest.raises(expected):
                optional_symbols_list_to_string(symbols, stype)

    @pytest.mark.parametrize(
        "symbols, stype, expected",
        [
            pytest.param("NVDA", SType.RAW_SYMBOL, "NVDA"),
            pytest.param(" nvda  ", SType.RAW_SYMBOL, "NVDA"),
            pytest.param("NVDA,amd", SType.RAW_SYMBOL, "NVDA,AMD"),
            pytest.param("NVDA,amd,NOC,", SType.RAW_SYMBOL, "NVDA,AMD,NOC"),
            pytest.param("NVDA,  amd,NOC, ", SType.RAW_SYMBOL, "NVDA,AMD,NOC"),
            pytest.param(["NVDA", ["NOC", "AMD"]], SType.RAW_SYMBOL, "NVDA,NOC,AMD"),
            pytest.param(["NVDA", "NOC,AMD"], SType.RAW_SYMBOL, "NVDA,NOC,AMD"),
            pytest.param("", SType.RAW_SYMBOL, ValueError),
            pytest.param([""], SType.RAW_SYMBOL, ValueError),
            pytest.param(["NVDA", ""], SType.RAW_SYMBOL, ValueError),
            pytest.param(["NVDA", [""]], SType.RAW_SYMBOL, ValueError),
        ],
    )
    def test_optional_symbols_list_to_string_native(
        self,
        symbols: Optional[Union[List[int], int]],
        stype: SType,
        expected: Union[str, Type[Exception]],
    ) -> None:
        """
        Test that str are allowed for SType.RAW_SYMBOL.
        """
        if isinstance(expected, str):
            assert optional_symbols_list_to_string(symbols, stype) == expected
        else:
            with pytest.raises(expected):
                optional_symbols_list_to_string(symbols, stype)

    @pytest.mark.parametrize(
        "value, expected",
        [
            [None, None],
            ["2020-11-07", "2020-11-07"],
            [dt.date(2020, 12, 28), "2020-12-28"],
        ],
    )
    def test_maybe_date_to_string_give_valid_values_returns_expected_results(
        self,
        value: Union[dt.date, str],
        expected: str,
    ) -> None:
        # Arrange, Act
        result: Optional[str] = optional_date_to_string(value)

        # Assert
        assert result == expected

    @pytest.mark.parametrize(
        "value, expected",
        [
            [None, None],
            [1604782791000000000, "1604782791000000000"],
            ["2020-11-07T20:59:51", "2020-11-07T20:59:51"],
            [dt.date(2020, 12, 28), "2020-12-28T00:00:00"],
            [pd.to_datetime("2020-12-28T23:12:01.123"), "2020-12-28T23:12:01.123000"],
        ],
    )
    def test_maybe_datetime_to_string_give_valid_values_returns_expected_results(
        self,
        value: Union[pd.Timestamp, dt.date, str, int],
        expected: str,
    ) -> None:
        # Arrange, Act
        result: Optional[str] = optional_datetime_to_string(value)

        # Assert
        assert result == expected
