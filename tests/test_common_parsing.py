import datetime as dt
from typing import Any, List, Optional, Union

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
            optional_symbols_list_to_string(INCORRECT_TYPE, SType.NATIVE)

    @pytest.mark.parametrize(
        "symbols, expected",
        [
            [None, "ALL_SYMBOLS"],
            ["ES.fut", "ES.FUT"],
            ["ES,CL", "ES,CL"],
            ["ES,CL,", "ES,CL"],
            ["es,cl,", "ES,CL"],
            [["ES", "CL"], "ES,CL"],
            [["es", "cl"], "ES,CL"],
            [["ES.N.0", "CL.n.0"], "ES.n.0,CL.n.0"],
        ],
    )
    def test_maybe_symbols_list_to_string_given_valid_inputs_returns_expected(
        self,
        symbols: Optional[List[str]],
        expected: str,
    ) -> None:
        # Arrange, Act
        result: str = optional_symbols_list_to_string(symbols, SType.SMART)

        # Assert
        assert result == expected

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
            [1604782791000000000, "2020-11-07T20:59:51"],
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
