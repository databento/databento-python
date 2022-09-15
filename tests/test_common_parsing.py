from datetime import date
from typing import Any

import numpy as np
import pandas as pd
import pytest
from databento.common.enums import Dataset, Flags
from databento.common.parsing import (
    enum_or_str_lowercase,
    maybe_date_to_string,
    maybe_datetime_to_string,
    maybe_enum_or_str_lowercase,
    maybe_symbols_list_to_string,
    maybe_values_list_to_string,
    parse_flags,
)


# Set the type to `Any` to disable mypy type checking. Used to test if functions
# will raise a `TypeError` when passed an incorrectly-typed argument.
INCORRECT_TYPE: Any = type


class TestParsing:
    def test_enum_or_str_lowercase_given_none_raises_type_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(TypeError):
            enum_or_str_lowercase(None, "param")

    def test_enum_or_str_lowercase_given_incorrect_type_raises_type_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(TypeError):
            enum_or_str_lowercase(INCORRECT_TYPE, "param")

    @pytest.mark.parametrize(
        "value, expected",
        [
            ["abc", "abc"],
            ["ABC", "abc"],
            [Dataset.GLBX_MDP3, "glbx.mdp3"],
        ],
    )
    def test_enum_or_str_lowercase_returns_expected_outputs(
        self, value, expected
    ) -> None:
        # Arrange, Act, Assert
        assert enum_or_str_lowercase(value, "param") == expected

    def test_maybe_enum_or_str_lowercase_given_incorrect_types_raises_error(
        self,
    ) -> None:
        # Arrange, Act, Assert
        with pytest.raises(TypeError):
            maybe_enum_or_str_lowercase(INCORRECT_TYPE, "param")

    @pytest.mark.parametrize(
        "value, expected",
        [
            [None, None],
            ["abc", "abc"],
            ["ABC", "abc"],
            [Dataset.GLBX_MDP3, "glbx.mdp3"],
        ],
    )
    def test_maybe_enum_or_str_lowercase_returns_expected_outputs(
        self, value, expected
    ):
        # Arrange, Act, Assert
        assert maybe_enum_or_str_lowercase(value, "param") == expected

    def test_maybe_values_list_to_string_given_invalid_input_raises_type_error(
        self,
    ) -> None:
        # Arrange, Act, Assert
        with pytest.raises(TypeError):
            maybe_values_list_to_string(INCORRECT_TYPE)

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
        values,
        expected,
    ) -> None:
        # Arrange, Act
        result = maybe_values_list_to_string(values)

        # Assert
        assert result == expected

    def test_maybe_symbols_list_to_string_given_invalid_input_raises_type_error(
        self,
    ) -> None:
        # Arrange, Act, Assert
        with pytest.raises(TypeError):
            maybe_symbols_list_to_string(INCORRECT_TYPE)

    @pytest.mark.parametrize(
        "symbols, expected",
        [
            [None, None],
            ["ES,CL", "ES,CL"],
            ["ES,CL,", "ES,CL"],
            ["es,cl,", "ES,CL"],
            [["ES", "CL"], "ES,CL"],
            [["es", "cl"], "ES,CL"],
        ],
    )
    def test_maybe_symbols_list_to_string_given_valid_inputs_returns_expected(
        self,
        symbols,
        expected,
    ) -> None:
        # Arrange, Act
        result = maybe_symbols_list_to_string(symbols)

        # Assert
        assert result == expected

    @pytest.mark.parametrize(
        "value, expected",
        [
            [None, None],
            [1604782791000000000, "2020-11-07"],
            ["2020-11-07T20:59:51", "2020-11-07"],
            [date(2020, 12, 28), "2020-12-28"],
            [pd.Timestamp("2020-12-28T23:12:01.123"), "2020-12-28"],
        ],
    )
    def test_maybe_date_to_string_give_valid_values_returns_expected_results(
        self,
        value,
        expected,
    ) -> None:
        # Arrange, Act
        result = maybe_date_to_string(value)

        # Assert
        assert result == expected

    @pytest.mark.parametrize(
        "value, expected",
        [
            [None, None],
            [1604782791000000000, "2020-11-07T20:59:51"],
            ["2020-11-07T20:59:51", "2020-11-07T20:59:51"],
            [date(2020, 12, 28), "2020-12-28T00:00:00"],
            [pd.Timestamp("2020-12-28T23:12:01.123"), "2020-12-28T23:12:01.123000"],
        ],
    )
    def test_maybe_datetime_to_string_give_valid_values_returns_expected_results(
        self,
        value,
        expected,
    ) -> None:
        # Arrange, Act
        result = maybe_datetime_to_string(value)

        # Assert
        assert result == expected

    @pytest.mark.parametrize(
        "value, expected",
        [
            [Flags.F_LAST.value, ["F_LAST"]],
            [Flags.F_DUPID.value | Flags.F_LAST.value, ["F_LAST", "F_DUPID"]],
            [128, ["F_LAST"]],
            [129, ["F_LAST", "F_RESERVED0"]],
        ],
    )
    def test_parse_flags_given_valid_values_returns_expected_results(
        self,
        value,
        expected,
    ) -> None:
        # Arrange, Act
        result = parse_flags(value)

        # Assert
        assert result == expected
