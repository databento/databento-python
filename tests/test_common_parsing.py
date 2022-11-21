import datetime as dt
from enum import Enum
from typing import Any, List, Optional, Union

import numpy as np
import pandas as pd
import pytest
from databento.common.enums import Dataset, Flags
from databento.common.parsing import (
    enum_or_str_lowercase,
    enum_or_str_uppercase,
    maybe_date_to_string,
    maybe_datetime_to_string,
    maybe_enum_or_str_lowercase,
    maybe_enum_or_str_uppercase,
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
        self,
        value: Union[Enum, str],
        expected: str,
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
        self,
        value: Optional[Union[Enum, str]],
        expected: Optional[str],
    ) -> None:
        # Arrange, Act, Assert
        assert maybe_enum_or_str_lowercase(value, "param") == expected

    def test_enum_or_str_uppercase_given_none_raises_type_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(TypeError):
            enum_or_str_uppercase(None, "param")  # noqa (passing None for test)

    def test_enum_or_str_uppercase_given_incorrect_type_raises_type_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(TypeError):
            enum_or_str_uppercase(INCORRECT_TYPE, "param")

    @pytest.mark.parametrize(
        "value, expected",
        [
            ["abc", "ABC"],
            ["ABC", "ABC"],
            [Dataset.GLBX_MDP3, "GLBX.MDP3"],
        ],
    )
    def test_enum_or_str_uppercase_returns_expected_outputs(
        self,
        value: Union[Enum, str],
        expected: str,
    ) -> None:
        # Arrange, Act, Assert
        assert enum_or_str_uppercase(value, "param") == expected

    def test_maybe_enum_or_str_uppercase_given_incorrect_types_raises_error(
        self,
    ) -> None:
        # Arrange, Act, Assert
        with pytest.raises(TypeError):
            maybe_enum_or_str_lowercase(INCORRECT_TYPE, "param")

    @pytest.mark.parametrize(
        "value, expected",
        [
            [None, None],
            ["abc", "ABC"],
            ["ABC", "ABC"],
            [Dataset.GLBX_MDP3, "GLBX.MDP3"],
        ],
    )
    def test_maybe_enum_or_str_uppercase_returns_expected_outputs(
        self,
        value: Optional[Union[Enum, str]],
        expected: Optional[str],
    ) -> None:
        # Arrange, Act, Assert
        assert maybe_enum_or_str_uppercase(value, "param") == expected

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
        values: Optional[List[str]],
        expected: str,
    ) -> None:
        # Arrange, Act
        result: Optional[str] = maybe_values_list_to_string(values)

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
        symbols: Optional[List[str]],
        expected: str,
    ) -> None:
        # Arrange, Act
        result: Optional[str] = maybe_symbols_list_to_string(symbols)

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
        result: Optional[str] = maybe_date_to_string(value)

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
        result: Optional[str] = maybe_datetime_to_string(value)

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
        value: int,
        expected: List[str],
    ) -> None:
        # Arrange, Act
        result: List[str] = parse_flags(value)

        # Assert
        assert result == expected
