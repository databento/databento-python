from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest
from databento_dbn import SType

from databento.common import zstd
from databento.common.constants import ALL_SYMBOLS
from databento.common.parsing import convert_jsonl_to_df
from databento.common.parsing import optional_date_to_string
from databento.common.parsing import optional_datetime_to_string
from databento.common.parsing import optional_datetime_to_unix_nanoseconds
from databento.common.parsing import optional_symbols_list_to_list
from databento.common.parsing import optional_values_list_to_string
from databento.common.parsing import symbols_list_to_list
from tests import TESTS_ROOT


# Set the type to `Any` to disable mypy type checking. Used to test if functions
# will raise a `TypeError` when passed an incorrectly-typed argument.
INCORRECT_TYPE: Any = type


def test_maybe_values_list_to_string_given_invalid_input_raises_type_error() -> None:
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
    values: list[str] | None,
    expected: str,
) -> None:
    # Arrange, Act
    result: str | None = optional_values_list_to_string(values)

    # Assert
    assert result == expected


@pytest.mark.parametrize(
    "stype, symbols, expected",
    [
        pytest.param(SType.RAW_SYMBOL, None, TypeError),
        pytest.param(SType.PARENT, ["ES", None], TypeError),
        pytest.param(SType.PARENT, ["ES", [None]], TypeError),
        pytest.param(SType.PARENT, "ES.fut", ["ES.FUT"]),
        pytest.param(SType.PARENT, "ES,CL", ["ES", "CL"]),
        pytest.param(SType.PARENT, "ES,CL,", ["ES", "CL"]),
        pytest.param(SType.PARENT, "es,cl,", ["ES", "CL"]),
        pytest.param(SType.PARENT, ["ES", "CL"], ["ES", "CL"]),
        pytest.param(SType.PARENT, ["es", "cl"], ["ES", "CL"]),
        pytest.param(SType.CONTINUOUS, ["ES.N.0", "CL.n.0"], ["ES.n.0", "CL.n.0"]),
        pytest.param(SType.CONTINUOUS, ["ES.N.0", ["ES,cl"]], ["ES.n.0", "ES", "CL"]),
        pytest.param(SType.CONTINUOUS, ["ES.N.0", "ES,cl"], ["ES.n.0", "ES", "CL"]),
        pytest.param(SType.CONTINUOUS, "", ValueError),
        pytest.param(SType.CONTINUOUS, [""], ValueError),
        pytest.param(SType.CONTINUOUS, ["ES.N.0", ""], ValueError),
        pytest.param(SType.CONTINUOUS, ["ES.N.0", "CL..0"], ValueError),
        pytest.param(SType.PARENT, 123458, ValueError),
    ],
)
def test_symbols_list_to_list_given_valid_inputs_returns_expected(
    stype: SType,
    symbols: list[str] | None,
    expected: list[object] | type[Exception],
) -> None:
    # Arrange, Act, Assert
    if isinstance(expected, list):
        assert symbols_list_to_list(symbols, stype) == expected
    else:
        with pytest.raises(expected):
            symbols_list_to_list(symbols, stype)


@pytest.mark.parametrize(
    "symbols, stype, expected",
    [
        pytest.param([12345, None], SType.INSTRUMENT_ID, TypeError),
        pytest.param([12345, [None]], SType.INSTRUMENT_ID, TypeError),
        pytest.param(12345, SType.INSTRUMENT_ID, ["12345"]),
        pytest.param("67890", SType.INSTRUMENT_ID, ["67890"]),
        pytest.param([12345, "  67890"], SType.INSTRUMENT_ID, ["12345", "67890"]),
        pytest.param(
            [12345, [67890, 66]],
            SType.INSTRUMENT_ID,
            ["12345", "67890", "66"],
        ),
        pytest.param(
            [12345, "67890,66"],
            SType.INSTRUMENT_ID,
            ["12345", "67890", "66"],
        ),
        pytest.param("", SType.INSTRUMENT_ID, ValueError),
        pytest.param([12345, ""], SType.INSTRUMENT_ID, ValueError),
        pytest.param([12345, [""]], SType.INSTRUMENT_ID, ValueError),
        pytest.param(12345, SType.RAW_SYMBOL, ValueError),
        pytest.param(12345, SType.PARENT, ValueError),
        pytest.param(12345, SType.CONTINUOUS, ValueError),
    ],
)
def test_symbols_list_to_list_int(
    symbols: list[int] | int | None,
    stype: SType,
    expected: list[object] | type[Exception],
) -> None:
    """
    Test that integers are allowed for SType.INSTRUMENT_ID.

    If integers are given for a different SType we expect a ValueError.

    """
    # Arrange, Act, Assert
    if isinstance(expected, list):
        assert symbols_list_to_list(symbols, stype) == expected
    else:
        with pytest.raises(expected):
            symbols_list_to_list(symbols, stype)


@pytest.mark.parametrize(
    "symbols, stype, expected",
    [
        pytest.param(None, SType.INSTRUMENT_ID, TypeError),
        pytest.param([np.byte(120), None], SType.INSTRUMENT_ID, TypeError),
        pytest.param([np.byte(120), [None]], SType.INSTRUMENT_ID, TypeError),
        pytest.param(np.byte(120), SType.INSTRUMENT_ID, ["120"]),
        pytest.param(np.short(32_000), SType.INSTRUMENT_ID, ["32000"]),
        pytest.param(
            [np.intc(12345), np.intc(67890)],
            SType.INSTRUMENT_ID,
            ["12345", "67890"],
        ),
        pytest.param(
            [np.int_(12345), np.longlong(67890)],
            SType.INSTRUMENT_ID,
            ["12345", "67890"],
        ),
    ],
)
def test_symbols_list_to_list_numpy(
    symbols: list[int] | int | None,
    stype: SType,
    expected: list[object] | type[Exception],
) -> None:
    """
    Test that weird numpy types are allowed for SType.INSTRUMENT_ID.

    If integers are given for a different SType we expect a ValueError.

    """
    # Arrange, Act, Assert
    if isinstance(expected, list):
        assert symbols_list_to_list(symbols, stype) == expected
    else:
        with pytest.raises(expected):
            symbols_list_to_list(symbols, stype)


@pytest.mark.parametrize(
    "symbols, stype, expected",
    [
        pytest.param(None, SType.RAW_SYMBOL, TypeError),
        pytest.param(["NVDA", None], SType.RAW_SYMBOL, TypeError),
        pytest.param(["NVDA", [None]], SType.RAW_SYMBOL, TypeError),
        pytest.param("NVDA", SType.RAW_SYMBOL, ["NVDA"]),
        pytest.param(" nvda  ", SType.RAW_SYMBOL, ["NVDA"]),
        pytest.param("NVDA,amd", SType.RAW_SYMBOL, ["NVDA", "AMD"]),
        pytest.param("NVDA,amd,NOC,", SType.RAW_SYMBOL, ["NVDA", "AMD", "NOC"]),
        pytest.param("NVDA,  amd,NOC, ", SType.RAW_SYMBOL, ["NVDA", "AMD", "NOC"]),
        pytest.param(
            ["NVDA", ["NOC", "AMD"]],
            SType.RAW_SYMBOL,
            ["NVDA", "NOC", "AMD"],
        ),
        pytest.param(["NVDA", "NOC,AMD"], SType.RAW_SYMBOL, ["NVDA", "NOC", "AMD"]),
        pytest.param("", SType.RAW_SYMBOL, ValueError),
        pytest.param([""], SType.RAW_SYMBOL, ValueError),
        pytest.param(["NVDA", ""], SType.RAW_SYMBOL, ValueError),
        pytest.param(["NVDA", [""]], SType.RAW_SYMBOL, ValueError),
    ],
)
def test_symbols_list_to_list_raw_symbol(
    symbols: list[int] | int | None,
    stype: SType,
    expected: list[object] | type[Exception],
) -> None:
    """
    Test that str are allowed for SType.RAW_SYMBOL.
    """
    # Arrange, Act, Assert
    if isinstance(expected, list):
        assert symbols_list_to_list(symbols, stype) == expected
    else:
        with pytest.raises(expected):
            symbols_list_to_list(symbols, stype)


@pytest.mark.parametrize(
    "symbols, stype, expected",
    [
        pytest.param(None, SType.RAW_SYMBOL, [ALL_SYMBOLS]),
        pytest.param(["NVDA", None], SType.RAW_SYMBOL, TypeError),
        pytest.param([12345, None], SType.INSTRUMENT_ID, TypeError),
        pytest.param("NVDA", SType.RAW_SYMBOL, ["NVDA"]),
        pytest.param(["NVDA", "TSLA"], SType.RAW_SYMBOL, ["NVDA", "TSLA"]),
        pytest.param(12345, SType.INSTRUMENT_ID, ["12345"]),
        pytest.param([12345, "67890"], SType.INSTRUMENT_ID, ["12345", "67890"]),
    ],
)
def test_optional_symbols_list_to_list_raw_symbol(
    symbols: list[int | str] | int | str | None,
    stype: SType,
    expected: list[object] | type[Exception],
) -> None:
    """
    Test an optional symbols list converts a value of `None` to `[ALL_SYMBOLS]`
    and handles other symbols lists.
    """
    # Arrange, Act, Assert
    if isinstance(expected, list):
        assert optional_symbols_list_to_list(symbols, stype) == expected
    else:
        with pytest.raises(expected):
            optional_symbols_list_to_list(symbols, stype)


@pytest.mark.parametrize(
    "value, expected",
    [
        [None, None],
        ["2020-11-07", "2020-11-07"],
        [dt.date(2020, 12, 28), "2020-12-28"],
    ],
)
def test_maybe_date_to_string_give_valid_values_returns_expected_results(
    value: dt.date | str,
    expected: str,
) -> None:
    # Arrange, Act
    result: str | None = optional_date_to_string(value)

    # Assert
    assert result == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        [None, None],
        [1604782791000000000, "1604782791000000000"],
        ["2020-11-07T20:59:51", "2020-11-07T20:59:51"],
        [dt.date(2020, 12, 28), "2020-12-28"],
        [dt.datetime(2020, 12, 28), "2020-12-28T00:00:00"],
        [dt.datetime(2020, 12, 28, 23, 12, 1), "2020-12-28T23:12:01"],
        [
            dt.datetime(2020, 12, 28, 23, 12, 1, tzinfo=ZoneInfo("America/New_York")),
            "2020-12-28T23:12:01-05:00",
        ],
        [dt.datetime(2020, 12, 28, 23, 12, 1, 123000), "2020-12-28T23:12:01.123000"],
        [pd.to_datetime("2020-12-28T23:12:01.123"), "2020-12-28T23:12:01.123000"],
        [pd.to_datetime("2020-12-28T23:12:01.123456789"), "2020-12-28T23:12:01.123456789"],
    ],
)
def test_maybe_datetime_to_string_give_valid_values_returns_expected_results(
    value: pd.Timestamp | dt.datetime | dt.date | str | int,
    expected: str,
) -> None:
    # Arrange, Act
    result: str | None = optional_datetime_to_string(value)

    # Assert
    assert result == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(1680736543000000000, 1680736543000000000, id="int"),
        pytest.param("1680736543000000000", 1680736543000000000, id="str-int"),
        pytest.param(dt.date(2023, 4, 5), 1680652800000000000, id="date"),
        pytest.param(
            dt.datetime(2023, 4, 5, 23, 15, 43, tzinfo=dt.UTC),
            1680736543000000000,
            id="datetime",
        ),
        pytest.param(
            pd.to_datetime("2023-04-05T00:00:00"),
            1680652800000000000,
            id="timestamp",
        ),
        pytest.param(
            "2023-04-05T23:15:43+00:00",
            1680736543000000000,
            id="iso timestamp",
        ),
    ],
)
def test_datetime_to_unix_nanoseconds(
    value: pd.Timestamp | dt.datetime | dt.date | str | int,
    expected: int,
) -> None:
    """
    Test that various inputs for times convert to unix nanoseconds.
    """
    # Arrange, Act, Assert
    assert optional_datetime_to_unix_nanoseconds(value) == expected


def test_convert_jsonl_to_df_keeps_numeric_strings() -> None:
    # Arrange
    data = (
        b'{"cik":"0000320193","us_code":"037833100","local_code":"00005"}\n'
        b'{"cik":null,"us_code":"594918104","local_code":"0700"}\n'
    )

    # Act
    df = convert_jsonl_to_df(data, compressed=False)

    # Assert
    assert df["cik"].iloc[0] == "0000320193"
    assert pd.isna(df["cik"].iloc[1])
    assert df["us_code"].tolist() == ["037833100", "594918104"]
    assert df["local_code"].tolist() == ["00005", "0700"]


def test_convert_jsonl_to_df_does_not_parse_time_columns() -> None:
    # Arrange
    data = b'{"expiry_time":"17:00:00"}\n{"expiry_time":null}\n'

    # Act
    df = convert_jsonl_to_df(data, compressed=False)

    # Assert
    assert df["expiry_time"].iloc[0] == "17:00:00"
    assert pd.isna(df["expiry_time"].iloc[1])


def test_convert_jsonl_to_df_keeps_json_number_types() -> None:
    # Arrange
    data = b'{"lot_size":100,"ratio_old":1000.0}\n{"lot_size":1,"ratio_old":1.0}\n'

    # Act
    df = convert_jsonl_to_df(data, compressed=False)

    # Assert
    assert df["lot_size"].dtype == np.int64
    assert df["ratio_old"].dtype == np.float64


@pytest.mark.parametrize(
    "compressed",
    [
        False,
        True,
    ],
)
def test_convert_jsonl_to_df_given_empty_data_returns_empty_frame(
    compressed: bool,
) -> None:
    # Arrange
    data = zstd.compress(b"") if compressed else b""

    # Act
    df = convert_jsonl_to_df(data, compressed=compressed)

    # Assert
    assert df.empty


@pytest.mark.parametrize(
    "file_name, column, expected",
    [
        ("test_data.security-master.jsonl", "us_code", "037833100"),
        ("test_data.security-master.jsonl", "cik", "0000320193"),
        ("test_data.security-master.jsonl", "sic", "3571"),
        ("test_data.security-master.jsonl", "naics", "334111"),
        ("test_data.adjustment-factors.jsonl", "us_code", "594918104"),
        ("test_data.corporate-actions.jsonl", "option_id", "1"),
        ("test_data.corporate-actions-pit.jsonl", "outturn_us_code", "228891305"),
    ],
)
def test_convert_jsonl_to_df_reference_identifiers_stay_strings(
    file_name: str,
    column: str,
    expected: str,
) -> None:
    # Arrange
    data = (Path(TESTS_ROOT) / "data" / "REFERENCE" / file_name).read_bytes()

    # Act
    df = convert_jsonl_to_df(zstd.compress(data), compressed=True)

    # Assert
    assert df[column].iloc[0] == expected
