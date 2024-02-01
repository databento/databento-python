from __future__ import annotations

import json
import pathlib
from collections.abc import Iterable
from typing import Callable, NamedTuple

import pandas as pd
import pytest
from databento.common.dbnstore import DBNStore
from databento.common.publishers import Dataset
from databento.common.symbology import InstrumentMap
from databento.common.symbology import MappingInterval
from databento_dbn import UNDEF_TIMESTAMP
from databento_dbn import Metadata
from databento_dbn import Schema
from databento_dbn import SType
from databento_dbn import SymbolMappingMsg
from databento_dbn import SymbolMappingMsgV1


class SymbolMapping(NamedTuple):
    """
    A mapping from raw symbol to a collection of MappingIntervals.

    Attributes
    ----------
    raw_symbol: str
        The raw symbol.
    intervals: list[MappingInterval]
        The MappingIntervals for the `raw_symbol`.

    """

    raw_symbol: str
    intervals: list[MappingInterval]


@pytest.fixture(name="instrument_map")
def fixture_instrument_map() -> InstrumentMap:
    """
    Fixture for a simple InstrumentMap.

    Returns
    -------
    InstrumentMap

    """
    return InstrumentMap()


@pytest.fixture(name="start_date")
def fixture_start_date() -> pd.Timestamp:
    """
    Fixture for a start date. This is one day behind the date provided by the
    `start_date` fixture.

    Returns
    -------
    dt.date

    See Also
    --------
    fixture_end_date

    """
    return pd.Timestamp(
        year=2021,
        month=5,
        day=20,
        tz="utc",
    )


@pytest.fixture(name="end_date")
def fixture_end_date() -> pd.Timestamp:
    """
    Fixture for an end date. This is one day head of the date provided by the
    `start_date` fixture.

    Returns
    -------
    dt.date

    See Also
    --------
    fixture_start_date

    """
    return pd.Timestamp(
        year=2021,
        month=5,
        day=21,
        tz="utc",
    )


def create_symbology_response(
    result: dict[str, list[dict[str, str | int]]] = {},
    symbols: Iterable[str] = [],
    stype_in: SType = SType.RAW_SYMBOL,
    stype_out: SType = SType.INSTRUMENT_ID,
    start_date: pd.Timestamp = pd.Timestamp.utcnow(),
    end_date: pd.Timestamp = pd.Timestamp.utcnow() + pd.Timedelta(days=1),
    partial: Iterable[str] = [],
    not_found: Iterable[str] = [],
    message: str = "",
    status: int = 0,
) -> dict[str, object]:
    """
    Create a mock symbology.resolve response as a dictionary.

    Returns
    -------
    dict[str, object]

    """
    return {
        "result": result,
        "symbols": list(symbols),
        "stype_in": str(stype_in),
        "stype_out": str(stype_out),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "partial": list(partial),
        "not_found": list(not_found),
        "message": list(message),
        "status": status,
    }


def create_symbol_mapping_message(
    publisher_id: int = 0,
    instrument_id: int = 0,
    ts_event: int = pd.Timestamp.utcnow().value,
    stype_in: SType = SType.RAW_SYMBOL,
    stype_in_symbol: str | int = "",
    stype_out: SType = SType.INSTRUMENT_ID,
    stype_out_symbol: str | int = "",
    start_ts: pd.Timestamp = pd.Timestamp.utcnow(),
    end_ts: pd.Timestamp = pd.Timestamp.utcnow(),
) -> SymbolMappingMsg:
    """
    Create a mock SymbolMappingMsg.

    Returns
    -------
    SymbolMappingMsg

    """
    return SymbolMappingMsg(
        publisher_id=publisher_id,
        instrument_id=instrument_id,
        ts_event=ts_event,
        stype_in=stype_in,
        stype_in_symbol=str(stype_in_symbol),
        stype_out=stype_out,
        stype_out_symbol=str(stype_out_symbol),
        start_ts=start_ts.value,
        end_ts=end_ts.value,
    )


def create_metadata(
    mappings: Iterable[SymbolMapping],
    dataset: str = "UNIT.TEST",
    start: int = UNDEF_TIMESTAMP,
    end: int = UNDEF_TIMESTAMP,
    stype_in: SType | None = SType.RAW_SYMBOL,
    stype_out: SType = SType.INSTRUMENT_ID,
    schema: Schema = Schema.TRADES,
    limit: int | None = None,
    ts_out: bool = False,
) -> Metadata:
    return Metadata(
        dataset=dataset,
        start=start,
        stype_out=stype_out,
        symbols=[m.raw_symbol for m in mappings],
        partial=[],
        not_found=[],
        mappings=mappings,
        schema=schema,
        stype_in=stype_in,
        end=end,
        limit=limit,
        ts_out=ts_out,
    )


def test_instrument_map(
    instrument_map: InstrumentMap,
) -> None:
    """
    Test the creation of an InstrumentMap.
    """
    # Arrange, Act, Assert
    assert instrument_map._data == {}


@pytest.mark.parametrize(
    "stype_in",
    [
        SType.RAW_SYMBOL,
        None,
    ],
)
def test_instrument_map_insert_metadata(
    instrument_map: InstrumentMap,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    stype_in: SType | None,
) -> None:
    """
    Test the insertion of DBN Metadata.
    """
    # Arrange
    symbol = "test"
    instrument_id = 1234

    mappings = [
        SymbolMapping(
            raw_symbol=symbol,
            intervals=[
                MappingInterval(
                    start_date=start_date,
                    end_date=end_date,
                    symbol=str(instrument_id),
                ),
            ],
        ),
    ]

    metadata = create_metadata(
        stype_in=stype_in,
        mappings=mappings,
    )

    # Act
    instrument_map.insert_metadata(metadata)

    # Assert
    assert instrument_map.resolve(instrument_id, start_date.date()) == symbol


def test_instrument_map_insert_metadata_multiple_mappings(
    instrument_map: InstrumentMap,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> None:
    """
    Test the insertion of a DBN Metadata with multiple mapping for the same
    instrument_id.
    """
    # Arrange
    symbols = ["test_1", "test_2", "test_3"]
    instrument_id = 1234

    mappings = []
    for offset, symbol in enumerate(symbols):
        mappings.append(
            SymbolMapping(
                raw_symbol=symbol,
                intervals=[
                    MappingInterval(
                        start_date=start_date + pd.Timedelta(days=offset),
                        end_date=end_date + pd.Timedelta(days=offset),
                        symbol=str(instrument_id),
                    ),
                ],
            ),
        )

    metadata = create_metadata(
        mappings=mappings,
    )

    # Act
    instrument_map.insert_metadata(metadata)

    # Assert
    for offset, symbol in enumerate(symbols):
        assert (
            instrument_map.resolve(
                instrument_id,
                (start_date + pd.Timedelta(days=offset)).date(),
            )
            == symbol
        )


def test_instrument_map_insert_metadata_empty_mappings(
    instrument_map: InstrumentMap,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> None:
    """
    Test the insertion of DBN Metadata that contains an empty mapping.
    """
    # Arrange
    mappings = [
        SymbolMapping(
            raw_symbol="empty",
            intervals=[
                MappingInterval(
                    start_date=start_date,
                    end_date=end_date,
                    symbol="",
                ),
            ],
        ),
    ]

    metadata = create_metadata(
        mappings=mappings,
    )

    # Act
    instrument_map.insert_metadata(metadata)

    # Assert
    assert instrument_map._data == {}


@pytest.mark.parametrize(
    "stype_in,symbol_in,stype_out,symbol_out,instrument_id,expected_symbol",
    [
        (SType.RAW_SYMBOL, "test", SType.INSTRUMENT_ID, 1234, 1234, "test"),
        (SType.INSTRUMENT_ID, 1234, SType.RAW_SYMBOL, "test", 1234, "test"),
        (SType.CONTINUOUS, "FOO.c.0", SType.RAW_SYMBOL, "test", 1234, "test"),
        (SType.PARENT, "FOO.OUT", SType.RAW_SYMBOL, "test", 1234, "test"),
    ],
)
def test_instrument_map_insert_symbol_mapping_message_v1(
    instrument_map: InstrumentMap,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    stype_in: SType,
    symbol_in: str | int,
    stype_out: SType,
    symbol_out: str | int,
    instrument_id: int,
    expected_symbol: str,
) -> None:
    """
    Test the insertion of a SymbolMappingMessageV1.

    This is a legacy message.

    """
    # Arrange
    sym_msg = create_symbol_mapping_message(
        instrument_id=instrument_id,
        stype_in=stype_in,
        stype_in_symbol=symbol_in,
        stype_out=stype_out,
        stype_out_symbol=symbol_out,
        start_ts=start_date,
        end_ts=end_date,
    )
    sym_msg_v1 = SymbolMappingMsgV1(
        publisher_id=sym_msg.publisher_id,
        instrument_id=sym_msg.instrument_id,
        ts_event=sym_msg.ts_event,
        stype_in_symbol=sym_msg.stype_in_symbol,
        stype_out_symbol=sym_msg.stype_out_symbol,
        start_ts=sym_msg.start_ts,
        end_ts=sym_msg.end_ts,
    )

    # Act
    instrument_map.insert_symbol_mapping_msg(sym_msg_v1)

    # Assert
    assert instrument_map.resolve(instrument_id, start_date.date()) == expected_symbol


@pytest.mark.parametrize(
    "stype_in,symbol_in,stype_out,symbol_out,instrument_id,expected_symbol",
    [
        (SType.RAW_SYMBOL, "test", SType.INSTRUMENT_ID, 1234, 1234, "test"),
        (SType.INSTRUMENT_ID, 1234, SType.RAW_SYMBOL, "test", 1234, "test"),
        (SType.CONTINUOUS, "FOO.c.0", SType.RAW_SYMBOL, "test", 1234, "test"),
        (SType.PARENT, "FOO.OUT", SType.RAW_SYMBOL, "test", 1234, "test"),
    ],
)
def test_instrument_map_insert_symbol_mapping_message(
    instrument_map: InstrumentMap,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    stype_in: SType,
    symbol_in: str | int,
    stype_out: SType,
    symbol_out: str | int,
    instrument_id: int,
    expected_symbol: str,
) -> None:
    """
    Test the insertion of a SymbolMappingMessage.
    """
    # Arrange
    sym_msg = create_symbol_mapping_message(
        instrument_id=instrument_id,
        stype_in=stype_in,
        stype_in_symbol=symbol_in,
        stype_out=stype_out,
        stype_out_symbol=symbol_out,
        start_ts=start_date,
        end_ts=end_date,
    )

    # Act
    instrument_map.insert_symbol_mapping_msg(sym_msg)

    # Assert
    assert instrument_map.resolve(instrument_id, start_date.date()) == expected_symbol


def test_instrument_map_insert_symbol_mapping_message_multiple_mappings(
    instrument_map: InstrumentMap,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> None:
    """
    Test the insertion of multiple SymbolMappingMsg object for the same
    instrument_id.
    """
    # Arrange
    symbols = ["test_1", "test_2", "test_3"]
    instrument_id = 1234

    # Act
    for offset, symbol in enumerate(symbols):
        sym_mapping_msg = create_symbol_mapping_message(
            instrument_id=instrument_id,
            stype_in_symbol=symbol,
            stype_out_symbol=instrument_id,
            start_ts=start_date + pd.Timedelta(days=offset),
            end_ts=end_date + pd.Timedelta(days=offset),
        )
        instrument_map.insert_symbol_mapping_msg(sym_mapping_msg)

    # Assert
    for offset, symbol in enumerate(symbols):
        assert (
            instrument_map.resolve(
                instrument_id,
                (start_date + pd.Timedelta(days=offset)).date(),
            )
            == symbol
        )


@pytest.mark.parametrize(
    "symbol_in,stype_in,symbol_out,stype_out",
    [
        pytest.param(
            "test_1",
            SType.RAW_SYMBOL,
            1234,
            SType.INSTRUMENT_ID,
            id="normal",
        ),
        pytest.param(
            1234,
            SType.INSTRUMENT_ID,
            "test_1",
            SType.RAW_SYMBOL,
            id="inverted",
        ),
    ],
)
def test_instrument_map_insert_symbology_response(
    instrument_map: InstrumentMap,
    symbol_in: str,
    stype_in: SType,
    symbol_out: str,
    stype_out: SType,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> None:
    """
    Test the insertion of a symbology responses.
    """
    # Arrange
    result = {
        symbol_in: [
            {"d0": start_date.isoformat(), "d1": end_date.isoformat(), "s": symbol_out},
        ],
    }
    sym_resp = create_symbology_response(
        result=result,
        stype_in=stype_in,
        stype_out=stype_out,
    )

    # Act
    instrument_map.insert_json(sym_resp)

    # Assert
    # This is hard coded because it should be invariant under parameterization
    assert instrument_map.resolve(1234, start_date.date()) == "test_1"


def test_instrument_map_insert_symbology_response_multiple_mappings(
    instrument_map: InstrumentMap,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> None:
    """
    Test the insertion of multiple symbology responses for the same
    instrument_id.
    """
    # Arrange, Act
    symbols = ["test_1", "test_2", "test_3"]
    instrument_id = 1234

    for offset, symbol in enumerate(symbols):
        result = {
            symbol: [
                {
                    "d0": (start_date + pd.Timedelta(days=offset)).isoformat(),
                    "d1": (end_date + pd.Timedelta(days=offset)).isoformat(),
                    "s": instrument_id,
                },
            ],
        }
        sym_resp = create_symbology_response(
            result=result,
        )

        instrument_map.insert_json(sym_resp)

    # Assert
    for offset, symbol in enumerate(symbols):
        assert (
            instrument_map.resolve(
                instrument_id,
                (start_date + pd.Timedelta(days=offset)).date(),
            )
            == symbol
        )


def test_instrument_map_insert_symbology_response_empty_mapping(
    instrument_map: InstrumentMap,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> None:
    """
    Test the insertion of an empty symbology mapping.
    """
    # Arrange
    result = {
        "test": [
            {"d0": start_date.isoformat(), "d1": end_date.isoformat(), "s": ""},
        ],
    }
    sym_resp = create_symbology_response(
        result=result,
        stype_in=SType.RAW_SYMBOL,
        stype_out=SType.INSTRUMENT_ID,
    )

    # Act
    instrument_map.insert_json(sym_resp)

    # Assert
    assert instrument_map._data == {}


@pytest.mark.parametrize(
    "symbol_in,stype_in,symbol_out,stype_out,expected_symbol",
    [
        pytest.param(
            "test_1",
            SType.RAW_SYMBOL,
            1234,
            SType.INSTRUMENT_ID,
            "test_1",
            id="normal",
        ),
        pytest.param(
            1234,
            SType.INSTRUMENT_ID,
            "test_1",
            SType.RAW_SYMBOL,
            "test_1",
            id="inverted",
        ),
    ],
)
def test_instrument_map_insert_json_str(
    instrument_map: InstrumentMap,
    symbol_in: str,
    stype_in: SType,
    symbol_out: str,
    stype_out: SType,
    expected_symbol: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> None:
    """
    Test the insertion of a JSON symbology response.
    """
    # Arrange
    result = {
        symbol_in: [
            {"d0": start_date.isoformat(), "d1": end_date.isoformat(), "s": symbol_out},
        ],
    }
    sym_resp = create_symbology_response(
        result=result,
        stype_in=stype_in,
        stype_out=stype_out,
    )

    # Act
    instrument_map.insert_json(json.dumps(sym_resp))

    # Assert
    assert instrument_map.resolve(1234, start_date.date()) == expected_symbol


@pytest.mark.parametrize(
    "symbol_in,stype_in,symbol_out,stype_out,expected_symbol",
    [
        pytest.param(
            "test_1",
            SType.RAW_SYMBOL,
            1234,
            SType.INSTRUMENT_ID,
            "test_1",
            id="normal",
        ),
        pytest.param(
            1234,
            SType.INSTRUMENT_ID,
            "test_1",
            SType.RAW_SYMBOL,
            "test_1",
            id="inverted",
        ),
    ],
)
def test_instrument_map_insert_json_file(
    instrument_map: InstrumentMap,
    symbol_in: str,
    stype_in: SType,
    symbol_out: str,
    stype_out: SType,
    expected_symbol: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    tmp_path: pathlib.Path,
) -> None:
    """
    Test the insertion of a JSON file.
    """
    # Arrange
    result = {
        symbol_in: [
            {"d0": start_date.isoformat(), "d1": end_date.isoformat(), "s": symbol_out},
        ],
    }
    sym_resp = create_symbology_response(
        result=result,
        stype_in=stype_in,
        stype_out=stype_out,
    )

    symboloy_json = tmp_path / "symbology.json"
    with open(symboloy_json, mode="w") as resp_file:
        json.dump(sym_resp, resp_file)

    # Act
    with open(symboloy_json) as resp_file:
        instrument_map.insert_json(resp_file)

    # Assert
    assert instrument_map.resolve(1234, start_date.date()) == expected_symbol


def test_instrument_map_insert_json_str_empty_mapping(
    instrument_map: InstrumentMap,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> None:
    """
    Test the insertion of an JSON symbology mapping.
    """
    # Arrange
    result = {
        "test": [
            {"d0": start_date.isoformat(), "d1": end_date.isoformat(), "s": ""},
        ],
    }
    sym_resp = create_symbology_response(
        result=result,
        stype_in=SType.RAW_SYMBOL,
        stype_out=SType.INSTRUMENT_ID,
    )

    # Act
    instrument_map.insert_json(json.dumps(sym_resp))

    # Assert
    assert instrument_map._data == {}


@pytest.mark.parametrize(
    "symbol_in,stype_in,symbol_out,stype_out",
    [
        pytest.param("test_1", SType.RAW_SYMBOL, "test_1", SType.RAW_SYMBOL),
        pytest.param("ES.FUT", SType.PARENT, "test_2", SType.RAW_SYMBOL),
        pytest.param("CL.c.1", SType.CONTINUOUS, "test_3", SType.RAW_SYMBOL),
    ],
)
def test_instrument_map_insert_symbology_response_invalid_stype(
    instrument_map: InstrumentMap,
    symbol_in: str,
    stype_in: SType,
    symbol_out: str,
    stype_out: SType,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> None:
    """
    Test that a symbology response with no instrument_id mapping raises a
    ValueError.
    """
    # Arrange
    result = {
        symbol_in: [
            {"d0": start_date.isoformat(), "d1": end_date.isoformat(), "s": symbol_out},
        ],
    }
    sym_resp = create_symbology_response(
        result=result,
        stype_in=stype_in,
        stype_out=stype_out,
    )

    # Act, Assert
    with pytest.raises(ValueError):
        instrument_map.insert_json(sym_resp)


def test_instrument_map_insert_symbology_response_invalid_response(
    instrument_map: InstrumentMap,
) -> None:
    """
    Test that an invalid symbology response raises a ValueError.
    """
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        instrument_map.insert_json({"foo": "bar"})


def test_instrument_map_insert_symbology_response_invalid_result_entry(
    instrument_map: InstrumentMap,
    start_date: pd.Timestamp,
) -> None:
    """
    Test that an invalid symbology response entry raises a ValueError.
    """
    # Arrange
    result = {
        "test_1": [{"d0": start_date.isoformat(), "s": 1234}],
    }
    sym_resp = create_symbology_response(
        result=result,
    )

    # Act, Assert
    with pytest.raises(ValueError):
        instrument_map.insert_json(sym_resp)


def test_instrument_map_resolve_with_date(
    instrument_map: InstrumentMap,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> None:
    """
    Test that resolve accepts `datetime.date` objects.
    """
    # Arrange, Act
    symbol = "test_1"
    instrument_id = 1234

    instrument_map._data[instrument_id] = [
        MappingInterval(
            start_date=start_date.date(),
            end_date=end_date.date(),
            symbol=symbol,
        ),
    ]

    # Assert
    assert (
        instrument_map.resolve(
            instrument_id,
            (start_date - pd.Timedelta(days=1)).date(),
        )
        is None
    )
    assert instrument_map.resolve(instrument_id, start_date.date()) == symbol
    assert instrument_map.resolve(instrument_id, end_date.date()) is None


def test_instrument_map_ignore_duplicate(
    instrument_map: InstrumentMap,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> None:
    """
    Test that a duplicate entry is not inserted into an InstrumentMap.
    """
    # Arrange, Act
    symbol = "test_1"
    instrument_id = 1234

    instrument_map._data[instrument_id] = [
        MappingInterval(
            start_date=start_date.date(),
            end_date=end_date.date(),
            symbol=symbol,
        ),
    ]

    # Act, Assert
    assert len(instrument_map._data[instrument_id]) == 1

    msg = create_symbol_mapping_message(
        instrument_id=instrument_id,
        stype_in_symbol=symbol,
        stype_out_symbol=instrument_id,
        start_ts=start_date,
        end_ts=end_date,
    )

    instrument_map.insert_symbol_mapping_msg(msg)

    assert len(instrument_map._data[instrument_id]) == 1


@pytest.mark.parametrize(
    "dataset",
    [
        Dataset.GLBX_MDP3,
        Dataset.XNAS_ITCH,
        Dataset.OPRA_PILLAR,
        Dataset.DBEQ_BASIC,
    ],
)
@pytest.mark.parametrize(
    "schema",
    [pytest.param(s, id=str(s)) for s in Schema.variants()],
)
@pytest.mark.parametrize(
    "pretty_ts",
    [
        True,
        False,
    ],
)
def test_instrument_map_symbols_csv(
    tmp_path: pathlib.Path,
    test_data_path: Callable[[Dataset, Schema], pathlib.Path],
    pretty_ts: bool,
    dataset: Dataset,
    schema: Schema,
) -> None:
    """
    Test that a CSV file without mapped symbols is equivalent to a CSV file
    with mapped symbols after processing with map_symbols_csv.
    """
    # Arrange, Act
    store = DBNStore.from_file(test_data_path(dataset, schema))
    csv_path = tmp_path / f"test_{schema}.csv"
    store.to_csv(
        csv_path,
        pretty_ts=pretty_ts,
        map_symbols=False,
    )

    expected_path = tmp_path / "expected.csv"
    store.to_csv(
        expected_path,
        pretty_ts=pretty_ts,
        map_symbols=True,
    )

    outfile = tmp_path / f"test_{schema}_mapped.csv"
    written_path = store._instrument_map.map_symbols_csv(
        csv_file=csv_path,
        out_file=outfile,
    )

    # Assert
    assert outfile == written_path
    assert outfile.read_text() == expected_path.read_text()


@pytest.mark.parametrize(
    "dataset",
    [
        Dataset.GLBX_MDP3,
        Dataset.XNAS_ITCH,
        Dataset.OPRA_PILLAR,
        Dataset.DBEQ_BASIC,
    ],
)
@pytest.mark.parametrize(
    "schema",
    [pytest.param(s, id=str(s)) for s in Schema.variants()],
)
@pytest.mark.parametrize(
    "pretty_ts",
    [
        True,
        False,
    ],
)
def test_instrument_map_symbols_json(
    tmp_path: pathlib.Path,
    test_data_path: Callable[[Dataset, Schema], pathlib.Path],
    pretty_ts: bool,
    dataset: Dataset,
    schema: Schema,
) -> None:
    """
    Test that a JSON file without mapped symbols is equivalent to a JSON file
    with mapped symbols after processing with map_symbols_json.
    """
    # Arrange, Act
    store = DBNStore.from_file(test_data_path(dataset, schema))
    json_path = tmp_path / f"test_{schema}.json"
    store.to_json(
        json_path,
        pretty_ts=pretty_ts,
        map_symbols=False,
    )

    expected_path = tmp_path / "expected.json"
    store.to_json(
        expected_path,
        pretty_ts=pretty_ts,
        map_symbols=True,
    )

    outfile = tmp_path / f"test_{schema}_mapped.json"
    written_path = store._instrument_map.map_symbols_json(
        json_file=json_path,
        out_file=outfile,
    )

    # Assert
    assert outfile == written_path
    assert outfile.read_text() == expected_path.read_text()
