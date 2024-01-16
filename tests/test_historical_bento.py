from __future__ import annotations

import collections
import datetime as dt
import decimal
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Literal
from unittest.mock import MagicMock

import databento
import databento.common.dbnstore
import numpy as np
import pandas as pd
import pytest
import zstandard
from databento.common.dbnstore import DBNStore
from databento.common.error import BentoError
from databento.common.publishers import Dataset
from databento.common.types import DBNRecord
from databento_dbn import MBOMsg
from databento_dbn import Schema
from databento_dbn import SType


def test_from_file_when_not_exists_raises_expected_exception() -> None:
    # Arrange, Act, Assert
    with pytest.raises(FileNotFoundError):
        DBNStore.from_file("my_data.dbn")


def test_from_file_when_file_empty_raises_expected_exception(
    tmp_path: Path,
) -> None:
    """
    Test that creating a DBNStore from an empty file raises a ValueError.
    """
    # Arrange
    path = tmp_path / "test.dbn"
    path.touch()

    # Act, Assert
    with pytest.raises(ValueError):
        DBNStore.from_file(path)


def test_from_file_when_buffer_corrupted_raises_expected_exception(
    tmp_path: Path,
) -> None:
    """
    Test that creating a DBNStore from an invalid DBN file raises a BentoError.
    """
    # Arrange
    path = tmp_path / "corrupted.dbn"
    path.write_text("this is a test")

    # Act, Assert
    with pytest.raises(BentoError):
        DBNStore.from_file(path)


def test_from_bytes_when_buffer_empty_raises_expected_exception() -> None:
    """
    Test that creating a DBNStore from an empty buffer raises a ValueError.
    """
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        DBNStore.from_bytes(BytesIO())


def test_from_bytes_when_buffer_corrupted_raises_expected_exception() -> None:
    """
    Test that creating a DBNStore from an invalid DBN stream raises a
    BentoError.
    """
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        DBNStore.from_bytes(BytesIO())


def test_sources_metadata_returns_expected_json_as_dict(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    # Arrange, Act
    stub_data = test_data(Dataset.GLBX_MDP3, Schema.MBO)
    dbnstore = DBNStore.from_bytes(data=stub_data)

    # Assert
    assert dbnstore.metadata.version == 1
    assert dbnstore.metadata.dataset == "GLBX.MDP3"
    assert dbnstore.metadata.schema == Schema.MBO
    assert dbnstore.metadata.stype_in == SType.RAW_SYMBOL
    assert dbnstore.metadata.stype_out == SType.INSTRUMENT_ID
    assert dbnstore.metadata.start == 1609113600000000000
    assert dbnstore.metadata.end == 1609200000000000000
    assert dbnstore.metadata.limit == 4
    assert dbnstore.metadata.symbols == ["ESH1"]
    assert dbnstore.metadata.ts_out is False
    assert dbnstore.metadata.partial == []
    assert dbnstore.metadata.not_found == []
    assert dbnstore.metadata.mappings == {
        "ESH1": [
            {
                "start_date": dt.date(2020, 12, 28),
                "end_date": dt.date(2020, 12, 29),
                "symbol": "5482",
            },
        ],
    }


def test_dbnstore_given_initial_nbytes_returns_expected_metadata(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Dataset.GLBX_MDP3, Schema.MBO)

    # Act
    dbnstore = DBNStore.from_bytes(data=stub_data)

    # Assert
    assert dbnstore.nbytes == 209
    assert dbnstore.dataset == "GLBX.MDP3"
    assert dbnstore.schema == Schema.MBO
    assert dbnstore.symbols == ["ESH1"]
    assert dbnstore.stype_in == SType.RAW_SYMBOL
    assert dbnstore.stype_out == SType.INSTRUMENT_ID
    assert dbnstore.start == pd.Timestamp("2020-12-28 00:00:00+0000", tz="UTC")
    assert dbnstore.end == pd.Timestamp("2020-12-29 00:00:00+0000", tz="UTC")
    assert dbnstore.limit == 4
    assert len(dbnstore.to_ndarray()) == 4
    assert dbnstore.mappings == {
        "ESH1": [
            {
                "symbol": "5482",
                "start_date": dt.date(2020, 12, 28),
                "end_date": dt.date(2020, 12, 29),
            },
        ],
    }
    assert dbnstore.symbology == {
        "symbols": ["ESH1"],
        "stype_in": "raw_symbol",
        "stype_out": "instrument_id",
        "start_date": "2020-12-28",
        "end_date": "2020-12-29",
        "not_found": [],
        "partial": [],
        "mappings": {
            "ESH1": [
                {
                    "symbol": "5482",
                    "start_date": dt.date(2020, 12, 28),
                    "end_date": dt.date(2020, 12, 29),
                },
            ],
        },
    }


def test_file_dbnstore_given_valid_path_initialized_expected_data(
    test_data_path: Callable[[Dataset, Schema], Path],
) -> None:
    # Arrange, Act
    path = test_data_path(Dataset.GLBX_MDP3, Schema.MBO)
    dbnstore = DBNStore.from_file(path=path)

    # Assert
    assert dbnstore.dataset == "GLBX.MDP3"
    assert dbnstore.nbytes == 209


def test_to_file_persists_to_disk(
    test_data: Callable[[Dataset, Schema], bytes],
    tmp_path: Path,
) -> None:
    # Arrange
    stub_data = test_data(Dataset.GLBX_MDP3, Schema.MBO)
    dbnstore = DBNStore.from_bytes(data=stub_data)

    # Act
    dbn_path = tmp_path / "my_test.dbn"
    dbnstore.to_file(path=dbn_path)

    # Assert
    assert dbn_path.exists()
    assert dbn_path.stat().st_size == 209


def test_to_ndarray_with_stub_data_returns_expected_array(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Dataset.GLBX_MDP3, Schema.MBO)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    array = data.to_ndarray()

    # Assert
    assert isinstance(array, np.ndarray)
    assert str(array) == (
        "[(14, 160, 1, 5482, 1609099225061045683, 647784248135, 3675750000000, 2, 40, 0, b'A', b'B', 1609113600000000000, 0, 1180)\n"
        " (14, 160, 1, 5482, 1609099225061045683, 647782686353, 3675500000000, 1, 40, 0, b'A', b'B', 1609113600000000000, 0, 1160)\n"
        " (14, 160, 1, 5482, 1609099225061045683, 647782884482, 3675250000000, 1, 40, 0, b'A', b'B', 1609113600000000000, 0, 1166)\n"
        " (14, 160, 1, 5482, 1609099225061045683, 647782912367, 3675000000000, 1, 40, 0, b'A', b'B', 1609113600000000000, 0, 1166)]"
    )


def test_iterator_produces_expected_data(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Dataset.GLBX_MDP3, Schema.MBO)
    data = DBNStore.from_bytes(data=stub_data)

    # Act (consume iterator)
    handler = collections.deque(data)

    # Assert
    assert len(handler) == 4


def test_replay_with_stub_data_record_passes_to_callback(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Dataset.GLBX_MDP3, Schema.MBO)
    data = DBNStore.from_bytes(data=stub_data)

    handler: list[MBOMsg] = []

    # Act
    data.replay(callback=handler.append)
    record: MBOMsg = handler[0]

    # Assert
    assert record.hd.length == 14
    assert record.hd.rtype == 160
    assert record.hd.rtype == 160
    assert record.hd.publisher_id == 1
    assert record.hd.instrument_id == 5482
    assert record.hd.ts_event == 1609099225061045683
    assert record.order_id == 647784248135
    assert record.price == 3675750000000
    assert record.size == 2
    assert record.flags == 40
    assert record.channel_id == 0
    assert record.action == "A"
    assert record.side == "B"
    assert record.ts_recv == 1609113600000000000
    assert record.ts_in_delta == 0
    assert record.sequence == 1180


@pytest.mark.parametrize(
    "schema",
    [
        s
        for s in Schema.variants()
        if s
        not in (
            Schema.OHLCV_1H,
            Schema.OHLCV_1D,
            Schema.DEFINITION,
            Schema.STATISTICS,
        )
    ],
)
def test_to_df_across_schemas_returns_identical_dimension_dfs(
    test_data: Callable[[Dataset, Schema], bytes],
    schema: Schema,
) -> None:
    # Arrange
    stub_data = test_data(Dataset.GLBX_MDP3, schema)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    df = data.to_df()

    # Assert
    assert list(df.columns) == list(df.columns)
    assert len(df) == 4


def test_to_df_with_mbo_data_returns_expected_record(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Dataset.GLBX_MDP3, Schema.MBO)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    df = data.to_df(
        pretty_ts=False,
        price_type="fixed",
        map_symbols=False,
    )

    # Assert
    assert len(df) == 4
    assert df.index.name == "ts_recv"
    assert df.index.values[0] == 1609113600000000000
    assert df.iloc[0]["ts_event"] == 1609099225061045683
    assert df.iloc[0]["rtype"] == 160
    assert df.iloc[0]["publisher_id"] == 1
    assert df.iloc[0]["instrument_id"] == 5482
    assert df.iloc[0]["action"] == "A"
    assert df.iloc[0]["side"] == "B"
    assert df.iloc[0]["price"] == 3675750000000
    assert df.iloc[0]["size"] == 2
    assert df.iloc[0]["order_id"] == 647784248135
    assert df.iloc[0]["flags"] == 40
    assert df.iloc[0]["ts_in_delta"] == 0
    assert df.iloc[0]["sequence"] == 1180


def test_to_df_with_stub_ohlcv_data_returns_expected_record(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Dataset.GLBX_MDP3, Schema.OHLCV_1M)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    df = data.to_df(
        pretty_ts=False,
        price_type="fixed",
        map_symbols=False,
    )

    # Assert
    assert len(df) == 4
    assert df.index.name == "ts_event"
    assert df.index.values[0] == 1609113600000000000
    assert df.iloc[0]["instrument_id"] == 5482
    assert df.iloc[0]["open"] == 3_702_750_000_000
    assert df.iloc[0]["high"] == 3_704_750_000_000
    assert df.iloc[0]["low"] == 3_702_500_000_000
    assert df.iloc[0]["close"] == 3_704_750_000_000
    assert df.iloc[0]["volume"] == 306


def test_to_df_with_pretty_ts_converts_timestamps_as_expected(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Dataset.GLBX_MDP3, Schema.MBO)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    df = data.to_df(pretty_ts=True)

    # Assert
    index0 = df.index[0]
    event0 = df["ts_event"][0]
    assert isinstance(index0, pd.Timestamp)
    assert isinstance(event0, pd.Timestamp)
    assert index0 == pd.Timestamp("2020-12-28 00:00:00.000000000+0000", tz="UTC")
    assert event0 == pd.Timestamp("2020-12-27 20:00:25.061045683+0000", tz="UTC")
    assert len(df) == 4


@pytest.mark.parametrize(
    "schema,columns",
    [
        [Schema.MBO, ["price"]],
        [Schema.TBBO, ["price", "bid_px_00", "ask_px_00"]],
        [Schema.TRADES, ["price"]],
        [Schema.MBP_1, ["price", "bid_px_00", "ask_px_00"]],
        [
            Schema.MBP_10,
            [
                "price",
                "bid_px_00",
                "bid_px_01",
                "bid_px_02",
                "bid_px_03",
                "bid_px_04",
                "bid_px_05",
                "bid_px_06",
                "bid_px_07",
                "bid_px_08",
                "bid_px_09",
                "ask_px_00",
                "ask_px_01",
                "ask_px_02",
                "ask_px_03",
                "ask_px_04",
                "ask_px_05",
                "ask_px_06",
                "ask_px_07",
                "ask_px_08",
                "ask_px_09",
            ],
        ],
    ],
)
@pytest.mark.parametrize(
    "price_type, expected_type",
    [
        ("fixed", np.integer),
        ("decimal", decimal.Decimal),
        ("float", np.floating),
    ],
)
def test_to_df_with_price_type_with_various_schemas_converts_prices_as_expected(
    test_data: Callable[[Dataset, Schema], bytes],
    schema: Schema,
    columns: list[str],
    price_type: Literal["float", "decimal"],
    expected_type: type,
) -> None:
    # Arrange
    stub_data = test_data(Dataset.GLBX_MDP3, schema)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    df = data.to_df(price_type=price_type)

    # Assert
    assert len(df) == 4
    for column in columns:
        assert isinstance(df[column].iloc(0)[1], expected_type)


def test_to_df_with_price_type_handles_null(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Dataset.GLBX_MDP3, Schema.DEFINITION)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    df_plain = data.to_df(price_type="fixed")
    df_pretty = data.to_df(price_type="float")

    # Assert
    assert all(df_plain["strike_price"] == 9223372036854775807)
    assert all(np.isnan(df_pretty["strike_price"]))


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
    [pytest.param(schema, id=str(schema)) for schema in Schema.variants()],
)
@pytest.mark.parametrize(
    "price_type",
    [
        "fixed",
        "float",
    ],
)
@pytest.mark.parametrize(
    "pretty_ts",
    [
        True,
        False,
    ],
)
@pytest.mark.parametrize(
    "map_symbols",
    [
        True,
        False,
    ],
)
def test_to_parquet(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    test_data: Callable[[Dataset, Schema], bytes],
    dataset: Dataset,
    schema: Schema,
    price_type: Literal["fixed", "float"],
    pretty_ts: bool,
    map_symbols: bool,
) -> None:
    # Arrange
    monkeypatch.setattr(databento.common.dbnstore, "PARQUET_CHUNK_SIZE", 1)
    stub_data = test_data(dataset, schema)
    data = DBNStore.from_bytes(data=stub_data)
    parquet_file = tmp_path / "test.parquet"

    # Act
    expected = data.to_df(
        price_type=price_type,
        pretty_ts=pretty_ts,
        map_symbols=map_symbols,
    )
    data.to_parquet(
        parquet_file,
        price_type=price_type,
        pretty_ts=pretty_ts,
        map_symbols=map_symbols,
    )
    actual = pd.read_parquet(parquet_file)

    # Replace None values with np.nan
    actual.fillna(value=np.nan)

    # Assert
    pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize(
    "expected_schema",
    [pytest.param(schema, id=str(schema)) for schema in Schema.variants()],
)
def test_from_file_given_various_paths_returns_expected_metadata(
    test_data_path: Callable[[Dataset, Schema], Path],
    expected_schema: Schema,
) -> None:
    # Arrange
    path = test_data_path(Dataset.GLBX_MDP3, expected_schema)

    # Act
    data = DBNStore.from_file(path=path)

    # Assert
    assert data.schema == expected_schema


def test_from_dbn_alias(
    test_data_path: Callable[[Dataset, Schema], Path],
) -> None:
    # Arrange
    path = test_data_path(Dataset.GLBX_MDP3, Schema.MBO)

    # Act
    data = databento.from_dbn(path=path)

    # Assert
    assert data.schema == Schema.MBO
    assert len(data.to_ndarray()) == 4


def test_mbo_to_csv_writes_expected_file_to_disk(
    test_data_path: Callable[[Dataset, Schema], Path],
    tmp_path: Path,
) -> None:
    # Arrange
    data = DBNStore.from_file(path=test_data_path(Dataset.GLBX_MDP3, Schema.MBO))

    path = tmp_path / "test.my_mbo.csv"

    # Act
    data.to_csv(
        path,
        pretty_ts=False,
        pretty_px=False,
        map_symbols=False,
    )

    # Assert
    written = path.read_text()
    expected = (
        "ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,price,size,channel_id,order_id,flags,ts_in_delta,sequence\n"
        "1609113600000000000,1609099225061045683,160,1,5482,A,B,3675750000000,2,0,647784248135,40,0,1180\n"
        "1609113600000000000,1609099225061045683,160,1,5482,A,B,3675500000000,1,0,647782686353,40,0,1160\n"
        "1609113600000000000,1609099225061045683,160,1,5482,A,B,3675250000000,1,0,647782884482,40,0,1166\n"
        "1609113600000000000,1609099225061045683,160,1,5482,A,B,3675000000000,1,0,647782912367,40,0,1166\n"
    )

    assert written == expected


def test_mbp_1_to_csv_with_no_options_writes_expected_file_to_disk(
    test_data_path: Callable[[Dataset, Schema], Path],
    tmp_path: Path,
) -> None:
    # Arrange
    data = DBNStore.from_file(path=test_data_path(Dataset.GLBX_MDP3, Schema.MBP_1))

    path = tmp_path / "test.my_mbo.csv"

    # Act
    data.to_csv(
        path,
        pretty_ts=False,
        pretty_px=False,
        map_symbols=False,
    )

    # Assert
    written = path.read_text()
    expected = (
        "ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,depth,price,size,flags,ts_in_delta,sequence,bid_px_00,ask_px_00,bid_sz_00,ask_sz_00,bid_ct_00,ask_ct_00\n"
        "1609113600006150193,1609113600005871213,1,1,5482,A,B,0,3702250000000,1,130,26128,145805,3702250000000,3702750000000,19,13,11,13\n"
        "1609113600062687776,1609113600062570311,1,1,5482,A,B,0,3702250000000,1,130,17256,145827,3702250000000,3702750000000,20,13,12,13\n"
        "1609113600076130343,1609113600076022275,1,1,5482,A,A,0,3702750000000,1,130,17470,145852,3702250000000,3702750000000,20,14,12,14\n"
        "1609113600076436915,1609113600076339855,1,1,5482,A,B,0,3702250000000,1,130,17409,145853,3702250000000,3702750000000,21,14,13,14\n"
    )

    assert written == expected


def test_mbp_1_to_csv_with_all_options_writes_expected_file_to_disk(
    test_data_path: Callable[[Dataset, Schema], Path],
    tmp_path: Path,
) -> None:
    # Arrange
    data = DBNStore.from_file(path=test_data_path(Dataset.GLBX_MDP3, Schema.MBP_1))

    path = tmp_path / "test.my_mbo.csv"

    # Act
    data.to_csv(
        path,
        pretty_ts=True,
        pretty_px=True,
        map_symbols=True,
    )

    # Assert
    written = path.read_text()
    expected = (
        "ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,depth,price,size,flags,ts_in_delta,sequence,bid_px_00,ask_px_00,bid_sz_00,ask_sz_00,bid_ct_00,ask_ct_00,symbol\n"
        "2020-12-28T00:00:00.006150193Z,2020-12-28T00:00:00.005871213Z,1,1,5482,A,B,0,3702.250000000,1,130,26128,145805,3702.250000000,3702.750000000,19,13,11,13,ESH1\n"
        "2020-12-28T00:00:00.062687776Z,2020-12-28T00:00:00.062570311Z,1,1,5482,A,B,0,3702.250000000,1,130,17256,145827,3702.250000000,3702.750000000,20,13,12,13,ESH1\n"
        "2020-12-28T00:00:00.076130343Z,2020-12-28T00:00:00.076022275Z,1,1,5482,A,A,0,3702.750000000,1,130,17470,145852,3702.250000000,3702.750000000,20,14,12,14,ESH1\n"
        "2020-12-28T00:00:00.076436915Z,2020-12-28T00:00:00.076339855Z,1,1,5482,A,B,0,3702.250000000,1,130,17409,145853,3702.250000000,3702.750000000,21,14,13,14,ESH1\n"
    )

    assert written == expected


def test_mbo_to_json_with_no_options_writes_expected_file_to_disk(
    test_data_path: Callable[[Dataset, Schema], Path],
    tmp_path: Path,
) -> None:
    # Arrange
    data = DBNStore.from_file(path=test_data_path(Dataset.GLBX_MDP3, Schema.MBO))

    path = tmp_path / "test.my_mbo.json"

    # Act
    data.to_json(
        path,
        pretty_ts=False,
        pretty_px=False,
        map_symbols=False,
    )

    # Assert
    written = path.read_text()
    expected = (
        '{"ts_recv":"1609113600000000000","hd":{"ts_event":"1609099225061045683","rtype":160,"publisher_id":1,"instrument_id":5482},"action":"A","side":"B","price":"3675750000000","size":2,"channel_id":0,"order_id":"647784248135","flags":40,"ts_in_delta":0,"sequence":1180}\n'
        '{"ts_recv":"1609113600000000000","hd":{"ts_event":"1609099225061045683","rtype":160,"publisher_id":1,"instrument_id":5482},"action":"A","side":"B","price":"3675500000000","size":1,"channel_id":0,"order_id":"647782686353","flags":40,"ts_in_delta":0,"sequence":1160}\n'
        '{"ts_recv":"1609113600000000000","hd":{"ts_event":"1609099225061045683","rtype":160,"publisher_id":1,"instrument_id":5482},"action":"A","side":"B","price":"3675250000000","size":1,"channel_id":0,"order_id":"647782884482","flags":40,"ts_in_delta":0,"sequence":1166}\n'
        '{"ts_recv":"1609113600000000000","hd":{"ts_event":"1609099225061045683","rtype":160,"publisher_id":1,"instrument_id":5482},"action":"A","side":"B","price":"3675000000000","size":1,"channel_id":0,"order_id":"647782912367","flags":40,"ts_in_delta":0,"sequence":1166}\n'
    )

    assert written == expected


def test_mbo_to_json_with_all_options_writes_expected_file_to_disk(
    test_data_path: Callable[[Dataset, Schema], Path],
    tmp_path: Path,
) -> None:
    # Arrange
    data = DBNStore.from_file(path=test_data_path(Dataset.GLBX_MDP3, Schema.MBO))

    path = tmp_path / "test.my_mbo.json"

    # Act
    data.to_json(
        path,
        pretty_ts=True,
        pretty_px=True,
        map_symbols=True,
    )

    # Assert
    written = path.read_text()
    expected = (
        '{"ts_recv":"2020-12-28T00:00:00.000000000Z","hd":{"ts_event":"2020-12-27T20:00:25.061045683Z","rtype":160,"publisher_id":1,"instrument_id":5482},"action":"A","side":"B","price":"3675.750000000","size":2,"channel_id":0,"order_id":"647784248135","flags":40,"ts_in_delta":0,"sequence":1180,"symbol":"ESH1"}\n'
        '{"ts_recv":"2020-12-28T00:00:00.000000000Z","hd":{"ts_event":"2020-12-27T20:00:25.061045683Z","rtype":160,"publisher_id":1,"instrument_id":5482},"action":"A","side":"B","price":"3675.500000000","size":1,"channel_id":0,"order_id":"647782686353","flags":40,"ts_in_delta":0,"sequence":1160,"symbol":"ESH1"}\n'
        '{"ts_recv":"2020-12-28T00:00:00.000000000Z","hd":{"ts_event":"2020-12-27T20:00:25.061045683Z","rtype":160,"publisher_id":1,"instrument_id":5482},"action":"A","side":"B","price":"3675.250000000","size":1,"channel_id":0,"order_id":"647782884482","flags":40,"ts_in_delta":0,"sequence":1166,"symbol":"ESH1"}\n'
        '{"ts_recv":"2020-12-28T00:00:00.000000000Z","hd":{"ts_event":"2020-12-27T20:00:25.061045683Z","rtype":160,"publisher_id":1,"instrument_id":5482},"action":"A","side":"B","price":"3675.000000000","size":1,"channel_id":0,"order_id":"647782912367","flags":40,"ts_in_delta":0,"sequence":1166,"symbol":"ESH1"}\n'
    )
    assert written == expected


def test_mbp_1_to_json_with_no_options_writes_expected_file_to_disk(
    test_data_path: Callable[[Dataset, Schema], Path],
    tmp_path: Path,
) -> None:
    # Arrange
    data = DBNStore.from_file(path=test_data_path(Dataset.GLBX_MDP3, Schema.MBP_1))

    path = tmp_path / "test.my_mbo.json"

    # Act
    data.to_json(
        path,
        pretty_ts=False,
        pretty_px=False,
        map_symbols=False,
    )

    # Assert
    written = path.read_text()
    expected = (
        '{"ts_recv":"1609113600006150193","hd":{"ts_event":"1609113600005871213","rtype":1,"publisher_id":1,"instrument_id":5482},"action":"A","side":"B","depth":0,"price":"3702250000000","size":1,"flags":130,"ts_in_delta":26128,"sequence":145805,"levels":[{"bid_px":"3702250000000","ask_px":"3702750000000","bid_sz":19,"ask_sz":13,"bid_ct":11,"ask_ct":13}]}\n'
        '{"ts_recv":"1609113600062687776","hd":{"ts_event":"1609113600062570311","rtype":1,"publisher_id":1,"instrument_id":5482},"action":"A","side":"B","depth":0,"price":"3702250000000","size":1,"flags":130,"ts_in_delta":17256,"sequence":145827,"levels":[{"bid_px":"3702250000000","ask_px":"3702750000000","bid_sz":20,"ask_sz":13,"bid_ct":12,"ask_ct":13}]}\n'
        '{"ts_recv":"1609113600076130343","hd":{"ts_event":"1609113600076022275","rtype":1,"publisher_id":1,"instrument_id":5482},"action":"A","side":"A","depth":0,"price":"3702750000000","size":1,"flags":130,"ts_in_delta":17470,"sequence":145852,"levels":[{"bid_px":"3702250000000","ask_px":"3702750000000","bid_sz":20,"ask_sz":14,"bid_ct":12,"ask_ct":14}]}\n'
        '{"ts_recv":"1609113600076436915","hd":{"ts_event":"1609113600076339855","rtype":1,"publisher_id":1,"instrument_id":5482},"action":"A","side":"B","depth":0,"price":"3702250000000","size":1,"flags":130,"ts_in_delta":17409,"sequence":145853,"levels":[{"bid_px":"3702250000000","ask_px":"3702750000000","bid_sz":21,"ask_sz":14,"bid_ct":13,"ask_ct":14}]}\n'
    )

    assert written == expected


def test_mbp_1_to_json_with_all_options_writes_expected_file_to_disk(
    test_data_path: Callable[[Dataset, Schema], Path],
    tmp_path: Path,
) -> None:
    # Arrange
    data = DBNStore.from_file(path=test_data_path(Dataset.GLBX_MDP3, Schema.MBP_1))

    path = tmp_path / "test.my_mbo.json"

    # Act
    data.to_json(
        path,
        pretty_ts=True,
        pretty_px=True,
        map_symbols=True,
    )

    # Assert
    written = path.read_text()
    expected = (
        '{"ts_recv":"2020-12-28T00:00:00.006150193Z","hd":{"ts_event":"2020-12-28T00:00:00.005871213Z","rtype":1,"publisher_id":1,"instrument_id":5482},"action":"A","side":"B","depth":0,"price":"3702.250000000","size":1,"flags":130,"ts_in_delta":26128,"sequence":145805,"levels":[{"bid_px":"3702.250000000","ask_px":"3702.750000000","bid_sz":19,"ask_sz":13,"bid_ct":11,"ask_ct":13}],"symbol":"ESH1"}\n'
        '{"ts_recv":"2020-12-28T00:00:00.062687776Z","hd":{"ts_event":"2020-12-28T00:00:00.062570311Z","rtype":1,"publisher_id":1,"instrument_id":5482},"action":"A","side":"B","depth":0,"price":"3702.250000000","size":1,"flags":130,"ts_in_delta":17256,"sequence":145827,"levels":[{"bid_px":"3702.250000000","ask_px":"3702.750000000","bid_sz":20,"ask_sz":13,"bid_ct":12,"ask_ct":13}],"symbol":"ESH1"}\n'
        '{"ts_recv":"2020-12-28T00:00:00.076130343Z","hd":{"ts_event":"2020-12-28T00:00:00.076022275Z","rtype":1,"publisher_id":1,"instrument_id":5482},"action":"A","side":"A","depth":0,"price":"3702.750000000","size":1,"flags":130,"ts_in_delta":17470,"sequence":145852,"levels":[{"bid_px":"3702.250000000","ask_px":"3702.750000000","bid_sz":20,"ask_sz":14,"bid_ct":12,"ask_ct":14}],"symbol":"ESH1"}\n'
        '{"ts_recv":"2020-12-28T00:00:00.076436915Z","hd":{"ts_event":"2020-12-28T00:00:00.076339855Z","rtype":1,"publisher_id":1,"instrument_id":5482},"action":"A","side":"B","depth":0,"price":"3702.250000000","size":1,"flags":130,"ts_in_delta":17409,"sequence":145853,"levels":[{"bid_px":"3702.250000000","ask_px":"3702.750000000","bid_sz":21,"ask_sz":14,"bid_ct":13,"ask_ct":14}],"symbol":"ESH1"}\n'
    )

    assert written == expected


@pytest.mark.parametrize(
    "schema",
    [pytest.param(schema, id=str(schema)) for schema in Schema.variants()],
)
def test_dbnstore_repr(
    test_data: Callable[[Dataset, Schema], bytes],
    schema: Schema,
) -> None:
    """
    Check that a more meaningful string is returned when calling `repr()` on a
    DBNStore.
    """
    # Arrange
    stub_data = test_data(Dataset.GLBX_MDP3, schema)

    # Act
    dbnstore = DBNStore.from_bytes(data=stub_data)

    # Assert
    assert repr(dbnstore) == f"<DBNStore(schema={schema})>"


def test_dbnstore_iterable(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    """
    Tests the DBNStore iterable implementation to ensure records can be
    accessed by iteration.
    """
    # Arrange, Act
    stub_data = test_data(Dataset.GLBX_MDP3, Schema.MBO)
    dbnstore = DBNStore.from_bytes(data=stub_data)

    record_list: list[DBNRecord] = list(dbnstore)
    first: MBOMsg = record_list[0]
    second: MBOMsg = record_list[1]

    # Assert
    assert first.hd.length == 14
    assert first.hd.rtype == 160
    assert first.hd.rtype == 160
    assert first.hd.publisher_id == 1
    assert first.hd.instrument_id == 5482
    assert first.hd.ts_event == 1609099225061045683
    assert first.order_id == 647784248135
    assert first.price == 3675750000000
    assert first.size == 2
    assert first.flags == 40
    assert first.channel_id == 0
    assert first.action == "A"
    assert first.side == "B"
    assert first.ts_recv == 1609113600000000000
    assert first.ts_in_delta == 0
    assert first.sequence == 1180

    assert second.hd.length == 14
    assert second.hd.rtype == 160
    assert second.hd.rtype == 160
    assert second.hd.publisher_id == 1
    assert second.hd.instrument_id == 5482
    assert second.hd.ts_event == 1609099225061045683
    assert second.order_id == 647782686353
    assert second.price == 3675500000000
    assert second.size == 1
    assert second.flags == 40
    assert second.channel_id == 0
    assert second.action == "A"
    assert second.side == "B"
    assert second.ts_recv == 1609113600000000000
    assert second.ts_in_delta == 0
    assert second.sequence == 1160


def test_dbnstore_iterable_parallel(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    """
    Tests the DBNStore iterable implementation to ensure iterators are not
    stateful.

    For example, calling next() on one iterator does not affect another.

    """
    # Arrange, Act
    stub_data = test_data(Dataset.GLBX_MDP3, Schema.MBO)
    dbnstore = DBNStore.from_bytes(data=stub_data)

    first = iter(dbnstore)
    second = iter(dbnstore)

    # Assert
    assert next(first) == next(second)
    assert next(first) == next(second)


@pytest.mark.parametrize(
    "schema",
    [pytest.param(schema, id=str(schema)) for schema in Schema.variants()],
)
def test_dbnstore_compression_equality(
    test_data: Callable[[Dataset, Schema], bytes],
    schema: Schema,
) -> None:
    """
    Test that a DBNStore constructed from compressed data contains the same
    records as an uncompressed version.

    Note that stub data is compressed with zstandard by default.

    """
    # Arrange
    zstd_stub_data = test_data(Dataset.GLBX_MDP3, schema)
    dbn_stub_data = zstandard.ZstdDecompressor().stream_reader(zstd_stub_data).read()

    # Act
    zstd_dbnstore = DBNStore.from_bytes(zstd_stub_data)
    dbn_dbnstore = DBNStore.from_bytes(dbn_stub_data)

    # Assert
    assert len(zstd_dbnstore.to_ndarray()) == len(dbn_dbnstore.to_ndarray())
    assert zstd_dbnstore.metadata == dbn_dbnstore.metadata
    assert zstd_dbnstore.reader.read() == dbn_dbnstore.reader.read()


def test_dbnstore_buffer_short(
    test_data: Callable[[Dataset, Schema], bytes],
    tmp_path: Path,
) -> None:
    """
    Test that creating a DBNStore with missing bytes raises a BentoError when
    decoding.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor().stream_reader(test_data(Dataset.GLBX_MDP3, Schema.MBO)).read()
    )

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data[:-2])

    # Assert
    with pytest.raises(BentoError):
        list(dbnstore)

    with pytest.raises(BentoError):
        dbnstore.to_ndarray()

    with pytest.raises(BentoError):
        dbnstore.to_df()

    with pytest.raises(BentoError):
        dbnstore.to_csv(tmp_path / "test.csv")

    with pytest.raises(BentoError):
        dbnstore.to_json(tmp_path / "test.json")


def test_dbnstore_buffer_long(
    test_data: Callable[[Dataset, Schema], bytes],
    tmp_path: Path,
) -> None:
    """
    Test that creating a DBNStore with excess bytes raises a BentoError when
    decoding.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor().stream_reader(test_data(Dataset.GLBX_MDP3, Schema.MBO)).read()
    )

    # Act
    dbn_stub_data += b"\xF0\xFF"
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    # Assert
    with pytest.raises(BentoError):
        list(dbnstore)

    with pytest.raises(BentoError):
        dbnstore.to_ndarray()

    with pytest.raises(BentoError):
        dbnstore.to_df()

    with pytest.raises(BentoError):
        dbnstore.to_csv(tmp_path / "test.csv")

    with pytest.raises(BentoError):
        dbnstore.to_json(tmp_path / "test.json")


def test_dbnstore_buffer_rewind(
    test_data: Callable[[Dataset, Schema], bytes],
    tmp_path: Path,
) -> None:
    """
    Test that creating a DBNStore from a seekable buffer will rewind.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor().stream_reader(test_data(Dataset.GLBX_MDP3, Schema.MBO)).read()
    )

    # Act
    dbn_bytes = BytesIO()
    dbn_bytes.write(dbn_stub_data)
    dbnstore = DBNStore.from_bytes(data=dbn_bytes)

    # Assert
    assert len(dbnstore.to_df()) == 4


@pytest.mark.parametrize(
    "schema",
    [pytest.param(schema, id=str(schema)) for schema in Schema.variants()],
)
@pytest.mark.parametrize(
    "count",
    [
        1,
        2,
        3,
    ],
)
def test_dbnstore_to_ndarray_with_count(
    schema: Schema,
    test_data: Callable[[Dataset, Schema], bytes],
    count: int,
) -> None:
    """
    Test that calling to_ndarray with count produces an identical result to
    without.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor().stream_reader(test_data(Dataset.GLBX_MDP3, schema)).read()
    )

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    expected = dbnstore.to_ndarray()
    nd_iter = dbnstore.to_ndarray(count=count)

    # Assert
    aggregator: list[np.ndarray[Any, Any]] = []
    for batch in nd_iter:
        assert len(batch) <= count
        aggregator.append(batch)

    assert np.array_equal(expected, np.concatenate(aggregator))


@pytest.mark.parametrize(
    "schema",
    [
        Schema.MBO,
        Schema.MBP_1,
        Schema.MBP_10,
        Schema.TRADES,
        Schema.OHLCV_1S,
        Schema.OHLCV_1M,
        Schema.OHLCV_1H,
        Schema.OHLCV_1D,
        Schema.DEFINITION,
        Schema.STATISTICS,
    ],
)
@pytest.mark.parametrize(
    "count",
    [
        1,
        2,
        3,
    ],
)
def test_dbnstore_to_ndarray_with_count_live(
    schema: Schema,
    live_test_data: bytes,
    count: int,
) -> None:
    """
    Test that calling to_ndarray with count produces an identical result to
    without.
    """
    # Arrange
    dbn_stub_data = zstandard.ZstdDecompressor().stream_reader(live_test_data).read()

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    expected = dbnstore.to_ndarray(schema=schema)
    nd_iter = dbnstore.to_ndarray(schema=schema, count=count)

    # Assert
    aggregator: list[np.ndarray[Any, Any]] = []

    for batch in nd_iter:
        assert len(batch) <= count
        aggregator.append(batch)

    assert np.array_equal(expected, np.concatenate(aggregator))


@pytest.mark.parametrize(
    "schema",
    [pytest.param(schema, id=str(schema)) for schema in Schema.variants()],
)
def test_dbnstore_to_ndarray_with_schema(
    schema: Schema,
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    """
    Test that calling to_ndarray with schema produces an identical result to
    without.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor().stream_reader(test_data(Dataset.GLBX_MDP3, schema)).read()
    )

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    expected = dbnstore.to_ndarray()
    actual = dbnstore.to_ndarray(schema=schema)

    # Assert
    for i, row in enumerate(actual):
        assert row == expected[i]


def test_dbnstore_to_ndarray_with_count_empty(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    """
    Test that calling to_ndarray on a DBNStore that contains no data with count
    set returns an iterator for one empty ndarray.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor()
        .stream_reader(test_data(Dataset.GLBX_MDP3, Schema.TRADES))
        .read()
    )

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    nd_iter = dbnstore.to_ndarray(
        schema=Schema.MBO,
        count=10,
    )

    # Assert
    assert len(next(nd_iter)) == 0


@pytest.mark.parametrize(
    "schema, expected_count",
    [
        (Schema.MBO, 5),
        (Schema.MBP_1, 2),
        (Schema.MBP_10, 2),
        (Schema.TRADES, 2),
        (Schema.OHLCV_1S, 2),
        (Schema.OHLCV_1M, 2),
        (Schema.OHLCV_1H, 0),
        (Schema.OHLCV_1D, 0),
        (Schema.DEFINITION, 2),
        (Schema.STATISTICS, 9),
    ],
)
def test_dbnstore_to_ndarray_with_schema_live(
    live_test_data: bytes,
    schema: Schema,
    expected_count: int,
) -> None:
    # Arrange
    dbn_stub_data = zstandard.ZstdDecompressor().stream_reader(live_test_data).read()

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    array = dbnstore.to_ndarray(schema=schema)

    # Assert
    assert len(array) == expected_count


def test_dbnstore_to_ndarray_with_schema_empty(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    """
    Test that calling to_ndarray on a DBNStore that contains no data of the
    specified schema returns an empty DataFrame.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor()
        .stream_reader(test_data(Dataset.GLBX_MDP3, Schema.TRADES))
        .read()
    )

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    array = dbnstore.to_ndarray(schema=Schema.MBO)

    # Assert
    assert len(array) == 0


def test_dbnstore_to_ndarray_with_schema_empty_live(
    live_test_data: bytes,
) -> None:
    """
    Test that a schema must be specified for live data.
    """
    # Arrange
    dbn_stub_data = zstandard.ZstdDecompressor().stream_reader(live_test_data).read()

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    # Assert
    with pytest.raises(ValueError):
        dbnstore.to_ndarray()


@pytest.mark.parametrize(
    "schema",
    [pytest.param(schema, id=str(schema)) for schema in Schema.variants()],
)
@pytest.mark.parametrize(
    "count",
    [
        1,
        2,
        3,
    ],
)
def test_dbnstore_to_df_with_count(
    schema: Schema,
    test_data: Callable[[Dataset, Schema], bytes],
    count: int,
) -> None:
    """
    Test that calling to_df with count produces an identical result to without.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor().stream_reader(test_data(Dataset.GLBX_MDP3, schema)).read()
    )

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    expected = dbnstore.to_df()
    df_iter = dbnstore.to_df(count=count)

    # Assert
    aggregator: list[pd.DataFrame] = []
    for batch in df_iter:
        assert len(batch) <= count
        aggregator.append(batch)

    pd.testing.assert_frame_equal(
        pd.concat(aggregator),
        expected,
        check_dtype=False,
    )


@pytest.mark.parametrize(
    "schema, expected_count",
    [
        (Schema.MBO, 5),
        (Schema.MBP_1, 2),
        (Schema.MBP_10, 2),
        (Schema.TRADES, 2),
        (Schema.OHLCV_1S, 2),
        (Schema.OHLCV_1M, 2),
        (Schema.OHLCV_1H, 0),
        (Schema.OHLCV_1D, 0),
        (Schema.DEFINITION, 2),
        (Schema.STATISTICS, 9),
    ],
)
def test_dbnstore_to_df_with_schema_live(
    schema: Schema,
    live_test_data: bytes,
    expected_count: int,
) -> None:
    """
    Test that calling to_df with schema produces a DataFrame for live data.
    """
    # Arrange
    dbn_stub_data = zstandard.ZstdDecompressor().stream_reader(live_test_data).read()

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    df = dbnstore.to_df(schema=schema)

    # Assert
    assert len(df) == expected_count


def test_dbnstore_to_df_with_schema_empty(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    """
    Test that calling to_df on a DBNStore that contains no data of the
    specified schema returns an empty DataFrame.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor()
        .stream_reader(test_data(Dataset.GLBX_MDP3, Schema.TRADES))
        .read()
    )

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    df = dbnstore.to_df(schema=Schema.MBO)

    # Assert
    assert df.empty


def test_dbnstore_to_df_with_count_empty(
    test_data: Callable[[Dataset, Schema], bytes],
) -> None:
    """
    Test that calling to_df on a DBNStore that contains no data with count set
    returns an iterator for one empty DataFrame.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor()
        .stream_reader(test_data(Dataset.GLBX_MDP3, Schema.TRADES))
        .read()
    )

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    df_iter = dbnstore.to_df(
        schema=Schema.MBO,
        count=10,
    )

    # Assert
    assert next(df_iter).empty


def test_dbnstore_to_df_cannot_map_symbols_default_to_false(
    test_data: Callable[[Dataset, Schema], bytes],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test that calling to_df with on a DBNStore with a stype_out other than
    'instrument_id' won't raise a ValueError if `map_symbols` is not explicitly
    set.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor()
        .stream_reader(test_data(Dataset.GLBX_MDP3, Schema.TRADES))
        .read()
    )

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)
    monkeypatch.setattr(DBNStore, "stype_out", MagicMock(return_type=SType.RAW_SYMBOL))

    df_iter = dbnstore.to_df()

    # Assert
    assert len(df_iter) == 4
