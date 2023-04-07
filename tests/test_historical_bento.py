import collections
import datetime as dt
import sys
from pathlib import Path
from typing import Callable, List, Tuple, Union

import databento
import numpy as np
import pandas as pd
import pytest
import zstandard
from databento.common.data import DEFINITION_DROP_COLUMNS
from databento.common.dbnstore import DBNStore
from databento.common.enums import Schema, SType
from databento.common.error import BentoError


def test_from_file_when_not_exists_raises_expected_exception() -> None:
    # Arrange, Act, Assert
    with pytest.raises(FileNotFoundError):
        DBNStore.from_file("my_data.dbn")


def test_from_file_when_file_empty_raises_expected_exception(
    tmp_path: Path,
) -> None:
    # Arrange
    path = tmp_path / "test.dbn"
    path.touch()

    # Act, Assert
    with pytest.raises(RuntimeError):
        DBNStore.from_file(path)


def test_sources_metadata_returns_expected_json_as_dict(
    test_data: Callable[[Schema], bytes],
) -> None:
    # Arrange, Act
    stub_data = test_data(Schema.MBO)
    dbnstore = DBNStore.from_bytes(data=stub_data)

    # Assert
    assert dbnstore.metadata == {
        "version": 1,
        "dataset": "GLBX.MDP3",
        "schema": "mbo",
        "stype_in": "native",
        "stype_out": "product_id",
        "start": 1609160400000000000,
        "end": 1609246860000000000,
        "limit": 2,
        "symbols": ["ESH1"],
        "ts_out": False,
        "partial": [],
        "not_found": [],
        "mappings": {
            "ESH1": [
                {
                    "start_date": dt.date(2020, 12, 28),
                    "end_date": dt.date(2020, 12, 30),
                    "symbol": "5482",
                },
            ],
        },
    }


def test_build_product_id_index(
    test_data: Callable[[Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Schema.MBO)
    dbnstore = DBNStore.from_bytes(data=stub_data)

    # Act
    product_id_index = dbnstore._build_product_id_index()

    # Assert
    assert product_id_index == {
        dt.date(2020, 12, 28): {5482: "ESH1"},
        dt.date(2020, 12, 29): {5482: "ESH1"},
    }


def test_dbnstore_given_initial_nbytes_returns_expected_metadata(
    test_data: Callable[[Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Schema.MBO)

    # Act
    dbnstore = DBNStore.from_bytes(data=stub_data)

    # Assert
    assert dbnstore.dtype == np.dtype(
        [
            ("length", "u1"),
            ("rtype", "u1"),
            ("publisher_id", "<u2"),
            ("product_id", "<u4"),
            ("ts_event", "<u8"),
            ("order_id", "<u8"),
            ("price", "<i8"),
            ("size", "<u4"),
            ("flags", "i1"),
            ("channel_id", "u1"),
            ("action", "S1"),
            ("side", "S1"),
            ("ts_recv", "<u8"),
            ("ts_in_delta", "<i4"),
            ("sequence", "<u4"),
        ],
    )
    assert dbnstore.record_size == 56
    assert dbnstore.nbytes == 182
    assert dbnstore.dataset == "GLBX.MDP3"
    assert dbnstore.schema == Schema.MBO
    assert dbnstore.symbols == ["ESH1"]
    assert dbnstore.stype_in == SType.NATIVE
    assert dbnstore.stype_out == SType.PRODUCT_ID
    assert dbnstore.start == pd.Timestamp("2020-12-28 13:00:00+0000", tz="UTC")
    assert dbnstore.end == pd.Timestamp("2020-12-29 13:01:00+0000", tz="UTC")
    assert dbnstore.limit == 2
    assert len(dbnstore.to_ndarray()) == 2
    assert dbnstore.mappings == {
        "ESH1": [
            {
                "symbol": "5482",
                "start_date": dt.date(2020, 12, 28),
                "end_date": dt.date(2020, 12, 30),
            },
        ],
    }
    assert dbnstore.symbology == {
        "symbols": ["ESH1"],
        "stype_in": "native",
        "stype_out": "product_id",
        "start_date": "2020-12-28",
        "end_date": "2020-12-29",
        "not_found": [],
        "partial": [],
        "mappings": {
            "ESH1": [
                {
                    "symbol": "5482",
                    "start_date": dt.date(2020, 12, 28),
                    "end_date": dt.date(2020, 12, 30),
                },
            ],
        },
    }


def test_file_dbnstore_given_valid_path_initialized_expected_data(
    test_data_path: Callable[[Schema], Path],
) -> None:
    # Arrange, Act
    path = test_data_path(Schema.MBO)
    dbnstore = DBNStore.from_file(path=path)

    # Assert
    assert dbnstore.dataset == "GLBX.MDP3"
    assert dbnstore.nbytes == 182


def test_to_file_persists_to_disk(
    test_data: Callable[[Schema], bytes],
    tmp_path: Path,
) -> None:
    # Arrange
    stub_data = test_data(Schema.MBO)
    dbnstore = DBNStore.from_bytes(data=stub_data)

    # Act
    dbn_path = tmp_path / "my_test.dbn"
    dbnstore.to_file(path=dbn_path)

    # Assert
    assert dbn_path.exists()
    assert dbn_path.stat().st_size == 182


def test_to_ndarray_with_stub_data_returns_expected_array(
    test_data: Callable[[Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Schema.MBO)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    array = data.to_ndarray()

    # Assert
    assert isinstance(array, np.ndarray)
    assert (
        str(array)
        == "[(14, 160, 1, 5482, 1609160400000429831, 647784973705, 3722750000000, 1, -128, 0, b'C', b'A', 1609160400000704060, 22993, 1170352)\n (14, 160, 1, 5482, 1609160400000431665, 647784973631, 3723000000000, 1, -128, 0, b'C', b'A', 1609160400000711344, 19621, 1170353)]"  # noqa
    )


def test_iterator_produces_expected_data(
    test_data: Callable[[Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Schema.MBO)
    data = DBNStore.from_bytes(data=stub_data)

    # Act (consume iterator)
    handler = collections.deque(data)

    # Assert
    assert len(handler) == 2


def test_replay_with_stub_data_record_passes_to_callback(
    test_data: Callable[[Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Schema.MBO)
    data = DBNStore.from_bytes(data=stub_data)

    handler: List[Tuple[Union[int, bytes], ...]] = []

    # Act
    data.replay(callback=handler.append)

    # Assert
    assert (
        str(handler[0])
        == "(14, 160, 1, 5482, 1609160400000429831, 647784973705, 3722750000000, 1, -128, 0, b'C', b'A', 1609160400000704060, 22993, 1170352)"  # noqa
    )


@pytest.mark.parametrize(
    "schema",
    [
        s
        for s in Schema
        if s
        not in (
            Schema.OHLCV_1H,
            Schema.OHLCV_1D,
            Schema.STATUS,
            Schema.STATISTICS,
            Schema.DEFINITION,
            Schema.GATEWAY_ERROR,
            Schema.SYMBOL_MAPPING,
        )
    ],
)
def test_to_df_across_schemas_returns_identical_dimension_dfs(
    test_data: Callable[[Schema], bytes],
    schema: Schema,
) -> None:
    # Arrange
    stub_data = test_data(schema)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    df = data.to_df()

    # Assert
    assert list(df.columns) == list(df.columns)
    assert len(df) == 2


@pytest.mark.parametrize(
    "schema",
    [
        pytest.param(schema, id=str(schema))
        for schema in (
            Schema.MBO,
            Schema.MBP_1,
            Schema.MBP_10,
            Schema.TBBO,
            Schema.TRADES,
            Schema.OHLCV_1S,
            Schema.OHLCV_1M,
            Schema.OHLCV_1H,
            Schema.OHLCV_1D,
            Schema.DEFINITION,
        )
    ],
)
def test_to_df_drop_columns(
    test_data: Callable[[Schema], bytes],
    schema: Schema,
) -> None:
    """
    Test that rtype, length, reserved, and dummy columns are dropped when
    calling to_df().
    """
    # Arrange
    stub_data = test_data(schema)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    df = data.to_df()

    # Assert
    for col in DEFINITION_DROP_COLUMNS:
        assert col not in df.columns


def test_to_df_with_mbo_data_returns_expected_record(
    test_data: Callable[[Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Schema.MBO)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    df = data.to_df(
        pretty_ts=False,
        pretty_px=False,
        map_symbols=False,
    )

    # Assert
    assert len(df) == 2
    assert df.index.name == "ts_recv"
    assert df.index.values[0] == 1609160400000704060
    assert df.iloc[0].ts_event == 1609160400000429831
    assert df.iloc[0].publisher_id == 1
    assert df.iloc[0].product_id == 5482
    assert df.iloc[0].order_id == 647784973705
    assert df.iloc[0].action == "C"
    assert df.iloc[0].side == "A"
    assert df.iloc[0].price == 3722750000000
    assert df.iloc[0].size == 12
    assert df.iloc[0].sequence == 1170352


def test_to_df_with_stub_ohlcv_data_returns_expected_record(
    test_data: Callable[[Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Schema.OHLCV_1M)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    df = data.to_df(
        pretty_ts=False,
        pretty_px=False,
        map_symbols=False,
    )

    # Assert
    assert len(df) == 2
    assert df.index.name == "ts_event"
    assert df.index.values[0] == 1609160400000000000
    assert df.iloc[0].product_id == 5482
    assert df.iloc[0].open == 3_720_250_000_000
    assert df.iloc[0].high == 3_721_500_000_000
    assert df.iloc[0].low == 3_720_250_000_000
    assert df.iloc[0].close == 3_721_000_000_000
    assert df.iloc[0].volume == 353


def test_to_df_with_pretty_ts_converts_timestamps_as_expected(
    test_data: Callable[[Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Schema.MBO)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    df = data.to_df(pretty_ts=True)

    # Assert
    index0 = df.index[0]
    event0 = df["ts_event"][0]
    assert isinstance(index0, pd.Timestamp)
    assert isinstance(event0, pd.Timestamp)
    assert index0 == pd.Timestamp("2020-12-28 13:00:00.000704060+0000", tz="UTC")
    assert event0 == pd.Timestamp("2020-12-28 13:00:00.000429831+0000", tz="UTC")
    assert len(df) == 2


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
def test_to_df_with_pretty_px_with_various_schemas_converts_prices_as_expected(
    test_data: Callable[[Schema], bytes],
    schema: Schema,
    columns: List[str],
) -> None:
    # Arrange
    stub_data = test_data(schema)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    df = data.to_df(pretty_px=True)

    # Assert
    assert len(df) == 2
    for column in columns:
        assert isinstance(df[column].iloc(0)[1], float)
    # TODO(cs): Check float values once display factor fixed


@pytest.mark.parametrize(
    "expected_schema",
    [
        Schema.MBO,
        Schema.MBP_1,
        Schema.MBP_10,
        Schema.TBBO,
        Schema.TRADES,
        Schema.OHLCV_1S,
        Schema.OHLCV_1M,
        Schema.OHLCV_1H,
        Schema.OHLCV_1D,
    ],
)
def test_from_file_given_various_paths_returns_expected_metadata(
    test_data_path: Callable[[Schema], Path],
    expected_schema: Schema,
) -> None:
    # Arrange
    path = test_data_path(expected_schema)

    # Act
    data = DBNStore.from_file(path=path)

    # Assert
    assert data.schema == expected_schema


def test_from_dbn_alias(
    test_data_path: Callable[[Schema], Path],
) -> None:
    # Arrange
    path = test_data_path(Schema.MBO)

    # Act
    data = databento.from_dbn(path=path)

    # Assert
    assert data.schema == Schema.MBO
    assert len(data.to_ndarray()) == 2


def test_mbo_to_csv_writes_expected_file_to_disk(
    test_data_path: Callable[[Schema], Path],
    tmp_path: Path,
) -> None:
    # Arrange
    data = DBNStore.from_file(path=test_data_path(Schema.MBO))

    path = tmp_path / "test.my_mbo.csv"

    # Act
    data.to_csv(
        path,
        pretty_ts=False,
        pretty_px=False,
        map_symbols=False,
    )

    # Assert
    written = open(path, mode="rb").read()
    assert path.exists()
    expected = (
        b"ts_recv,ts_event,ts_in_delta,publisher_id,channel_id,product_id,order_id,act"  # noqa
        b"ion,side,flags,price,size,sequence\n1609160400000704060,16091604000004298"  # noqa
        b"31,22993,1,0,5482,647784973705,C,A,128,3722750000000,1,1170352\n160916040"  # noqa
        b"0000711344,1609160400000431665,19621,1,0,5482,647784973631,C,A,128,372300000"  # noqa
        b"0000,1,1170353\n"
    )
    if sys.platform == "win32":
        expected = expected.replace(b"\n", b"\r\n")
    assert written == expected


def test_mbp_1_to_csv_with_no_options_writes_expected_file_to_disk(
    test_data_path: Callable[[Schema], Path],
    tmp_path: Path,
) -> None:
    # Arrange
    data = DBNStore.from_file(path=test_data_path(Schema.MBP_1))

    path = tmp_path / "test.my_mbo.csv"

    # Act
    data.to_csv(
        path,
        pretty_ts=False,
        pretty_px=False,
        map_symbols=False,
    )

    # Assert
    written = open(path, mode="rb").read()
    assert path.exists()
    expected = (
        b"ts_recv,ts_event,ts_in_delta,publisher_id,product_id,action,side,depth,flags"  # noqa
        b",price,size,sequence,bid_px_00,ask_px_00,bid_sz_00,ask_sz_00,bid_oq_00,ask_o"  # noqa
        b"q_00\n1609160400006136329,1609160400006001487,17214,1,5482,A,A,0,128,3720"  # noqa
        b"500000000,1,1170362,3720250000000,3720500000000,24,11,15,9\n1609160400006"  # noqa
        b"246513,1609160400006146661,18858,1,5482,A,A,0,128,3720500000000,1,1170364,37"  # noqa
        b"20250000000,3720500000000,24,12,15,10\n"
    )
    if sys.platform == "win32":
        expected = expected.replace(b"\n", b"\r\n")
    assert written == expected


def test_mbp_1_to_csv_with_all_options_writes_expected_file_to_disk(
    test_data_path: Callable[[Schema], Path],
    tmp_path: Path,
) -> None:
    # Arrange
    data = DBNStore.from_file(path=test_data_path(Schema.MBP_1))

    path = tmp_path / "test.my_mbo.csv"

    # Act
    data.to_csv(
        path,
        pretty_ts=True,
        pretty_px=True,
        map_symbols=True,
    )

    # Assert
    written = open(path, mode="rb").read()
    assert path.exists()
    expected = (
        b"ts_recv,ts_event,ts_in_delta,publisher_id,product_id,action,si"
        b"de,depth,flags,price,size,sequence,bid_px_00,ask_px_00,bid_sz_"
        b"00,ask_sz_00,bid_oq_00,ask_oq_00,symbol\n2020-12-28 13:00:00.0"
        b"06136329+00:00,2020-12-28 13:00:00.006001487+00:00,17214,1,548"
        b"2,A,A,0,128,3720.5000000000005,1,1170362,3720.2500000000005,37"
        b"20.5000000000005,24,11,15,9,ESH1\n2020-12-28 13:00:00.00624651"
        b"3+00:00,2020-12-28 13:00:00.006146661+00:00,18858,1,5482,A,A,0"
        b",128,3720.5000000000005,1,1170364,3720.2500000000005,3720.5000"
        b"000000005,24,12,15,10,ESH1\n"
    )
    if sys.platform == "win32":
        expected = expected.replace(b"\n", b"\r\n")
    assert written == expected


def test_mbo_to_json_with_no_options_writes_expected_file_to_disk(
    test_data_path: Callable[[Schema], Path],
    tmp_path: Path,
) -> None:
    # Arrange
    data = DBNStore.from_file(path=test_data_path(Schema.MBO))

    path = tmp_path / "test.my_mbo.json"

    # Act
    data.to_json(
        path,
        pretty_ts=False,
        pretty_px=False,
        map_symbols=False,
    )

    # Assert
    written = open(path, mode="rb").read()
    assert path.exists()
    assert written == (
        b'{"ts_event":1609160400000429831,"ts_in_delta":22993,"publisher_id":1,"channe'  # noqa
        b'l_id":0,"product_id":5482,"order_id":647784973705,"action":"C","side":"A","f'  # noqa
        b'lags":128,"price":3722750000000,"size":1,"sequence":1170352}\n{"ts_event"'  # noqa
        b':1609160400000431665,"ts_in_delta":19621,"publisher_id":1,"channel_id":0,"pr'  # noqa
        b'oduct_id":5482,"order_id":647784973631,"action":"C","side":"A","flags":128,"'  # noqa
        b'price":3723000000000,"size":1,"sequence":1170353}\n'
    )


def test_mbo_to_json_with_all_options_writes_expected_file_to_disk(
    test_data_path: Callable[[Schema], Path],
    tmp_path: Path,
) -> None:
    # Arrange
    data = DBNStore.from_file(path=test_data_path(Schema.MBO))

    path = tmp_path / "test.my_mbo.json"

    # Act
    data.to_json(
        path,
        pretty_ts=True,
        pretty_px=True,
        map_symbols=True,
    )

    # Assert
    written = open(path, mode="rb").read()
    assert path.exists()
    assert written == (
        b'{"ts_event":1609160400000,"ts_in_delta":22993,"publisher_id":1,"ch'
        b'annel_id":0,"product_id":5482,"order_id":647784973705,"action":"C"'
        b',"side":"A","flags":128,"price":3722.75,"size":1,"sequence":117035'
        b'2,"symbol":"ESH1"}\n{"ts_event":1609160400000,"ts_in_delta":19621,'
        b'"publisher_id":1,"channel_id":0,"product_id":5482,"order_id":64778'
        b'4973631,"action":"C","side":"A","flags":128,"price":3723.0,"size":'
        b'1,"sequence":1170353,"symbol":"ESH1"}\n'
    )


def test_mbp_1_to_json_with_no_options_writes_expected_file_to_disk(
    test_data_path: Callable[[Schema], Path],
    tmp_path: Path,
) -> None:
    # Arrange
    data = DBNStore.from_file(path=test_data_path(Schema.MBP_1))

    path = tmp_path / "test.my_mbo.json"

    # Act
    data.to_json(
        path,
        pretty_ts=False,
        pretty_px=False,
        map_symbols=False,
    )

    # Assert
    written = open(path, mode="rb").read()
    assert path.exists()
    assert written == (
        b'{"ts_event":1609160400006001487,"ts_in_delta":17214,"publisher_id":1,"produc'  # noqa
        b't_id":5482,"action":"A","side":"A","depth":0,"flags":128,"price":37205000000'  # noqa
        b'00,"size":1,"sequence":1170362,"bid_px_00":3720250000000,"ask_px_00":3720500'  # noqa
        b'000000,"bid_sz_00":24,"ask_sz_00":11,"bid_oq_00":15,"ask_oq_00":9}\n{"ts_'  # noqa
        b'event":1609160400006146661,"ts_in_delta":18858,"publisher_id":1,"product_id"'  # noqa
        b':5482,"action":"A","side":"A","depth":0,"flags":128,"price":3720500000000,"s'  # noqa
        b'ize":1,"sequence":1170364,"bid_px_00":3720250000000,"ask_px_00":372050000000'  # noqa
        b'0,"bid_sz_00":24,"ask_sz_00":12,"bid_oq_00":15,"ask_oq_00":10}\n'  # noqa
    )


def test_mbp_1_to_json_with_all_options_writes_expected_file_to_disk(
    test_data_path: Callable[[Schema], Path],
    tmp_path: Path,
) -> None:
    # Arrange
    data = DBNStore.from_file(path=test_data_path(Schema.MBP_1))

    path = tmp_path / "test.my_mbo.json"

    # Act
    data.to_json(
        path,
        pretty_ts=True,
        pretty_px=True,
        map_symbols=True,
    )

    # Assert
    written = open(path, mode="rb").read()
    assert path.exists()
    assert written == (
        b'{"ts_event":1609160400006,"ts_in_delta":17214,"publisher_id":1,"pr'
        b'oduct_id":5482,"action":"A","side":"A","depth":0,"flags":128,"pric'
        b'e":3720.5,"size":1,"sequence":1170362,"bid_px_00":3720.25,"ask_px_'
        b'00":3720.5,"bid_sz_00":24,"ask_sz_00":11,"bid_oq_00":15,"ask_oq_00'
        b'":9,"symbol":"ESH1"}\n{"ts_event":1609160400006,"ts_in_delta":1885'
        b'8,"publisher_id":1,"product_id":5482,"action":"A","side":"A","dept'
        b'h":0,"flags":128,"price":3720.5,"size":1,"sequence":1170364,"bid_p'
        b'x_00":3720.25,"ask_px_00":3720.5,"bid_sz_00":24,"ask_sz_00":12,"bi'
        b'd_oq_00":15,"ask_oq_00":10,"symbol":"ESH1"}\n'
    )


@pytest.mark.parametrize(
    "schema",
    [
        s
        for s in Schema
        if s
        not in (
            Schema.OHLCV_1H,
            Schema.OHLCV_1D,
            Schema.STATUS,
            Schema.STATISTICS,
            Schema.DEFINITION,
            Schema.GATEWAY_ERROR,
            Schema.SYMBOL_MAPPING,
        )
    ],
)
def test_dbnstore_repr(
    test_data: Callable[[Schema], bytes],
    schema: Schema,
) -> None:
    """
    Check that a more meaningful string is returned
    when calling `repr()` on a DBNStore.
    """
    # Arrange
    stub_data = test_data(schema)

    # Act
    dbnstore = DBNStore.from_bytes(data=stub_data)

    # Assert
    assert repr(dbnstore) == f"<DBNStore(schema={schema})>"


def test_dbnstore_iterable(
    test_data: Callable[[Schema], bytes],
) -> None:
    """
    Tests the DBNStore iterable implementation to ensure records
    can be accessed by iteration.
    """
    # Arrange
    stub_data = test_data(Schema.MBO)
    dbnstore = DBNStore.from_bytes(data=stub_data)

    record_list = list(dbnstore)
    assert (
        str(record_list[0]) == "(14, 160, 1, 5482, 1609160400000429831, 647784973705, "
        "3722750000000, 1, -128, 0, b'C', b'A', 1609160400000704060, "
        "22993, 1170352)"
    )
    assert (
        str(record_list[1]) == "(14, 160, 1, 5482, 1609160400000431665, 647784973631, "
        "3723000000000, 1, -128, 0, b'C', b'A', 1609160400000711344, "
        "19621, 1170353)"
    )


def test_dbnstore_iterable_parallel(
    test_data: Callable[[Schema], bytes],
) -> None:
    """
    Tests the DBNStore iterable implementation to ensure iterators are
    not stateful. For example, calling next() on one iterator does
    not affect another.
    """
    # Arrange
    stub_data = test_data(Schema.MBO)
    dbnstore = DBNStore.from_bytes(data=stub_data)

    first = iter(dbnstore)
    second = iter(dbnstore)

    assert (
        str(next(first)) == "(14, 160, 1, 5482, 1609160400000429831, 647784973705, "
        "3722750000000, 1, -128, 0, b'C', b'A', 1609160400000704060, "
        "22993, 1170352)"
    )
    assert (
        str(next(second)) == "(14, 160, 1, 5482, 1609160400000429831, 647784973705, "
        "3722750000000, 1, -128, 0, b'C', b'A', 1609160400000704060, "
        "22993, 1170352)"
    )
    assert (
        str(next(second)) == "(14, 160, 1, 5482, 1609160400000431665, 647784973631, "
        "3723000000000, 1, -128, 0, b'C', b'A', 1609160400000711344, "
        "19621, 1170353)"
    )
    assert (
        str(next(first)) == "(14, 160, 1, 5482, 1609160400000431665, 647784973631, "
        "3723000000000, 1, -128, 0, b'C', b'A', 1609160400000711344, "
        "19621, 1170353)"
    )


@pytest.mark.parametrize(
    "schema",
    [
        Schema.MBO,
        Schema.MBP_1,
        Schema.MBP_10,
        Schema.OHLCV_1D,
        Schema.OHLCV_1H,
        Schema.OHLCV_1M,
        Schema.OHLCV_1S,
        Schema.TBBO,
        Schema.TRADES,
    ],
)
def test_dbnstore_compression_equality(
    test_data: Callable[[Schema], bytes],
    schema: Schema,
) -> None:
    """
    Test that a DBNStore constructed from compressed data contains the same
    records as an uncompressed version. Note that stub data is compressed
    with zstandard by default.
    """
    zstd_stub_data = test_data(schema)
    dbn_stub_data = zstandard.ZstdDecompressor().stream_reader(zstd_stub_data).read()

    zstd_dbnstore = DBNStore.from_bytes(zstd_stub_data)
    dbn_dbnstore = DBNStore.from_bytes(dbn_stub_data)

    assert len(zstd_dbnstore.to_ndarray()) == len(dbn_dbnstore.to_ndarray())
    assert zstd_dbnstore.metadata == dbn_dbnstore.metadata
    assert zstd_dbnstore.reader.read() == dbn_dbnstore.reader.read()


def test_dbnstore_buffer_short(
    test_data: Callable[[Schema], bytes],
    tmp_path: Path,
) -> None:
    """
    Test that creating a DBNStore with missing bytes raises a
    BentoError when decoding.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor().stream_reader(test_data(Schema.MBO)).read()
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
    test_data: Callable[[Schema], bytes],
    tmp_path: Path,
) -> None:
    """
    Test that creating a DBNStore with excess bytes raises a
    BentoError when decoding.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor().stream_reader(test_data(Schema.MBO)).read()
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
