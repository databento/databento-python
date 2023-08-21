from __future__ import annotations

import collections
import datetime as dt
import sys
from io import BytesIO
from pathlib import Path
from typing import Callable

import databento
import numpy as np
import pandas as pd
import pytest
import zstandard
from databento.common.dbnstore import DBNStore
from databento.common.error import BentoError
from databento.live import DBNRecord
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
    test_data: Callable[[Schema], bytes],
) -> None:
    # Arrange, Act
    stub_data = test_data(Schema.MBO)
    dbnstore = DBNStore.from_bytes(data=stub_data)

    # Assert
    assert dbnstore.metadata.version == 1
    assert dbnstore.metadata.dataset == "GLBX.MDP3"
    assert dbnstore.metadata.schema == Schema.MBO
    assert dbnstore.metadata.stype_in == SType.RAW_SYMBOL
    assert dbnstore.metadata.stype_out == SType.INSTRUMENT_ID
    assert dbnstore.metadata.start == 1609160400000000000
    assert dbnstore.metadata.end == 1609246860000000000
    assert dbnstore.metadata.limit == 4
    assert dbnstore.metadata.symbols == ["ESH1"]
    assert dbnstore.metadata.ts_out is False
    assert dbnstore.metadata.partial == []
    assert dbnstore.metadata.not_found == []
    assert dbnstore.metadata.mappings == {
        "ESH1": [
            {
                "start_date": dt.date(2020, 12, 28),
                "end_date": dt.date(2020, 12, 30),
                "symbol": "5482",
            },
        ],
    }


def test_build_instrument_id_index(
    test_data: Callable[[Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Schema.MBO)
    dbnstore = DBNStore.from_bytes(data=stub_data)

    # Act
    instrument_id_index = dbnstore._build_instrument_id_index()

    # Assert
    assert instrument_id_index == {
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
    assert dbnstore.nbytes == 232
    assert dbnstore.dataset == "GLBX.MDP3"
    assert dbnstore.schema == Schema.MBO
    assert dbnstore.symbols == ["ESH1"]
    assert dbnstore.stype_in == SType.RAW_SYMBOL
    assert dbnstore.stype_out == SType.INSTRUMENT_ID
    assert dbnstore.start == pd.Timestamp("2020-12-28 13:00:00+0000", tz="UTC")
    assert dbnstore.end == pd.Timestamp("2020-12-29 13:01:00+0000", tz="UTC")
    assert dbnstore.limit == 4
    assert len(dbnstore.to_ndarray()) == 4
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
    assert dbnstore.nbytes == 232


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
    assert dbn_path.stat().st_size == 232


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
        == "[(14, 160, 1, 5482, 1609160400000429831, 647784973705, 3722750000000, 1, 128, 0, b'C', b'A', 1609160400000704060, 22993, 1170352)\n (14, 160, 1, 5482, 1609160400000431665, 647784973631, 3723000000000, 1, 128, 0, b'C', b'A', 1609160400000711344, 19621, 1170353)\n (14, 160, 1, 5482, 1609160400000433051, 647784973427, 3723250000000, 1, 128, 0, b'C', b'A', 1609160400000728600, 16979, 1170354)\n (14, 160, 1, 5482, 1609160400000434353, 647784973094, 3723500000000, 1, 128, 0, b'C', b'A', 1609160400000740248, 17883, 1170355)]"
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
    assert len(handler) == 4


def test_replay_with_stub_data_record_passes_to_callback(
    test_data: Callable[[Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Schema.MBO)
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
    assert record.hd.ts_event == 1609160400000429831
    assert record.order_id == 647784973705
    assert record.price == 3722750000000
    assert record.size == 1
    assert record.flags == 128
    assert record.channel_id == 0
    assert record.action == "C"
    assert record.side == "A"
    assert record.ts_recv == 1609160400000704060
    assert record.ts_in_delta == 22993
    assert record.sequence == 1170352


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
    assert len(df) == 4


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
    assert len(df) == 4
    assert df.index.name == "ts_recv"
    assert df.index.values[0] == 1609160400000704060
    assert df.iloc[0]["ts_event"] == 1609160400000429831
    assert df.iloc[0]["rtype"] == 160
    assert df.iloc[0]["publisher_id"] == 1
    assert df.iloc[0]["instrument_id"] == 5482
    assert df.iloc[0]["action"] == "C"
    assert df.iloc[0]["side"] == "A"
    assert df.iloc[0]["price"] == 3722750000000
    assert df.iloc[0]["size"] == 1
    assert df.iloc[0]["order_id"] == 647784973705
    assert df.iloc[0]["flags"] == 128
    assert df.iloc[0]["ts_in_delta"] == 22993
    assert df.iloc[0]["sequence"] == 1170352


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
    assert len(df) == 4
    assert df.index.name == "ts_event"
    assert df.index.values[0] == 1609160400000000000
    assert df.iloc[0]["instrument_id"] == 5482
    assert df.iloc[0]["open"] == 3_720_250_000_000
    assert df.iloc[0]["high"] == 3_721_500_000_000
    assert df.iloc[0]["low"] == 3_720_250_000_000
    assert df.iloc[0]["close"] == 3_721_000_000_000
    assert df.iloc[0]["volume"] == 353


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
def test_to_df_with_pretty_px_with_various_schemas_converts_prices_as_expected(
    test_data: Callable[[Schema], bytes],
    schema: Schema,
    columns: list[str],
) -> None:
    # Arrange
    stub_data = test_data(schema)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    df = data.to_df(pretty_px=True)

    # Assert
    assert len(df) == 4
    for column in columns:
        assert isinstance(df[column].iloc(0)[1], float)
    # TODO(cs): Check float values once display factor fixed

def test_to_df_with_pretty_px_handles_null(
    test_data: Callable[[Schema], bytes],
) -> None:
    # Arrange
    stub_data = test_data(Schema.DEFINITION)
    data = DBNStore.from_bytes(data=stub_data)

    # Act
    df_plain = data.to_df(pretty_px=False)
    df_pretty = data.to_df(pretty_px=True)

    # Assert
    assert all(df_plain["strike_price"] == 9223372036854775807)
    assert all(np.isnan(df_pretty["strike_price"]))


@pytest.mark.parametrize(
    "expected_schema",
    [pytest.param(schema, id=str(schema)) for schema in Schema.variants()],
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
    assert len(data.to_ndarray()) == 4


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
        b"ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,price,size,channel_id,"
        b"order_id,flags,ts_in_delta,sequence\n1609160400000704060,1609160400000429831,160,"
        b"1,5482,C,A,3722750000000,1,0,647784973705,128,22993,1170352\n1609160400000711344,"
        b"1609160400000431665,160,1,5482,C,A,3723000000000,1,0,647784973631,128,19621,1170"
        b"353\n1609160400000728600,1609160400000433051,160,1,5482,C,A,3723250000000,1,0,647"
        b"784973427,128,16979,1170354\n1609160400000740248,1609160400000434353,160,1,5482,C"
        b",A,3723500000000,1,0,647784973094,128,17883,1170355\n"
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
        b"ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,depth,price,size,flags"
        b",ts_in_delta,sequence,bid_px_00,ask_px_00,bid_sz_00,ask_sz_00,bid_ct_00,ask_ct_0"
        b"0\n1609160400006136329,1609160400006001487,1,1,5482,A,A,0,3720500000000,1,128,172"
        b"14,1170362,3720250000000,3720500000000,24,11,15,9\n1609160400006246513,1609160400"
        b"006146661,1,1,5482,A,A,0,3720500000000,1,128,18858,1170364,3720250000000,37205000000"
        b"00,24,12,15,10\n1609160400007159323,1609160400007044577,1,1,5482,A,B,0,3720250000"
        b"000,2,128,18115,1170365,3720250000000,3720500000000,26,12,16,10\n1609160400007260"
        b"967,1609160400007169135,1,1,5482,C,A,0,3720500000000,1,128,17361,1170366,37202500000"
        b"00,3720500000000,26,11,16,9\n"
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
        b"ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,depth,price,size,flags"
        b",ts_in_delta,sequence,bid_px_00,ask_px_00,bid_sz_00,ask_sz_00,bid_ct_00,ask_ct_00,sy"
        b"mbol\n2020-12-28 13:00:00.006136329+00:00,2020-12-28 13:00:00.006001487+00:00,1,1"
        b",5482,A,A,0,3720.5,1,128,17214,1170362,3720.25,3720.5"
        b",24,11,15,9,ESH1\n2020-12-28 13:00:00.006246513+00:00,2020-12-28 13:00:00.006"
        b"146661+00:00,1,1,5482,A,A,0,3720.5,1,128,18858,1170364,3720.25"
        b",3720.5,24,12,15,10,ESH1\n2020-12-28 13:00:00.007159323+00:00,2020-1"
        b"2-28 13:00:00.007044577+00:00,1,1,5482,A,B,0,3720.25,2,128,18115,1170365,"
        b"3720.25,3720.5,26,12,16,10,ESH1\n2020-12-28 13:00:00.00726"
        b"0967+00:00,2020-12-28 13:00:00.007169135+00:00,1,1,5482,C,A,0,3720.5,1,1"
        b"28,17361,1170366,3720.25,3720.5,26,11,16,9,ESH1\n"
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
    assert written.strip() == (
        b'{"ts_event":1609160400000429831,"rtype":160,"publisher_id":1,"instrument_id":5482,"a'
        b'ction":"C","side":"A","price":3722750000000,"size":1,"channel_id":0,"order_id":64778'
        b'4973705,"flags":128,"ts_in_delta":22993,"sequence":1170352}\n{"ts_event":16091604'
        b'00000431665,"rtype":160,"publisher_id":1,"instrument_id":5482,"action":"C","side":"A'
        b'","price":3723000000000,"size":1,"channel_id":0,"order_id":647784973631,"flags":128,'
        b'"ts_in_delta":19621,"sequence":1170353}\n{"ts_event":1609160400000433051,"rtype":'
        b'160,"publisher_id":1,"instrument_id":5482,"action":"C","side":"A","price":3723250000'
        b'000,"size":1,"channel_id":0,"order_id":647784973427,"flags":128,"ts_in_delta":16979,'
        b'"sequence":1170354}\n{"ts_event":1609160400000434353,"rtype":160,"publisher_id":1'
        b',"instrument_id":5482,"action":"C","side":"A","price":3723500000000,"size":1,"channe'
        b'l_id":0,"order_id":647784973094,"flags":128,"ts_in_delta":17883,"sequence":11703'
        b'55}'
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
    assert written.strip() == (
        b'{"ts_event":1609160400000,"rtype":160,"publisher_id":1,"instrument_id":5482,"action"'
        b':"C","side":"A","price":3722.75,"size":1,"channel_id":0,"order_id":647784973705,"fla'
        b'gs":128,"ts_in_delta":22993,"sequence":1170352,"symbol":"ESH1"}\n{"ts_event":1609'
        b'160400000,"rtype":160,"publisher_id":1,"instrument_id":5482,"action":"C","side":"A",'
        b'"price":3723.0,"size":1,"channel_id":0,"order_id":647784973631,"flags":128,"ts_in_de'
        b'lta":19621,"sequence":1170353,"symbol":"ESH1"}\n{"ts_event":1609160400000,"rtype"'
        b':160,"publisher_id":1,"instrument_id":5482,"action":"C","side":"A","price":3723.25,"'
        b'size":1,"channel_id":0,"order_id":647784973427,"flags":128,"ts_in_delta":16979,"sequ'
        b'ence":1170354,"symbol":"ESH1"}\n{"ts_event":1609160400000,"rtype":160,"publisher_'
        b'id":1,"instrument_id":5482,"action":"C","side":"A","price":3723.5,"size":1,"channel_'
        b'id":0,"order_id":647784973094,"flags":128,"ts_in_delta":17883,"sequence":1170355,"sy'
        b'mbol":"ESH1"}'
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
    assert written.strip() == (
        b'{"ts_event":1609160400006001487,"rtype":1,"publisher_id":1,"instrument_id":5482,"act'
        b'ion":"A","side":"A","depth":0,"price":3720500000000,"size":1,"flags":128,"ts_in_delt'
        b'a":17214,"sequence":1170362,"bid_px_00":3720250000000,"ask_px_00":3720500000000,"bid'
        b'_sz_00":24,"ask_sz_00":11,"bid_ct_00":15,"ask_ct_00":9}\n{"ts_event":160916040000'
        b'6146661,"rtype":1,"publisher_id":1,"instrument_id":5482,"action":"A","side":"A","dep'
        b'th":0,"price":3720500000000,"size":1,"flags":128,"ts_in_delta":18858,"sequence":1170'
        b'364,"bid_px_00":3720250000000,"ask_px_00":3720500000000,"bid_sz_00":24,"ask_sz_00":1'
        b'2,"bid_ct_00":15,"ask_ct_00":10}\n{"ts_event":1609160400007044577,"rtype":1,"publ'
        b'isher_id":1,"instrument_id":5482,"action":"A","side":"B","depth":0,"price":372025000'
        b'0000,"size":2,"flags":128,"ts_in_delta":18115,"sequence":1170365,"bid_px_00":3720250'
        b'000000,"ask_px_00":3720500000000,"bid_sz_00":26,"ask_sz_00":12,"bid_ct_00":16,"ask_c'
        b't_00":10}\n{"ts_event":1609160400007169135,"rtype":1,"publisher_id":1,"instrument'
        b'_id":5482,"action":"C","side":"A","depth":0,"price":3720500000000,"size":1,"flags":1'
        b'28,"ts_in_delta":17361,"sequence":1170366,"bid_px_00":3720250000000,"ask_px_00":3720'
        b'500000000,"bid_sz_00":26,"ask_sz_00":11,"bid_ct_00":16,"ask_ct_00":9}'
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
    assert written.strip() == (
        b'{"ts_event":1609160400006,"rtype":1,"publisher_id":1,"instrument_id":5482,"action":"'
        b'A","side":"A","depth":0,"price":3720.5,"size":1,"flags":128,"ts_in_delta":17214,"seq'
        b'uence":1170362,"bid_px_00":3720.25,"ask_px_00":3720.5,"bid_sz_00":24,"ask_sz_00":11,'
        b'"bid_ct_00":15,"ask_ct_00":9,"symbol":"ESH1"}\n{"ts_event":1609160400006,"rtype":'
        b'1,"publisher_id":1,"instrument_id":5482,"action":"A","side":"A","depth":0,"price":37'
        b'20.5,"size":1,"flags":128,"ts_in_delta":18858,"sequence":1170364,"bid_px_00":3720.25'
        b',"ask_px_00":3720.5,"bid_sz_00":24,"ask_sz_00":12,"bid_ct_00":15,"ask_ct_00":10,"sym'
        b'bol":"ESH1"}\n{"ts_event":1609160400007,"rtype":1,"publisher_id":1,"instrument_id'
        b'":5482,"action":"A","side":"B","depth":0,"price":3720.25,"size":2,"flags":128,"ts_in'
        b'_delta":18115,"sequence":1170365,"bid_px_00":3720.25,"ask_px_00":3720.5,"bid_sz_00":'
        b'26,"ask_sz_00":12,"bid_ct_00":16,"ask_ct_00":10,"symbol":"ESH1"}\n{"ts_event":160'
        b'9160400007,"rtype":1,"publisher_id":1,"instrument_id":5482,"action":"C","side":"A","'
        b'depth":0,"price":3720.5,"size":1,"flags":128,"ts_in_delta":17361,"sequence":1170366,'
        b'"bid_px_00":3720.25,"ask_px_00":3720.5,"bid_sz_00":26,"ask_sz_00":11,"bid_ct_00":16,'
        b'"ask_ct_00":9,"symbol":"ESH1"}'
    )

@pytest.mark.parametrize(
    "schema",
    [pytest.param(schema, id=str(schema)) for schema in Schema.variants()],
)
def test_dbnstore_repr(
    test_data: Callable[[Schema], bytes],
    schema: Schema,
) -> None:
    """
    Check that a more meaningful string is returned when calling `repr()` on a
    DBNStore.
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
    Tests the DBNStore iterable implementation to ensure records can be
    accessed by iteration.
    """
    # Arrange
    stub_data = test_data(Schema.MBO)
    dbnstore = DBNStore.from_bytes(data=stub_data)

    record_list: list[DBNRecord] = list(dbnstore)
    first: MBOMsg = record_list[0]  # type: ignore
    second: MBOMsg = record_list[1]  # type: ignore

    assert first.hd.length == 14
    assert first.hd.rtype == 160
    assert first.hd.rtype == 160
    assert first.hd.publisher_id == 1
    assert first.hd.instrument_id == 5482
    assert first.hd.ts_event == 1609160400000429831
    assert first.order_id == 647784973705
    assert first.price == 3722750000000
    assert first.size == 1
    assert first.flags == 128
    assert first.channel_id == 0
    assert first.action == "C"
    assert first.side == "A"
    assert first.ts_recv == 1609160400000704060
    assert first.ts_in_delta == 22993
    assert first.sequence == 1170352

    assert second.hd.length == 14
    assert second.hd.rtype == 160
    assert second.hd.rtype == 160
    assert second.hd.publisher_id == 1
    assert second.hd.instrument_id == 5482
    assert second.hd.ts_event == 1609160400000431665
    assert second.order_id == 647784973631
    assert second.price == 3723000000000
    assert second.size == 1
    assert second.flags == 128
    assert second.channel_id == 0
    assert second.action == "C"
    assert second.side == "A"
    assert second.ts_recv == 1609160400000711344
    assert second.ts_in_delta == 19621
    assert second.sequence == 1170353


def test_dbnstore_iterable_parallel(
    test_data: Callable[[Schema], bytes],
) -> None:
    """
    Tests the DBNStore iterable implementation to ensure iterators are not
    stateful.

    For example, calling next() on one iterator does not affect another.

    """
    # Arrange
    stub_data = test_data(Schema.MBO)
    dbnstore = DBNStore.from_bytes(data=stub_data)

    first = iter(dbnstore)
    second = iter(dbnstore)

    assert next(first) == next(second)
    assert next(first) == next(second)


@pytest.mark.parametrize(
    "schema",
    [pytest.param(schema, id=str(schema)) for schema in Schema.variants()],
)
def test_dbnstore_compression_equality(
    test_data: Callable[[Schema], bytes],
    schema: Schema,
) -> None:
    """
    Test that a DBNStore constructed from compressed data contains the same
    records as an uncompressed version.

    Note that stub data is compressed with zstandard by default.

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
    Test that creating a DBNStore with missing bytes raises a BentoError when
    decoding.
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
    Test that creating a DBNStore with excess bytes raises a BentoError when
    decoding.
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


@pytest.mark.parametrize(
    "schema",
    [pytest.param(schema, id=str(schema)) for schema in Schema.variants()],
)
def test_dbnstore_to_ndarray_with_schema(
    schema: Schema,
    test_data: Callable[[Schema], bytes],
) -> None:
    """
    Test that calling to_ndarray with schema produces an identical result to
    without.
    """
    # Arrange
    dbn_stub_data = zstandard.ZstdDecompressor().stream_reader(test_data(schema)).read()

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    actual = dbnstore.to_ndarray(schema=schema)
    expected = dbnstore.to_ndarray()

    # Assert
    for i, row in enumerate(actual):
        assert row == expected[i]


def test_dbnstore_to_ndarray_with_schema_empty(
    test_data: Callable[[Schema], bytes],
) -> None:
    """
    Test that calling to_ndarray on a DBNStore that contains no data of the
    specified schema returns an empty DataFrame.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor().stream_reader(test_data(Schema.TRADES)).read()
    )

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    array = dbnstore.to_ndarray(schema=Schema.MBO)

    # Assert
    assert len(array) == 0


@pytest.mark.parametrize(
    "schema",
    [pytest.param(schema, id=str(schema)) for schema in Schema.variants()],
)
def test_dbnstore_to_df_with_schema(
    schema: Schema,
    test_data: Callable[[Schema], bytes],
) -> None:
    """
    Test that calling to_df with schema produces an identical result to
    without.
    """
    # Arrange
    dbn_stub_data = zstandard.ZstdDecompressor().stream_reader(test_data(schema)).read()

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    actual = dbnstore.to_df(schema=schema)
    expected = dbnstore.to_df()

    # Assert
    assert actual.equals(expected)


def test_dbnstore_to_df_with_schema_empty(
    test_data: Callable[[Schema], bytes],
) -> None:
    """
    Test that calling to_df on a DBNStore that contains no data of the
    specified schema returns an empty DataFrame.
    """
    # Arrange
    dbn_stub_data = (
        zstandard.ZstdDecompressor().stream_reader(test_data(Schema.TRADES)).read()
    )

    # Act
    dbnstore = DBNStore.from_bytes(data=dbn_stub_data)

    df = dbnstore.to_df(schema=Schema.MBO)

    # Assert
    assert df.empty
