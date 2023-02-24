import datetime as dt
import os
import sys
from pathlib import Path
from typing import List, Tuple, Union

import numpy as np
import pandas as pd
import pytest
from databento.common.bento import Bento
from databento.common.enums import Compression, Encoding, Schema, SType

from tests.fixtures import get_test_data, get_test_data_path


class TestBento:
    def test_from_file_when_not_exists_raises_expected_exception(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(FileNotFoundError):
            Bento.from_file("my_data.dbn")

    def test_from_file_when_file_empty_raises_expected_exception(self) -> None:
        # Arrange
        path = "my_data.dbn"
        Path(path).touch()

        # Act, Assert
        with pytest.raises(RuntimeError):
            Bento.from_file(path)

        # Cleanup
        os.remove(path)

    def test_sources_metadata_returns_expected_json_as_dict(self) -> None:
        # Arrange, Act
        stub_data = get_test_data(schema=Schema.MBO)
        bento = Bento.from_bytes(data=stub_data)

        # Assert
        assert bento.metadata == {
            "version": 1,
            "dataset": "GLBX.MDP3",
            "schema": "mbo",
            "stype_in": "native",
            "stype_out": "product_id",
            "start": 1609160400000000000,
            "end": 1609200000000000000,
            "limit": 2,
            "compression": "zstd",
            "record_count": 2,
            "symbols": ["ESH1"],
            "partial": [],
            "not_found": [],
            "mappings": {
                "ESH1": [
                    {
                        "start_date": dt.date(2020, 12, 28),
                        "end_date": dt.date(2020, 12, 29),
                        "symbol": "5482",
                    },
                ],
            },
        }

    def test_build_product_id_index(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)
        bento = Bento.from_bytes(data=stub_data)

        # Act
        product_id_index = bento._build_product_id_index()

        # Assert
        assert product_id_index == {dt.date(2020, 12, 28): {5482: "ESH1"}}

    def test_bento_given_initial_nbytes_returns_expected_metadata(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)

        # Act
        bento = Bento.from_bytes(data=stub_data)

        # Assert
        assert bento.dtype == np.dtype(
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
        assert bento.record_size == 56
        assert bento.nbytes == 249
        assert bento.dataset == "GLBX.MDP3"
        assert bento.schema == Schema.MBO
        assert bento.symbols == ["ESH1"]
        assert bento.stype_in == SType.NATIVE
        assert bento.stype_out == SType.PRODUCT_ID
        assert bento.start == pd.Timestamp("2020-12-28 13:00:00+0000", tz="UTC")
        assert bento.end == pd.Timestamp("2020-12-29 00:00:00+0000", tz="UTC")
        assert bento.limit == 2
        assert bento.compression == Compression.ZSTD
        assert bento.record_count == 2
        assert bento.mappings == {
            "ESH1": [
                {
                    "symbol": "5482",
                    "start_date": dt.date(2020, 12, 28),
                    "end_date": dt.date(2020, 12, 29),
                },
            ],
        }
        assert bento.symbology == {
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
                        "end_date": dt.date(2020, 12, 29),
                    },
                ],
            },
        }

    def test_file_bento_given_valid_path_initialized_expected_data(self) -> None:
        # Arrange, Act
        path = get_test_data_path(schema=Schema.MBO)
        bento = Bento.from_file(path=path)

        # Assert
        assert bento.dataset == "GLBX.MDP3"
        assert bento.nbytes == 249

    def test_to_file_persists_to_disk(self, tmp_path: Path) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)
        bento = Bento.from_bytes(data=stub_data)

        # Act
        dbn_path = tmp_path / "my_test.dbn"
        bento.to_file(path=dbn_path)

        # Assert
        assert dbn_path.is_file()
        assert dbn_path.stat().st_size == 249

    def test_to_ndarray_with_stub_data_returns_expected_array(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)
        data = Bento.from_bytes(data=stub_data)

        # Act
        array = data.to_ndarray()

        # Assert
        assert isinstance(array, np.ndarray)
        assert (
            str(array)
            == "[(14, 160, 1, 5482, 1609160400000429831, 647784973705, 3722750000000, 1, -128, 0, b'C', b'A', 1609160400000704060, 22993, 1170352)\n (14, 160, 1, 5482, 1609160400000431665, 647784973631, 3723000000000, 1, -128, 0, b'C', b'A', 1609160400000711344, 19621, 1170353)]"  # noqa
        )

    def test_replay_with_stub_data_record_passes_to_callback(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)
        data = Bento.from_bytes(data=stub_data)

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
        self,
        schema: Schema,
    ) -> None:
        # Arrange
        stub_data = get_test_data(schema=schema)
        data = Bento.from_bytes(data=stub_data)

        # Act
        df = data.to_df()

        # Assert
        assert list(df.columns) == list(df.columns)
        assert len(df) == 2

    def test_to_df_with_mbo_data_returns_expected_record(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)
        data = Bento.from_bytes(data=stub_data)

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

    def test_to_df_with_stub_ohlcv_data_returns_expected_record(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.OHLCV_1M)
        data = Bento.from_bytes(data=stub_data)

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

    def test_to_df_with_pretty_ts_converts_timestamps_as_expected(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)
        data = Bento.from_bytes(data=stub_data)

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
        self,
        schema: Schema,
        columns: List[str],
    ) -> None:
        # Arrange
        stub_data = get_test_data(schema=schema)
        data = Bento.from_bytes(data=stub_data)

        # Act
        df = data.to_df(pretty_px=True)

        # Assert
        assert len(df) == 2
        for column in columns:
            assert isinstance(df[column].iloc(0)[1], float)
        # TODO(cs): Check float values once display factor fixed

    @pytest.mark.parametrize(
        "expected_schema, expected_encoding, expected_compression",
        [
            [Schema.MBO, Encoding.DBN, Compression.ZSTD],
            [Schema.MBP_1, Encoding.DBN, Compression.ZSTD],
            [Schema.MBP_10, Encoding.DBN, Compression.ZSTD],
            [Schema.TBBO, Encoding.DBN, Compression.ZSTD],
            [Schema.TRADES, Encoding.DBN, Compression.ZSTD],
            [Schema.OHLCV_1S, Encoding.DBN, Compression.ZSTD],
            [Schema.OHLCV_1M, Encoding.DBN, Compression.ZSTD],
            [Schema.OHLCV_1H, Encoding.DBN, Compression.ZSTD],
            [Schema.OHLCV_1D, Encoding.DBN, Compression.ZSTD],
        ],
    )
    def test_from_file_given_various_paths_returns_expected_metadata(
        self,
        expected_schema: Schema,
        expected_encoding: Encoding,
        expected_compression: Compression,
    ) -> None:
        # Arrange, Act
        path = get_test_data_path(schema=expected_schema)
        data = Bento.from_file(path=path)

        # Assert
        assert data.schema == expected_schema
        assert data.compression == expected_compression

    def test_mbo_to_csv_writes_expected_file_to_disk(self) -> None:
        # Arrange
        test_data_path = get_test_data_path(schema=Schema.MBO)
        data = Bento.from_file(path=test_data_path)

        path = "test.my_mbo.csv"

        # Act
        data.to_csv(
            path,
            pretty_ts=False,
            pretty_px=False,
            map_symbols=False,
        )

        # Assert
        written = open(path, mode="rb").read()
        assert os.path.isfile(path)
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

        # Cleanup
        os.remove(path)

    def test_mbp_1_to_csv_with_no_options_writes_expected_file_to_disk(self) -> None:
        # Arrange
        test_data_path = get_test_data_path(schema=Schema.MBP_1)
        data = Bento.from_file(path=test_data_path)

        path = "test.my_mbo.csv"

        # Act
        data.to_csv(
            path,
            pretty_ts=False,
            pretty_px=False,
            map_symbols=False,
        )

        # Assert
        written = open(path, mode="rb").read()
        assert os.path.isfile(path)
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

        # Cleanup
        os.remove(path)

    def test_mbp_1_to_csv_with_all_options_writes_expected_file_to_disk(self) -> None:
        # Arrange
        test_data_path = get_test_data_path(schema=Schema.MBP_1)
        data = Bento.from_file(path=test_data_path)

        path = "test.my_mbo.csv"

        # Act
        data.to_csv(
            path,
            pretty_ts=True,
            pretty_px=True,
            map_symbols=True,
        )

        # Assert
        written = open(path, mode="rb").read()
        assert os.path.isfile(path)
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

        # Cleanup
        os.remove(path)

    def test_mbo_to_json_with_no_options_writes_expected_file_to_disk(self) -> None:
        # Arrange
        test_data_path = get_test_data_path(schema=Schema.MBO)
        data = Bento.from_file(path=test_data_path)

        path = "test.my_mbo.json"

        # Act
        data.to_json(
            path,
            pretty_ts=False,
            pretty_px=False,
            map_symbols=False,
        )

        # Assert
        written = open(path, mode="rb").read()
        assert os.path.isfile(path)
        assert written == (
            b'{"ts_event":1609160400000429831,"ts_in_delta":22993,"publisher_id":1,"channe'  # noqa
            b'l_id":0,"product_id":5482,"order_id":647784973705,"action":"C","side":"A","f'  # noqa
            b'lags":128,"price":3722750000000,"size":1,"sequence":1170352}\n{"ts_event"'  # noqa
            b':1609160400000431665,"ts_in_delta":19621,"publisher_id":1,"channel_id":0,"pr'  # noqa
            b'oduct_id":5482,"order_id":647784973631,"action":"C","side":"A","flags":128,"'  # noqa
            b'price":3723000000000,"size":1,"sequence":1170353}\n'
        )

        # Cleanup
        os.remove(path)

    def test_mbo_to_json_with_all_options_writes_expected_file_to_disk(self) -> None:
        # Arrange
        test_data_path = get_test_data_path(schema=Schema.MBO)
        data = Bento.from_file(path=test_data_path)

        path = "test.my_mbo.json"

        # Act
        data.to_json(
            path,
            pretty_ts=True,
            pretty_px=True,
            map_symbols=True,
        )

        # Assert
        written = open(path, mode="rb").read()
        assert os.path.isfile(path)
        assert written == (
            b'{"ts_event":1609160400000,"ts_in_delta":22993,"publisher_id":1,"ch'
            b'annel_id":0,"product_id":5482,"order_id":647784973705,"action":"C"'
            b',"side":"A","flags":128,"price":3722.75,"size":1,"sequence":117035'
            b'2,"symbol":"ESH1"}\n{"ts_event":1609160400000,"ts_in_delta":19621,'
            b'"publisher_id":1,"channel_id":0,"product_id":5482,"order_id":64778'
            b'4973631,"action":"C","side":"A","flags":128,"price":3723.0,"size":'
            b'1,"sequence":1170353,"symbol":"ESH1"}\n'
        )

        # Cleanup
        os.remove(path)

    def test_mbp_1_to_json_with_no_options_writes_expected_file_to_disk(self) -> None:
        # Arrange
        test_data_path = get_test_data_path(schema=Schema.MBP_1)
        data = Bento.from_file(path=test_data_path)

        path = "test.my_mbo.json"

        # Act
        data.to_json(
            path,
            pretty_ts=False,
            pretty_px=False,
            map_symbols=False,
        )

        # Assert
        written = open(path, mode="rb").read()
        assert os.path.isfile(path)
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

        # Cleanup
        os.remove(path)

    def test_mbp_1_to_json_with_all_options_writes_expected_file_to_disk(self) -> None:
        # Arrange
        test_data_path = get_test_data_path(schema=Schema.MBP_1)
        data = Bento.from_file(path=test_data_path)

        path = "test.my_mbo.json"

        # Act
        data.to_json(
            path,
            pretty_ts=True,
            pretty_px=True,
            map_symbols=True,
        )

        # Assert
        written = open(path, mode="rb").read()
        assert os.path.isfile(path)
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

        # Cleanup
        os.remove(path)

    def test_bento_iterable(self) -> None:
        """
        Tests the Bento iterable implementation to ensure records
        can be accessed by iteration.
        """
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)
        bento = Bento.from_bytes(data=stub_data)

        record_list = list(bento)
        assert (
            str(record_list[0])
            == "(14, 160, 1, 5482, 1609160400000429831, 647784973705, "
            "3722750000000, 1, -128, 0, b'C', b'A', 1609160400000704060, "
            "22993, 1170352)"
        )
        assert (
            str(record_list[1])
            == "(14, 160, 1, 5482, 1609160400000429831, 647784973705, "
            "3722750000000, 1, -128, 0, b'C', b'A', 1609160400000704060, "
            "22993, 1170352)"
        )

    def test_bento_iterable_parallel(self) -> None:
        """
        Tests the Bento iterable implementation to ensure iterators are
        not stateful. For example, calling next() on one iterator does
        not affect another.
        """
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)
        bento = Bento.from_bytes(data=stub_data)

        first = iter(bento)
        second = iter(bento)

        assert (
            str(next(first)) == "(14, 160, 1, 5482, 1609160400000429831, 647784973705, "
            "3722750000000, 1, -128, 0, b'C', b'A', 1609160400000704060, "
            "22993, 1170352)"
        )
        assert (
            str(next(second))
            == "(14, 160, 1, 5482, 1609160400000429831, 647784973705, "
            "3722750000000, 1, -128, 0, b'C', b'A', 1609160400000704060, "
            "22993, 1170352)"
        )
        assert (
            str(next(second))
            == "(14, 160, 1, 5482, 1609160400000429831, 647784973705, "
            "3722750000000, 1, -128, 0, b'C', b'A', 1609160400000704060, "
            "22993, 1170352)"
        )
        assert (
            str(next(first)) == "(14, 160, 1, 5482, 1609160400000429831, 647784973705, "
            "3722750000000, 1, -128, 0, b'C', b'A', 1609160400000704060, "
            "22993, 1170352)"
        )
