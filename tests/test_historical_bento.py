import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from databento.common.bento import Bento, FileBento, MemoryBento
from databento.common.enums import Compression, Encoding, Schema, SType
from tests.fixtures import get_test_data, get_test_data_path


class TestBento:
    def test_from_file_when_not_exists_raises_expected_exception(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(FileNotFoundError):
            Bento.from_file("my_data.dbz")

    def test_from_file_when_file_empty_raises_expected_exception(self) -> None:
        # Arrange
        path = "my_data.dbz"
        Path(path).touch()

        # Act, Assert
        with pytest.raises(RuntimeError):
            Bento.from_file(path)

        # Cleanup
        os.remove(path)

    def test_dataset_when_metadata_with_empty_bento_raises_runtime_error(self) -> None:
        # Arrange
        data = Bento()

        # Act, Assert
        with pytest.raises(RuntimeError):
            data.dataset

    def test_sources_metadata_returns_expected_json_as_dict(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)
        data = MemoryBento(initial_bytes=stub_data)

        # Act
        metadata = data.source_metadata()
        data.set_metadata(metadata)

        # Assert
        assert metadata == {
            "version": 1,
            "dataset": "GLBX.MDP3",
            "schema": "mbo",
            "stype_in": "native",
            "stype_out": "product_id",
            "start": 1609160400000000000,
            "end": 1609200000000000000,
            "limit": 2,
            "encoding": "dbz",
            "compression": "zstd",
            "nrows": 2,
            "ncols": 14,
            "symbols": ["ESH1"],
            "status": 0,
            "partial": [],
            "not_found": [],
            "mappings": {
                "ESH1": [{"t0": "2020-12-28", "t1": "2020-12-29", "s": "5482"}]
            },
            "definitions": {},
        }
        assert data.metadata == metadata

    def test_bento_given_initial_nbytes_returns_expected_metadata(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)

        # Act
        data = MemoryBento(initial_bytes=stub_data)

        # Assert
        assert data.dtype == np.dtype(
            [
                ("nwords", "u1"),
                ("type", "u1"),
                ("dataset_id", "<u2"),
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
            ]
        )
        assert data.struct_size == 56
        assert data.nbytes == 322
        assert data.dataset == "GLBX.MDP3"
        assert data.schema == Schema.MBO
        assert data.symbols == ["ESH1"]
        assert data.stype_in == SType.NATIVE
        assert data.stype_out == SType.PRODUCT_ID
        assert data.start == pd.Timestamp("2020-12-28 13:00:00+0000", tz="UTC")
        assert data.end == pd.Timestamp("2020-12-29 00:00:00+0000", tz="UTC")
        assert data.limit == 2
        assert data.encoding == Encoding.DBZ
        assert data.compression == Compression.ZSTD
        assert data.shape == (2, 14)
        assert data.mappings == {
            "ESH1": [{"s": "5482", "t0": "2020-12-28", "t1": "2020-12-29"}]
        }
        assert data.symbology == {
            "from_date": "2020-12-28",
            "message": "OK",
            "not_found": [],
            "partial": [],
            "result": {"ESH1": [{"s": "5482", "t0": "2020-12-28", "t1": "2020-12-29"}]},
            "status": 0,
            "stype_in": "native",
            "stype_out": "product_id",
            "symbols": ["ESH1"],
            "to_date": "2020-12-29",
        }

    def test_file_bento_given_valid_path_initialized_expected_data(self) -> None:
        # Arrange, Act
        path = get_test_data_path(schema=Schema.MBO)
        data = FileBento(path=path)

        # Assert
        assert data.dataset == "GLBX.MDP3"
        assert data.nbytes == 322

    def test_to_file_persists_to_disk(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)
        data = MemoryBento(initial_bytes=stub_data)

        path = "test.my_mbo.dbz"

        # Act
        data.to_file(path=path)

        # Assert
        assert os.path.isfile(path)
        assert os.path.getsize(path) == 322

        # Cleanup
        os.remove(path)

    def test_to_ndarray_with_stub_data_returns_expected_array(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)
        data = MemoryBento(initial_bytes=stub_data)

        # Act
        array = data.to_ndarray()

        # Assert
        assert isinstance(array, np.ndarray)
        assert (
            str(array)
            == "[(14, 160, 1, 5482, 1609160400000429831, 647784973705, 372275000000000, 1, -128, 0, b'A', b'C', 1609160400000704060, 22993, 1170352)\n (14, 160, 1, 5482, 1609160400000431665, 647784973631, 372300000000000, 1, -128, 0, b'A', b'C', 1609160400000711344, 19621, 1170353)]"  # noqa
        )

    def test_replay_with_stub_data_record_passes_to_callback(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)
        data = MemoryBento(initial_bytes=stub_data)

        handler = []

        # Act
        data.replay(callback=handler.append)

        # Assert
        assert (
            str(handler[0])
            == "(14, 160, 1, 5482, 1609160400000429831, 647784973705, 372275000000000, 1, -128, 0, b'A', b'C', 1609160400000704060, 22993, 1170352)"  # noqa
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
            )
        ],
    )
    def test_to_df_across_schemas_returns_identical_dimension_dfs(self, schema) -> None:
        # Arrange
        stub_data = get_test_data(schema=schema)
        data = MemoryBento(initial_bytes=stub_data)

        # Act
        df = data.to_df()

        # Assert
        assert list(df.columns) == list(df.columns)
        assert len(df) == 2

    def test_to_df_with_mbo_data_returns_expected_record(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)
        data = MemoryBento(initial_bytes=stub_data)

        # Act
        df = data.to_df()

        # Assert
        assert len(df) == 2
        assert df.index.name == "ts_recv"
        assert df.index.values[0] == 1609160400000704060
        assert df.iloc[0].ts_event == 1609160400000429831
        assert df.iloc[0].dataset_id == 1
        assert df.iloc[0].product_id == 5482
        assert df.iloc[0].order_id == 647784973705
        assert df.iloc[0].action == "A"  # TODO(cs): Invalid until data regenerated
        assert df.iloc[0].side == "C"  # TODO(cs): Invalid until data regenerated
        assert df.iloc[0].price == 372275000000000
        assert df.iloc[0].size == 11
        assert df.iloc[0].sequence == 1170352

    def test_to_df_with_stub_ohlcv_data_returns_expected_record(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.OHLCV_1M)
        data = MemoryBento(initial_bytes=stub_data)

        # Act
        df = data.to_df()

        # Assert
        assert len(df) == 2
        assert df.index.name == "ts_event"
        assert df.index.values[0] == 1609160400000000000
        assert df.iloc[0].product_id == 5482
        assert df.iloc[0].open == 372025000000000
        assert df.iloc[0].high == 372150000000000
        assert df.iloc[0].low == 372025000000000
        assert df.iloc[0].close == 372100000000000
        assert df.iloc[0].volume == 353

    def test_to_df_with_pretty_ts_converts_timestamps_as_expected(self) -> None:
        # Arrange
        stub_data = get_test_data(schema=Schema.MBO)
        data = MemoryBento(initial_bytes=stub_data)

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
        schema,
        columns,
    ) -> None:
        # Arrange
        stub_data = get_test_data(schema=schema)
        data = MemoryBento(initial_bytes=stub_data)

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
            [Schema.MBO, Encoding.DBZ, Compression.ZSTD],
            [Schema.MBP_1, Encoding.DBZ, Compression.ZSTD],
            [Schema.MBP_10, Encoding.DBZ, Compression.ZSTD],
            [Schema.TBBO, Encoding.DBZ, Compression.ZSTD],
            [Schema.TRADES, Encoding.DBZ, Compression.ZSTD],
            [Schema.OHLCV_1S, Encoding.DBZ, Compression.ZSTD],
            [Schema.OHLCV_1M, Encoding.DBZ, Compression.ZSTD],
            [Schema.OHLCV_1H, Encoding.DBZ, Compression.ZSTD],
            [Schema.OHLCV_1D, Encoding.DBZ, Compression.ZSTD],
        ],
    )
    def test_from_file_given_various_paths_returns_expected_metadata(
        self,
        expected_schema,
        expected_encoding,
        expected_compression,
    ) -> None:
        # Arrange, Act
        path = get_test_data_path(schema=expected_schema)
        data = Bento.from_file(path=path)

        # Assert
        assert data.schema == expected_schema
        assert data.encoding == expected_encoding
        assert data.compression == expected_compression

    def test_to_csv_writes_expected_file_to_disk(self) -> None:
        # Arrange
        test_data_path = get_test_data_path(schema=Schema.MBO)
        data = FileBento(path=test_data_path)

        path = "test.my_mbo.csv"

        # Act
        data.to_csv(path)

        # Assert
        written = open(path, mode="rb").read()
        assert os.path.isfile(path)
        expected = (
            b"ts_recv,ts_event,ts_in_delta,dataset_id,product_id,order_id,action,side,flags,pr"  # noqa
            b"ice,size,sequence\n1609160400000704060,1609160400000429831,22993,1,5482,6"  # noqa
            b"47784973705,A,C,128,372275000000000,1,1170352\n1609160400000711344,160916"  # noqa
            b"0400000431665,19621,1,5482,647784973631,A,C,128,372300000000000,1,1170353\n"  # noqa
        )
        if sys.platform == "win32":
            expected = expected.replace(b"\n", b"\r\n")
        assert written == expected

        # Cleanup
        os.remove(path)

    def test_to_json_writes_expected_file_to_disk(self) -> None:
        # Arrange
        test_data_path = get_test_data_path(schema=Schema.MBO)
        data = FileBento(path=test_data_path)

        path = "test.my_mbo.json"

        # Act
        data.to_json(path)

        # Assert
        written = open(path, mode="rb").read()
        assert os.path.isfile(path)
        assert written == (
            b'{"ts_event":1609160400000429831,"ts_in_delta":22993,"dataset_id":1,"product_id":'  # noqa
            b'5482,"order_id":647784973705,"action":"A","side":"C","flags":128,"price":372'  # noqa
            b'275000000000,"size":1,"sequence":1170352}\n{"ts_event":160916040000043166'  # noqa
            b'5,"ts_in_delta":19621,"dataset_id":1,"product_id":5482,"order_id":647784973631,"'  # noqa
            b'action":"A","side":"C","flags":128,"price":372300000000000,"size":1,"sequenc'  # noqa
            b'e":1170353}\n'
        )

        # Cleanup
        os.remove(path)
