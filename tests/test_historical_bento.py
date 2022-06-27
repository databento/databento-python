import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from databento import from_file
from databento.common.bento import Bento, FileBento, MemoryBento
from databento.common.enums import Compression, Encoding, Schema


TESTS_ROOT = os.path.dirname(os.path.abspath(__file__))


def get_test_data(file_name):
    with open(os.path.join(TESTS_ROOT, "data", file_name), "rb") as f:
        return f.read()


class TestBento:
    def test_from_file_when_not_exists_raises_expected_exception(self):
        # Arrange, Act, Assert
        with pytest.raises(FileNotFoundError):
            from_file("my_data.csv")

    def test_from_file_when_file_empty_raises_expected_exception(self):
        # Arrange
        path = "my_data.csv"
        Path(path).touch()

        # Act, Assert
        with pytest.raises(RuntimeError):
            from_file(path)

        # Cleanup
        os.remove(path)

    def test_properties_when_instantiated(self) -> None:
        # Arrange
        bento_io = Bento(
            schema=Schema.MBO,
            encoding=Encoding.CSV,
            compression=Compression.ZSTD,
        )

        # Act, Assert
        assert bento_io.schema == Schema.MBO
        assert bento_io.encoding == Encoding.CSV
        assert bento_io.compression == Compression.ZSTD
        assert bento_io.struct_fmt == np.dtype(
            [
                ("order_id", "<u8"),
                ("pub_id", "<u2"),
                ("chan_id", "<u2"),
                ("product_id", "<u4"),
                ("ts_event", "<u8"),
                ("price", "<i8"),
                ("size", "<u4"),
                ("type", "S1"),
                ("flags", "i1"),
                ("side", "S1"),
                ("action", "S1"),
                ("ts_recv", "<u8"),
                ("ts_in_delta", "<i4"),
                ("sequence", "<u4"),
            ]
        )
        assert bento_io.struct_size == 56


class TestMemoryBento:
    @pytest.mark.parametrize(
        "schema, data_path, expected_encoding, expected_compression",
        [
            [Schema.MBO, "mbo.dbz", Encoding.DBZ, Compression.ZSTD],
            [Schema.MBO, "mbo.csv", Encoding.CSV, Compression.NONE],
            [Schema.MBO, "mbo.csv.zst", Encoding.CSV, Compression.ZSTD],
            [Schema.MBO, "mbo.json.raw", Encoding.JSON, Compression.NONE],
            [Schema.MBO, "mbo.json.zst", Encoding.JSON, Compression.ZSTD],
        ],
    )
    def test_memory_io_inference(
        self,
        schema,
        data_path,
        expected_encoding,
        expected_compression,
    ) -> None:
        # Arrange
        stub_data = get_test_data("test_data." + data_path)

        # Act
        bento_io = MemoryBento(schema=Schema.MBO, initial_bytes=stub_data)

        # Assert
        assert bento_io.encoding == expected_encoding
        assert bento_io.compression == expected_compression
        assert bento_io.raw == stub_data  # Ensure stream position hasn't moved

    def test_memory_io_nbytes(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.mbo.dbz")

        # Act
        bento_io = MemoryBento(schema=Schema.MBO, initial_bytes=stub_data)

        # Assert
        assert bento_io.nbytes == 84

    def test_disk_io_nbytes(self) -> None:
        # Arrange, Act
        path = os.path.join(TESTS_ROOT, "data", "test_data.mbo.dbz")
        bento_io = FileBento(path=path)

        # Assert
        assert bento_io.nbytes == 84

    @pytest.mark.parametrize(
        "path, expected_schema, expected_encoding, expected_compression",
        [
            ["mbo.dbz", Schema.MBO, Encoding.DBZ, Compression.ZSTD],
            ["mbo.csv", Schema.MBO, Encoding.CSV, Compression.NONE],
            ["mbo.csv.zst", Schema.MBO, Encoding.CSV, Compression.ZSTD],
            ["mbo.json.raw", Schema.MBO, Encoding.JSON, Compression.NONE],
            ["mbo.json.zst", Schema.MBO, Encoding.JSON, Compression.ZSTD],
            ["mbp-1.dbz", Schema.MBP_1, Encoding.DBZ, Compression.ZSTD],
            ["mbp-5.json.zst", Schema.MBP_5, Encoding.JSON, Compression.ZSTD],
            ["mbp-10.dbz", Schema.MBP_10, Encoding.DBZ, Compression.ZSTD],
            ["trades.csv", Schema.TRADES, Encoding.CSV, Compression.NONE],
            ["tbbo.csv.zst", Schema.TBBO, Encoding.CSV, Compression.ZSTD],
            ["ohlcv-1h.json.raw", Schema.OHLCV_1H, Encoding.JSON, Compression.NONE],
        ],
    )
    def test_disk_io_inference(
        self,
        path,
        expected_schema,
        expected_encoding,
        expected_compression,
    ) -> None:
        # Arrange, Act
        path = os.path.join(TESTS_ROOT, "data", "test_data." + path)
        bento_io = FileBento(path=path)

        # Assert
        assert bento_io.schema == expected_schema
        assert bento_io.encoding == expected_encoding
        assert bento_io.compression == expected_compression

    @pytest.mark.parametrize(
        "schema, "
        "encoding, "
        "compression, "
        "stub_data_path, "
        "decompress, "
        "expected_path",
        [
            [
                Schema.MBO,
                Encoding.DBZ,
                Compression.ZSTD,
                "mbo.dbz",
                False,
                "mbo.dbz",
            ],
            [
                Schema.MBO,
                Encoding.CSV,
                Compression.NONE,
                "mbo.csv",
                True,
                "mbo.csv",
            ],
            [
                Schema.MBO,
                Encoding.CSV,
                Compression.ZSTD,
                "mbo.csv.zst",
                False,
                "mbo.csv.zst",
            ],
            [
                Schema.MBO,
                Encoding.CSV,
                Compression.ZSTD,
                "mbo.csv.zst",
                True,
                "mbo.csv",
            ],
            [
                Schema.MBO,
                Encoding.JSON,
                Compression.NONE,
                "mbo.json.raw",
                False,
                "mbo.json.raw",
            ],
            [
                Schema.MBO,
                Encoding.JSON,
                Compression.ZSTD,
                "mbo.json.zst",
                False,
                "mbo.json.zst",
            ],
            [
                Schema.MBO,
                Encoding.JSON,
                Compression.ZSTD,
                "mbo.json.zst",
                True,
                "mbo.json.raw",
            ],
        ],
    )
    def test_to_disk_with_various_combinations_persists_to_disk(
        self,
        schema,
        encoding,
        compression,
        stub_data_path,
        decompress,
        expected_path,
    ) -> None:
        # Arrange
        stub_data = get_test_data("test_data." + stub_data_path)

        bento_io = MemoryBento(
            schema=schema,
            encoding=encoding,
            compression=compression,
            initial_bytes=stub_data,
        )

        path = f"test.test_data.{stub_data_path}"

        # Act
        bento_io.to_file(path=path)

        # Assert
        expected = get_test_data("test_data." + expected_path)
        assert os.path.isfile(path)
        assert bento_io.reader(decompress=decompress).read() == expected

        # Cleanup
        os.remove(path)

    def test_to_list_with_stub_data_returns_expected(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.mbo.dbz")

        bento_io = MemoryBento(
            schema=Schema.MBO,
            encoding=Encoding.DBZ,
            compression=Compression.ZSTD,
            initial_bytes=stub_data,
        )

        # Act
        data = bento_io.to_list()

        # Assert
        assert (
            str(data)
            == "[(647439984644, 1, 310, 5482, 1609099225061045683, 315950000000000, 2, b'B', 0, b'B', b'A', 1609099225250461359, 92701, 1098)\n (647439984689, 1, 310, 5482, 1609099225061045683, 310550000000000, 3, b'B', 0, b'B', b'A', 1609099225250461359, 92701, 1098)]"  # noqa
        )

    def test_replay_with_stub_bin_record_passes_to_callback(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.mbo.dbz")

        handler = []
        bento_io = MemoryBento(
            schema=Schema.MBO,
            encoding=Encoding.DBZ,
            compression=Compression.ZSTD,
            initial_bytes=stub_data,
        )

        # Act
        bento_io.replay(callback=handler.append)

        # Assert
        assert (
            str(handler[0])
            == "(647439984644, 1, 310, 5482, 1609099225061045683, 315950000000000, 2, b'B', 0, b'B', b'A', 1609099225250461359, 92701, 1098)"  # noqa
        )  # noqa

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
    def test_to_df_across_all_encodings_returns_identical_dfs(self, schema) -> None:
        # Arrange
        stub_data_bin = get_test_data(f"test_data.{schema.value}.dbz")
        stub_data_csv = get_test_data(f"test_data.{schema.value}.csv.zst")
        stub_data_json = get_test_data(f"test_data.{schema.value}.json.zst")

        bento_io_bin = MemoryBento(
            schema=schema,
            encoding=Encoding.DBZ,
            compression=Compression.ZSTD,
            initial_bytes=stub_data_bin,
        )

        bento_io_csv = MemoryBento(
            schema=schema,
            encoding=Encoding.CSV,
            compression=Compression.ZSTD,
            initial_bytes=stub_data_csv,
        )

        bento_io_json = MemoryBento(
            schema=schema,
            encoding=Encoding.JSON,
            compression=Compression.ZSTD,
            initial_bytes=stub_data_json,
        )

        # Act
        df_bin = bento_io_bin.to_df()
        df_csv = bento_io_csv.to_df()
        df_json = bento_io_json.to_df()

        # Assert
        assert list(df_bin.columns) == list(df_csv.columns)
        assert len(df_bin) == 2
        assert len(df_csv) == 2
        assert len(df_json) == 2

    def test_to_df_with_mbo_compressed_record_returns_expected(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.mbo.csv.zst")

        bento_io = MemoryBento(
            schema=Schema.MBO,
            encoding=Encoding.CSV,
            compression=Compression.ZSTD,
            initial_bytes=stub_data,
        )

        # Act
        data = bento_io.to_df()

        # Assert
        assert len(data) == 2
        assert data.index.name == "ts_recv"
        assert data.index.values[0] == 1609099225250461359
        assert data.iloc[0].ts_event == 1609099225061045683
        assert data.iloc[0].pub_id == 1
        assert data.iloc[0].product_id == 5482
        assert data.iloc[0].order_id == 647439984644
        assert data.iloc[0].action == "A"
        assert data.iloc[0].side == "B"
        assert data.iloc[0].price == 315950000000000
        assert data.iloc[0].size == 11
        assert data.iloc[0].sequence == 1098

    def test_to_df_with_stub_ohlcv_record_returns_expected(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.ohlcv-1m.csv.zst")

        bento_io = MemoryBento(
            schema=Schema.OHLCV_1H,
            encoding=Encoding.CSV,
            compression=Compression.ZSTD,
            initial_bytes=stub_data,
        )

        # Act
        data = bento_io.to_df()

        # Assert
        assert len(data) == 2
        assert data.index.name == "ts_event"
        assert data.index.values[0] == 1609110000000000000
        assert data.iloc[0].product_id == 5482
        assert data.iloc[0].open == 368200000000000
        assert data.iloc[0].high == 368725000000000
        assert data.iloc[0].low == 367600000000000
        assert data.iloc[0].close == 368650000000000
        assert data.iloc[0].volume == 2312

    def test_to_df_with_pretty_ts_converts_timestamps_as_expected(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.mbo.dbz")

        bento_io = MemoryBento(
            schema=Schema.MBO,
            encoding=Encoding.DBZ,
            compression=Compression.ZSTD,
            initial_bytes=stub_data,
        )

        # Act
        data = bento_io.to_df(pretty_ts=True)

        # Assert
        index0 = data.index[0]
        event0 = data["ts_event"][0]
        assert isinstance(index0, pd.Timestamp)
        assert isinstance(event0, pd.Timestamp)
        assert index0 == pd.Timestamp("2020-12-27 20:00:25.250461359+0000", tz="UTC")
        assert event0 == pd.Timestamp("2020-12-27 20:00:25.061045683+0000", tz="UTC")
        assert len(data) == 2

    @pytest.mark.parametrize(
        "schema,columns",
        [
            [Schema.MBO, ["price"]],
            [Schema.TBBO, ["price", "bid_px_00", "ask_px_00"]],
            [Schema.MBP_1, ["price", "bid_px_00", "ask_px_00"]],
            [
                Schema.MBP_5,
                [
                    "price",
                    "bid_px_00",
                    "bid_px_01",
                    "bid_px_02",
                    "bid_px_03",
                    "bid_px_04",
                    "ask_px_00",
                    "ask_px_01",
                    "ask_px_02",
                    "ask_px_03",
                    "ask_px_04",
                ],
            ],
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
        stub_data = get_test_data(f"test_data.{schema.value}.dbz")

        bento_io = MemoryBento(
            schema=schema,
            encoding=Encoding.DBZ,
            compression=Compression.ZSTD,
            initial_bytes=stub_data,
        )

        # Act
        data = bento_io.to_df(pretty_px=True)

        # Assert
        assert len(data) == 2
        for column in columns:
            assert isinstance(data[column].iloc(0)[1], float)
        # TODO(cs): Check float values once display factor fixed


class TestFileBento:
    @pytest.mark.parametrize(
        "schema, data_path, expected_encoding, expected_compression",
        [
            [Schema.MBO, "mbo.dbz", Encoding.DBZ, Compression.ZSTD],
            [Schema.MBO, "mbo.csv", Encoding.CSV, Compression.NONE],
            [Schema.MBO, "mbo.csv.zst", Encoding.CSV, Compression.ZSTD],
            [Schema.MBO, "mbo.json.raw", Encoding.JSON, Compression.NONE],
            [Schema.MBO, "mbo.json.zst", Encoding.JSON, Compression.ZSTD],
        ],
    )
    def test_inference(
        self,
        schema,
        data_path,
        expected_encoding,
        expected_compression,
    ) -> None:
        # Arrange
        path = os.path.join(TESTS_ROOT, "data", "test_data." + data_path)
        stub_data = get_test_data("test_data." + data_path)

        # Act
        bento_io = FileBento(path=path, schema=Schema.MBO)

        # Assert
        assert bento_io.encoding == expected_encoding
        assert bento_io.compression == expected_compression
        assert bento_io.raw == stub_data  # Ensure stream position hasn't moved

    def test_disk_io_bin_without_compression(self) -> None:
        # Arrange
        path = os.path.join(TESTS_ROOT, "data/test_data.mbo.dbz")
        stub_data = get_test_data("test_data.mbo.dbz")
        bento_io = FileBento(
            path=path,
            schema=Schema.MBO,
            encoding=Encoding.DBZ,
            compression=Compression.ZSTD,
        )

        # Act
        data = bento_io.raw

        # Assert
        assert data == stub_data
        assert len(bento_io.to_list()) == 2

    def test_disk_io_bin_with_compression(self) -> None:
        # Arrange
        path = os.path.join(TESTS_ROOT, "data/test_data.mbo.dbz")
        stub_data = get_test_data("test_data.mbo.dbz")

        bento_io = FileBento(
            path=path,
            schema=Schema.MBO,
            encoding=Encoding.DBZ,
            compression=Compression.ZSTD,
        )

        # Act
        data = bento_io.raw

        # Assert
        assert data == stub_data
        assert len(bento_io.to_list()) == 2

    def test_disk_io_csv_compressed(self) -> None:
        # Arrange
        path = os.path.join(TESTS_ROOT, "data/test_data.mbo.csv.zst")
        stub_data = get_test_data("test_data.mbo.csv.zst")

        bento_io = FileBento(
            path=path,
            schema=Schema.MBO,
            encoding=Encoding.CSV,
            compression=Compression.ZSTD,
        )

        # Act
        data = bento_io.raw

        # Assert
        assert data == stub_data
        assert len(bento_io.to_list()) == 3  # includes header
        assert len(bento_io.to_df()) == 2

    def test_disk_io_csv_uncompressed(self) -> None:
        # Arrange
        path = os.path.join(TESTS_ROOT, "data/test_data.mbo.csv")
        stub_data = get_test_data("test_data.mbo.csv")

        bento_io = FileBento(
            path=path,
            schema=Schema.MBO,
            encoding=Encoding.CSV,
            compression=Compression.NONE,
        )

        # Act
        data = bento_io.raw

        # Assert
        assert data == stub_data
        assert len(bento_io.to_list()) == 3  # includes header
        assert len(bento_io.to_df()) == 2
