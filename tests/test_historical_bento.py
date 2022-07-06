import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from databento import from_dbz_file
from databento.common.bento import Bento, FileBento, MemoryBento
from databento.common.enums import Compression, Encoding, Schema


TESTS_ROOT = os.path.dirname(os.path.abspath(__file__))


def get_test_data(file_name):
    with open(os.path.join(TESTS_ROOT, "data", file_name), "rb") as f:
        return f.read()


class TestBento:
    def test_from_dbz_file_when_not_exists_raises_expected_exception(self):
        # Arrange, Act, Assert
        with pytest.raises(FileNotFoundError):
            from_dbz_file("my_data.dbz")

    def test_from_dbz_file_when_file_empty_raises_expected_exception(self):
        # Arrange
        path = "my_data.dbz"
        Path(path).touch()

        # Act, Assert
        with pytest.raises(RuntimeError):
            from_dbz_file(path)

        # Cleanup
        os.remove(path)

    def test_properties_when_instantiated(self) -> None:
        # Arrange
        bento = Bento(
            schema=Schema.MBO,
            encoding=Encoding.CSV,
            compression=Compression.ZSTD,
        )

        # Act, Assert
        assert bento.schema == Schema.MBO
        assert bento.encoding == Encoding.CSV
        assert bento.compression == Compression.ZSTD
        assert bento.struct_fmt == np.dtype(
            [
                ("nwords", "u1"),
                ("type", "u1"),
                ("pub_id", "<u2"),
                ("product_id", "<u4"),
                ("ts_event", "<u8"),
                ("order_id", "<u8"),
                ("price", "<i8"),
                ("size", "<u4"),
                ("flags", "i1"),
                ("chan_id", "u1"),
                ("side", "S1"),
                ("action", "S1"),
                ("ts_recv", "<u8"),
                ("ts_in_delta", "<i4"),
                ("sequence", "<u4"),
            ]
        )
        assert bento.struct_size == 56


class TestMemoryBento:
    def test_memory_io_nbytes(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.mbo.dbz")

        # Act
        bento = MemoryBento(schema=Schema.MBO, initial_bytes=stub_data)

        # Assert
        assert bento.nbytes == 386

    def test_disk_io_nbytes(self) -> None:
        # Arrange, Act
        path = os.path.join(TESTS_ROOT, "data", "test_data.mbo.dbz")
        bento = FileBento(path=path)

        # Assert
        assert bento.nbytes == 386

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

        bento = MemoryBento(
            schema=schema,
            encoding=encoding,
            compression=compression,
            initial_bytes=stub_data,
        )

        path = f"test.test_data.{stub_data_path}"

        # Act
        bento.to_file(path=path)

        # Assert
        expected = get_test_data("test_data." + expected_path)
        assert os.path.isfile(path)
        assert bento.reader(decompress=decompress).read() == expected

        # Cleanup
        os.remove(path)

    def test_to_list_with_stub_data_returns_expected(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.mbo.dbz")

        bento = MemoryBento(
            schema=Schema.MBO,
            encoding=Encoding.DBZ,
            compression=Compression.ZSTD,
            initial_bytes=stub_data,
        )

        # Act
        data = bento.to_list()

        # Assert
        assert (
            str(data)
            == "[(14, 32, 1, 5482, 1609160400000429831, 647784973705, 372275000000000, 1, -128, 0, b'A', b'C', 1609160400000704060, 22993, 1170352)\n (14, 32, 1, 5482, 1609160400000431665, 647784973631, 372300000000000, 1, -128, 0, b'A', b'C', 1609160400000711344, 19621, 1170353)]"  # noqa
        )

    def test_replay_with_stub_dbz_record_passes_to_callback(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.mbo.dbz")

        handler = []
        bento = MemoryBento(
            schema=Schema.MBO,
            encoding=Encoding.DBZ,
            compression=Compression.ZSTD,
            initial_bytes=stub_data,
        )

        # Act
        bento.replay(callback=handler.append)

        # Assert
        assert (
            str(handler[0])
            == "(14, 32, 1, 5482, 1609160400000429831, 647784973705, 372275000000000, 1, -128, 0, b'A', b'C', 1609160400000704060, 22993, 1170352)"  # noqa
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
    def test_to_df_across_all_encodings_returns_identical_dfs(self, schema) -> None:
        # Arrange
        stub_data_bin = get_test_data(f"test_data.{schema.value}.dbz")
        stub_data_csv = get_test_data(f"test_data.{schema.value}.csv.zst")
        stub_data_json = get_test_data(f"test_data.{schema.value}.json.zst")

        bento_bin = MemoryBento(
            schema=schema,
            encoding=Encoding.DBZ,
            compression=Compression.ZSTD,
            initial_bytes=stub_data_bin,
        )

        bento_csv = MemoryBento(
            schema=schema,
            encoding=Encoding.CSV,
            compression=Compression.ZSTD,
            initial_bytes=stub_data_csv,
        )

        bento_json = MemoryBento(
            schema=schema,
            encoding=Encoding.JSON,
            compression=Compression.ZSTD,
            initial_bytes=stub_data_json,
        )

        # Act
        df_bin = bento_bin.to_df()
        df_csv = bento_csv.to_df()
        df_json = bento_json.to_df()

        # Assert
        assert list(df_bin.columns) == list(df_csv.columns)
        assert len(df_bin) == 2
        assert len(df_csv) == 2
        assert len(df_json) == 2

    def test_to_df_with_mbo_compressed_record_returns_expected(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.mbo.csv.zst")

        bento = MemoryBento(
            schema=Schema.MBO,
            encoding=Encoding.CSV,
            compression=Compression.ZSTD,
            initial_bytes=stub_data,
        )

        # Act
        data = bento.to_df()

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

        bento = MemoryBento(
            schema=Schema.OHLCV_1H,
            encoding=Encoding.CSV,
            compression=Compression.ZSTD,
            initial_bytes=stub_data,
        )

        # Act
        data = bento.to_df()

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

        bento = MemoryBento(
            schema=Schema.MBO,
            encoding=Encoding.DBZ,
            compression=Compression.ZSTD,
            initial_bytes=stub_data,
        )

        # Act
        data = bento.to_df(pretty_ts=True)

        # Assert
        index0 = data.index[0]
        event0 = data["ts_event"][0]
        assert isinstance(index0, pd.Timestamp)
        assert isinstance(event0, pd.Timestamp)
        assert index0 == pd.Timestamp("2020-12-28 13:00:00.000704060+0000", tz="UTC")
        assert event0 == pd.Timestamp("2020-12-28 13:00:00.000429831+0000", tz="UTC")
        assert len(data) == 2

    @pytest.mark.parametrize(
        "schema,columns",
        [
            [Schema.MBO, ["price"]],
            [Schema.TBBO, ["price", "bid_px_00", "ask_px_00"]],
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
        stub_data = get_test_data(f"test_data.{schema.value}.dbz")

        bento = MemoryBento(
            schema=schema,
            encoding=Encoding.DBZ,
            compression=Compression.ZSTD,
            initial_bytes=stub_data,
        )

        # Act
        data = bento.to_df(pretty_px=True)

        # Assert
        assert len(data) == 2
        for column in columns:
            assert isinstance(data[column].iloc(0)[1], float)
        # TODO(cs): Check float values once display factor fixed


class TestFileBento:
    @pytest.mark.parametrize(
        "path, expected_schema, expected_encoding, expected_compression",
        [
            ["mbo.dbz", Schema.MBO, Encoding.DBZ, Compression.ZSTD],
            ["mbp-1.dbz", Schema.MBP_1, Encoding.DBZ, Compression.ZSTD],
            ["mbp-10.dbz", Schema.MBP_10, Encoding.DBZ, Compression.ZSTD],
            ["tbbo.dbz", Schema.TBBO, Encoding.DBZ, Compression.ZSTD],
            ["trades.dbz", Schema.TRADES, Encoding.DBZ, Compression.ZSTD],
            ["ohlcv-1s.dbz", Schema.OHLCV_1S, Encoding.DBZ, Compression.ZSTD],
            ["ohlcv-1m.dbz", Schema.OHLCV_1M, Encoding.DBZ, Compression.ZSTD],
            ["ohlcv-1h.dbz", Schema.OHLCV_1H, Encoding.DBZ, Compression.ZSTD],
            ["ohlcv-1d.dbz", Schema.OHLCV_1D, Encoding.DBZ, Compression.ZSTD],
        ],
    )
    def test_from_dbz_file_inference(
        self,
        path,
        expected_schema,
        expected_encoding,
        expected_compression,
    ) -> None:
        # Arrange, Act
        path = os.path.join(TESTS_ROOT, "data", "test_data." + path)
        bento = from_dbz_file(path=path)

        # Assert
        assert bento.schema == expected_schema
        assert bento.encoding == expected_encoding
        assert bento.compression == expected_compression

    def test_file_bento_given_path(self) -> None:
        # Arrange
        path = os.path.join(TESTS_ROOT, "data/test_data.mbo.dbz")
        stub_data = get_test_data("test_data.mbo.dbz")
        bento = FileBento(
            path=path,
            schema=Schema.MBO,
            encoding=Encoding.DBZ,
            compression=Compression.ZSTD,
        )

        # Act
        data = bento.raw

        # Assert
        assert data == stub_data
        assert len(bento.to_list()) == 2

    def test_file_bento_dbz_with_compression(self) -> None:
        # Arrange
        path = os.path.join(TESTS_ROOT, "data/test_data.mbo.dbz")
        stub_data = get_test_data("test_data.mbo.dbz")

        bento = FileBento(
            path=path,
            schema=Schema.MBO,
            encoding=Encoding.DBZ,
            compression=Compression.ZSTD,
        )

        # Act
        data = bento.raw

        # Assert
        assert data == stub_data
        assert len(bento.to_list()) == 2

    def test_file_bento_csv_compressed(self) -> None:
        # Arrange
        path = os.path.join(TESTS_ROOT, "data/test_data.mbo.csv.zst")
        stub_data = get_test_data("test_data.mbo.csv.zst")

        bento = FileBento(
            path=path,
            schema=Schema.MBO,
            encoding=Encoding.CSV,
            compression=Compression.ZSTD,
        )

        # Act
        data = bento.raw

        # Assert
        assert data == stub_data
        assert len(bento.to_list()) == 2  # Does not include header
        assert len(bento.to_df()) == 2

    def test_file_bento_csv_uncompressed(self) -> None:
        # Arrange
        path = os.path.join(TESTS_ROOT, "data/test_data.mbo.csv")
        stub_data = get_test_data("test_data.mbo.csv")

        bento = FileBento(
            path=path,
            schema=Schema.MBO,
            encoding=Encoding.CSV,
            compression=Compression.NONE,
        )

        # Act
        data = bento.raw

        # Assert
        assert data == stub_data
        assert len(bento.to_list()) == 2  # Does not include header
        assert len(bento.to_df()) == 2
