import os
from pathlib import Path

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
    def test_from_dbz_file_when_not_exists_raises_expected_exception(self):
        # Arrange, Act, Assert
        with pytest.raises(FileNotFoundError):
            from_file("my_data.dbz")

    def test_from_dbz_file_when_file_empty_raises_expected_exception(self):
        # Arrange
        path = "my_data.dbz"
        Path(path).touch()

        # Act, Assert
        with pytest.raises(RuntimeError):
            from_file(path)

        # Cleanup
        os.remove(path)

    def test_dataset_when_metadata_not_initialized_raises_runtime_error(self):
        # Arrange
        bento = Bento()

        # Act, Assert
        with pytest.raises(RuntimeError):
            bento.dataset

    def test_shape_when_metadata_initialized_returns_expected_tuple(self):
        # Arrange
        metadata = {
            "dataset": "GLBX.MDP3",
            "schema": "mbo",
            "encoding": "dbz",
            "compression": "zstd",
            "nrows": 1000,
            "ncols": 10,
        }

        bento = Bento()
        bento.set_metadata(metadata)

        # Act, Assert
        assert bento.shape == (1000, 10)


class TestMemoryBento:
    def test_memory_io_nbytes(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.mbo.dbz")

        # Act
        bento = MemoryBento(initial_bytes=stub_data)

        # Assert
        assert bento.nbytes == 386

    def test_disk_io_nbytes(self) -> None:
        # Arrange, Act
        path = os.path.join(TESTS_ROOT, "data", "test_data.mbo.dbz")
        bento = FileBento(path=path)

        # Assert
        assert bento.nbytes == 386

    @pytest.mark.parametrize(
        "stub_data_path, " "decompress, " "expected_path",
        [
            [
                "mbo.dbz",
                False,
                "mbo.dbz",
            ],
        ],
    )
    def test_to_disk_with_various_combinations_persists_to_disk(
        self,
        stub_data_path,
        decompress,
        expected_path,
    ) -> None:
        # Arrange
        stub_data = get_test_data("test_data." + stub_data_path)

        bento = MemoryBento(initial_bytes=stub_data)

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

        metadata = {
            "dataset": "GLBX.MDP3",
            "schema": "mbo",
            "encoding": "dbz",
            "compression": "zstd",
        }
        bento = MemoryBento(initial_bytes=stub_data)
        bento.set_metadata(metadata)

        # Act
        data = bento.to_ndarray()

        # Assert
        assert (
            str(data)
            == "[(14, 32, 1, 5482, 1609160400000429831, 647784973705, 372275000000000, 1, -128, 0, b'A', b'C', 1609160400000704060, 22993, 1170352)\n (14, 32, 1, 5482, 1609160400000431665, 647784973631, 372300000000000, 1, -128, 0, b'A', b'C', 1609160400000711344, 19621, 1170353)]"  # noqa
        )

    def test_replay_with_stub_dbz_record_passes_to_callback(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.mbo.dbz")

        metadata = {
            "dataset": "GLBX.MDP3",
            "schema": "mbo",
            "encoding": "dbz",
            "compression": "zstd",
        }

        handler = []
        bento = MemoryBento(initial_bytes=stub_data)
        bento.set_metadata(metadata)

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
        stub_data_dbz = get_test_data(f"test_data.{schema.value}.dbz")
        bento = MemoryBento(initial_bytes=stub_data_dbz)

        metadata = bento.source_metadata()
        bento.set_metadata(metadata)

        # Act
        df = bento.to_df()

        # Assert
        assert list(df.columns) == list(df.columns)
        assert len(df) == 2

    def test_to_df_with_mbo_compressed_record_returns_expected(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.mbo.dbz")

        metadata = {
            "dataset": "GLBX.MDP3",
            "schema": "mbo",
            "encoding": "dbz",
            "compression": "zstd",
        }

        bento = MemoryBento(initial_bytes=stub_data)
        bento.set_metadata(metadata)

        # Act
        data = bento.to_df()

        # Assert
        assert len(data) == 2
        assert data.index.name == "ts_recv"
        assert data.index.values[0] == 1609160400000704060
        assert data.iloc[0].ts_event == 1609160400000429831
        assert data.iloc[0].pub_id == 1
        assert data.iloc[0].product_id == 5482
        assert data.iloc[0].order_id == 647784973705
        assert data.iloc[0].action == "A"  # TODO(cs): Invalid until data regenerated
        assert data.iloc[0].side == "C"  # TODO(cs): Invalid until data regenerated
        assert data.iloc[0].price == 372275000000000
        assert data.iloc[0].size == 11
        assert data.iloc[0].sequence == 1170352

    def test_to_df_with_stub_ohlcv_record_returns_expected(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.ohlcv-1m.dbz")

        bento = MemoryBento(initial_bytes=stub_data)

        metadata = bento.source_metadata()
        bento.set_metadata(metadata)

        # Act
        data = bento.to_df()

        # Assert
        assert len(data) == 2
        assert data.index.name == "ts_event"
        assert data.index.values[0] == 1609160400000000000
        assert data.iloc[0].product_id == 5482
        assert data.iloc[0].open == 372025000000000
        assert data.iloc[0].high == 372150000000000
        assert data.iloc[0].low == 372025000000000
        assert data.iloc[0].close == 372100000000000
        assert data.iloc[0].volume == 353

    def test_to_df_with_pretty_ts_converts_timestamps_as_expected(self) -> None:
        # Arrange
        stub_data = get_test_data("test_data.mbo.dbz")

        metadata = {
            "dataset": "GLBX.MDP3",
            "schema": "mbo",
            "encoding": "dbz",
            "compression": "zstd",
        }

        bento = MemoryBento(initial_bytes=stub_data)
        bento.set_metadata(metadata)

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

        bento = MemoryBento(initial_bytes=stub_data)

        metadata = bento.source_metadata()
        bento.set_metadata(metadata)

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
        bento = from_file(path=path)

        # Assert
        assert bento.schema == expected_schema
        assert bento.encoding == expected_encoding
        assert bento.compression == expected_compression

    def test_file_bento_given_path(self) -> None:
        # Arrange
        path = os.path.join(TESTS_ROOT, "data/test_data.mbo.dbz")
        stub_data = get_test_data("test_data.mbo.dbz")

        bento = FileBento(path=path)

        metadata = bento.source_metadata()
        bento.set_metadata(metadata)

        # Act
        data = bento.raw

        # Assert
        assert data == stub_data
        assert len(bento.to_ndarray()) == 2
