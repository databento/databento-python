from __future__ import annotations

import pathlib
import sys
import sysconfig
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest
from databento_dbn import DBNDecoder
from databento_dbn import Schema

import databento
from databento.common.publishers import Dataset


pytestmark = pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"),
    reason="requires a free-threaded interpreter",
)

N_THREADS = 8


def test_import_keeps_gil_disabled() -> None:
    # Arrange, Act
    assert databento is not None

    # Assert
    assert sys._is_gil_enabled() is False  # type: ignore[attr-defined]


def test_concurrent_dbnstore_to_df(
    test_data_path: Callable[[Dataset, Schema], pathlib.Path],
) -> None:
    # Arrange
    path = test_data_path(Dataset.GLBX_MDP3, Schema.MBO)
    expected = databento.DBNStore.from_file(path).to_df()
    barrier = threading.Barrier(N_THREADS)

    def decode(_: int) -> pd.DataFrame:
        barrier.wait()
        return databento.DBNStore.from_file(path).to_df()

    # Act
    with ThreadPoolExecutor(max_workers=N_THREADS) as pool:
        results = list(pool.map(decode, range(N_THREADS)))

    # Assert
    assert len(results) == N_THREADS
    for actual in results:
        pd.testing.assert_frame_equal(actual, expected)


def test_concurrent_dbnstore_iteration(
    test_data_path: Callable[[Dataset, Schema], pathlib.Path],
) -> None:
    # Arrange
    path = test_data_path(Dataset.GLBX_MDP3, Schema.MBO)
    expected = [bytes(record) for record in databento.DBNStore.from_file(path)]
    barrier = threading.Barrier(N_THREADS)

    def iterate(_: int) -> list[bytes]:
        barrier.wait()
        return [bytes(record) for record in databento.DBNStore.from_file(path)]

    # Act
    with ThreadPoolExecutor(max_workers=N_THREADS) as pool:
        results = list(pool.map(iterate, range(N_THREADS)))

    # Assert
    assert expected
    for actual in results:
        assert actual == expected


def test_shared_decoder_raises_or_succeeds(
    test_data_path: Callable[[Dataset, Schema], pathlib.Path],
) -> None:
    # Arrange
    store = databento.DBNStore.from_file(test_data_path(Dataset.GLBX_MDP3, Schema.MBO))
    decoder = DBNDecoder()
    decoder.write(store.reader.read())
    records = decoder.decode()
    record_bytes = b"".join(bytes(record) for record in records[1:])
    records_per_write = len(records) - 1
    assert records_per_write > 0

    counts = {"written": 0, "decoded": 0}
    counts_lock = threading.Lock()
    barrier = threading.Barrier(N_THREADS)

    def hammer(_: int) -> None:
        barrier.wait()
        for _ in range(20):
            try:
                decoder.write(record_bytes)
                with counts_lock:
                    counts["written"] += 1
            except RuntimeError:
                pass
            try:
                decoded = len(decoder.decode())
                with counts_lock:
                    counts["decoded"] += decoded
            except RuntimeError:
                pass

    # Act
    with ThreadPoolExecutor(max_workers=N_THREADS) as pool:
        list(pool.map(hammer, range(N_THREADS)))
    counts["decoded"] += len(decoder.decode())

    # Assert
    assert counts["written"] > 0
    assert counts["decoded"] == counts["written"] * records_per_write
