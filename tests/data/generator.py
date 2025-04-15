"""
Utility to generate stub data for testing purposes.
"""

from __future__ import annotations

import argparse
import asyncio
import pathlib
import sys
import warnings
from typing import Final

import databento as db
from databento.common.publishers import Dataset
from databento_dbn import Schema

from tests import TESTS_ROOT


warnings.simplefilter("ignore")

STUB_DATA_PATH: Final = TESTS_ROOT / "data"

LIMIT: Final = 4

MANIFEST: Final = {
    Dataset.GLBX_MDP3: [
        (Schema.MBO, ["ESH1"], "2020-12-28"),
        (Schema.MBP_1, ["ESH1"], "2020-12-28"),
        (Schema.MBP_10, ["ESH1"], "2020-12-28"),
        (Schema.BBO_1S, ["ESH1"], "2020-12-28"),
        (Schema.BBO_1M, ["ESH1"], "2020-12-28"),
        (Schema.TBBO, ["ESH1"], "2020-12-28"),
        (Schema.TRADES, ["ESH1"], "2020-12-28"),
        (Schema.OHLCV_1S, ["ESH1"], "2020-12-28"),
        (Schema.OHLCV_1M, ["ESH1"], "2020-12-28"),
        (Schema.OHLCV_1H, ["ESH1"], "2020-12-28"),
        (Schema.OHLCV_1D, ["ESH1"], "2020-12-28"),
        (Schema.DEFINITION, ["ESH1"], "2020-12-28"),
        (Schema.STATISTICS, ["ESH1"], "2020-12-28"),
        (Schema.STATUS, ["ESH1"], "2020-12-28"),
    ],
    Dataset.XNAS_ITCH: [
        (Schema.MBO, ["NVDA"], "2020-12-28"),
        (Schema.MBP_1, ["NVDA"], "2020-12-28"),
        (Schema.MBP_10, ["NVDA"], "2020-12-28"),
        (Schema.TBBO, ["NVDA"], "2020-12-28"),
        (Schema.TRADES, ["NVDA"], "2020-12-28"),
        (Schema.OHLCV_1S, ["NVDA"], "2020-12-28"),
        (Schema.OHLCV_1M, ["NVDA"], "2020-12-28"),
        (Schema.OHLCV_1H, ["NVDA"], "2020-12-28"),
        (Schema.OHLCV_1D, ["NVDA"], "2020-12-28"),
        (Schema.DEFINITION, ["NVDA"], "2020-12-28"),
        (Schema.IMBALANCE, ["NVDA"], "2020-12-28"),
    ],
    Dataset.OPRA_PILLAR: [
        (Schema.MBP_1, ["AAPL  250221C00250000"], "2025-02-20"),
        (Schema.TBBO, ["AAPL  250221C00250000"], "2025-02-20"),
        (Schema.TRADES, ["AAPL  250221C00250000"], "2025-02-20"),
        (Schema.CBBO_1S, ["AAPL  250221C00250000"], "2025-02-20"),
        (Schema.CBBO_1M, ["AAPL  250221C00250000"], "2025-02-20"),
        (Schema.OHLCV_1S, ["AAPL  250221C00250000"], "2025-02-20"),
        (Schema.OHLCV_1M, ["AAPL  250221C00250000"], "2025-02-20"),
        (Schema.OHLCV_1H, ["AAPL  250221C00250000"], "2025-02-20"),
        (Schema.OHLCV_1D, ["AAPL  250221C00250000"], "2025-02-20"),
        (Schema.DEFINITION, ["AAPL  250221C00250000"], "2025-02-20"),
        (Schema.STATISTICS, ["AAPL  250221C00250000"], "2025-02-20"),
    ],
    Dataset.EQUS_MINI: [
        (Schema.MBP_1, ["QQQ"], "2023-03-28"),
        (Schema.TBBO, ["QQQ"], "2023-03-28"),
        (Schema.TRADES, ["QQQ"], "2023-03-28"),
        (Schema.BBO_1S, ["QQQ"], "2023-03-28"),
        (Schema.BBO_1M, ["QQQ"], "2023-03-28"),
        (Schema.OHLCV_1S, ["QQQ"], "2023-03-28"),
        (Schema.OHLCV_1M, ["QQQ"], "2023-03-28"),
        (Schema.OHLCV_1H, ["QQQ"], "2023-03-28"),
        (Schema.OHLCV_1D, ["QQQ"], "2023-03-28"),
        (Schema.DEFINITION, ["QQQ"], "2023-03-28"),
    ],
    Dataset.IFEU_IMPACT: [
        (Schema.MBO, ["BRN FMJ0024!"], "2024-01-18"),
        (Schema.MBP_1, ["BRN FMJ0024!"], "2024-01-18"),
        (Schema.MBP_10, ["BRN FMJ0024!"], "2024-01-18"),
        (Schema.BBO_1S, ["BRN FMJ0024!"], "2024-01-18"),
        (Schema.BBO_1M, ["BRN FMJ0024!"], "2024-01-18"),
        (Schema.TBBO, ["BRN FMJ0024!"], "2024-01-18"),
        (Schema.TRADES, ["BRN FMJ0024!"], "2024-01-18"),
        (Schema.OHLCV_1S, ["BRN FMJ0024!"], "2024-01-18"),
        (Schema.OHLCV_1M, ["BRN FMJ0024!"], "2024-01-18"),
        (Schema.OHLCV_1H, ["BRN FMJ0024!"], "2024-01-18"),
        (Schema.OHLCV_1D, ["BRN FMJ0024!"], "2024-01-18"),
        (Schema.DEFINITION, ["BRN FMJ0024!"], "2024-01-18"),
        (Schema.STATISTICS, ["BRN FMJ0024!"], "2024-01-18"),
    ],
    Dataset.NDEX_IMPACT: [
        (Schema.MBO, ["TFM FMH0024!"], "2024-01-18"),
        (Schema.MBP_1, ["TFM FMH0024!"], "2024-01-18"),
        (Schema.MBP_10, ["TFM FMH0024!"], "2024-01-18"),
        (Schema.BBO_1S, ["TFM FMH0024!"], "2024-01-18"),
        (Schema.BBO_1M, ["TFM FMH0024!"], "2024-01-18"),
        (Schema.TBBO, ["TFM FMH0024!"], "2024-01-18"),
        (Schema.TRADES, ["TFM FMH0024!"], "2024-01-18"),
        (Schema.OHLCV_1S, ["TFM FMH0024!"], "2024-01-18"),
        (Schema.OHLCV_1M, ["TFM FMH0024!"], "2024-01-18"),
        (Schema.OHLCV_1H, ["TFM FMH0024!"], "2024-01-18"),
        (Schema.OHLCV_1D, ["TFM FMH0024!"], "2024-01-18"),
        (Schema.DEFINITION, ["TFM FMH0024!"], "2024-01-18"),
        (Schema.STATISTICS, ["TFM FMH0024!"], "2024-01-18"),
    ],
}


async def generate_stub_data(
    regenerate: bool = False,
    dataset_select: Dataset | None = None,
) -> None:
    client = db.Historical()

    tasks = []
    for dataset, specifications in MANIFEST.items():
        if dataset_select is not None and dataset != dataset_select:
            print(f"skipping dataset {dataset}")
            continue

        dataset_dir = STUB_DATA_PATH / dataset
        dataset_dir.mkdir(parents=True, exist_ok=True)
        for spec in specifications:
            schema, symbols, start = spec
            path = dataset_dir / f"test_data.{schema}.dbn.zst"
            if path.exists():
                if not regenerate:
                    continue
                path.unlink()

            tasks.append(
                asyncio.create_task(
                    client.timeseries.get_range_async(
                        dataset=dataset,
                        schema=schema,
                        symbols=symbols,
                        start=start,
                        limit=LIMIT,
                        path=path,
                    ),
                ),
            )

    print(f"generating {len(tasks)} stub files", end="...", flush=True)
    stores = await asyncio.gather(*tasks)
    print("done")

    # Check for empty stubs
    for store in stores:
        num_records = len(list(store))
        if num_records == 0:
            print(
                f"WARNING: {store.dataset} stub file {store._data_source.name} has no records!",
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=f"Generate DBN stub data to: ./{STUB_DATA_PATH.relative_to(pathlib.Path.cwd())}",
    )
    parser.add_argument(
        "-d",
        "--dataset",
        choices=tuple(str(x.value) for x in MANIFEST),
        default=None,
    )
    parser.add_argument(
        "--regenerate",
        action="store_true",
        dest="regenerate",
        help="remove old stub files to regenerate existing data",
    )

    parsed = parser.parse_args(sys.argv[1:])
    asyncio.run(
        generate_stub_data(
            regenerate=parsed.regenerate,
            dataset_select=parsed.dataset,
        ),
    )
