import pathlib
from typing import Callable

import pytest
from databento.common.dbnstore import FileDataSource
from databento.common.dbnstore import MemoryDataSource
from databento.common.publishers import Dataset
from databento_dbn import Schema


@pytest.mark.parametrize(
    "dataset",
    [
        Dataset.GLBX_MDP3,
        Dataset.XNAS_ITCH,
        Dataset.OPRA_PILLAR,
        Dataset.EQUS_MINI,
        Dataset.IFEU_IMPACT,
        Dataset.NDEX_IMPACT,
    ],
)
@pytest.mark.parametrize("schema", [pytest.param(x) for x in Schema.variants()])
def test_memory_data_source(
    test_data: Callable[[Dataset, Schema], bytes],
    dataset: Dataset,
    schema: Schema,
) -> None:
    """
    Test create of MemoryDataSource.
    """
    # Arrange, Act
    data = test_data(dataset, schema)
    data_source = MemoryDataSource(data)

    # Assert
    assert len(data) == data_source.nbytes
    assert repr(data) == data_source.name


@pytest.mark.parametrize(
    "dataset",
    [
        Dataset.GLBX_MDP3,
        Dataset.XNAS_ITCH,
        Dataset.OPRA_PILLAR,
        Dataset.EQUS_MINI,
        Dataset.IFEU_IMPACT,
        Dataset.NDEX_IMPACT,
    ],
)
@pytest.mark.parametrize("schema", [pytest.param(x) for x in Schema.variants()])
def test_file_data_source(
    test_data_path: Callable[[Dataset, Schema], pathlib.Path],
    dataset: Dataset,
    schema: Schema,
) -> None:
    """
    Test create of FileDataSource.
    """
    # Arrange, Act
    path = test_data_path(dataset, schema)
    data_source = FileDataSource(path)

    # Assert
    assert path.stat().st_size == data_source.nbytes
    assert path.name == data_source.name
