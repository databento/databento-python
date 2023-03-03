import pytest
from databento.common.bento import FileDataSource, MemoryDataSource
from databento.common.enums import Schema

from tests.fixtures import get_test_data, get_test_data_path


@pytest.mark.parametrize("schema", [pytest.param(x) for x in Schema])
def test_memory_data_source(
    schema: Schema,
) -> None:
    """Test create of MemoryDataSource"""
    if schema in (
        Schema.STATUS,
        Schema.STATISTICS,
        Schema.SYMBOL_MAPPING,
        Schema.GATEWAY_ERROR,
    ):
        pytest.skip(f"untested schema {schema}")

    data = get_test_data(schema)
    data_source = MemoryDataSource(data)

    assert len(data) == data_source.nbytes
    assert repr(data) == data_source.name


@pytest.mark.parametrize("schema", [pytest.param(x) for x in Schema])
def test_file_data_source(
    schema: Schema,
) -> None:
    """Test create of FileDataSource"""
    if schema in (
        Schema.STATUS,
        Schema.STATISTICS,
        Schema.SYMBOL_MAPPING,
        Schema.GATEWAY_ERROR,
    ):
        pytest.skip(f"untested schema {schema}")

    path = get_test_data_path(schema)
    data_source = FileDataSource(path)

    assert path.stat().st_size == data_source.nbytes
    assert path.name == data_source.name
