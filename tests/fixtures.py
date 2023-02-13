import pathlib

from databento.common.enums import Schema


TESTS_ROOT = pathlib.Path(__file__).absolute().parent


def get_test_data_path(schema: Schema) -> pathlib.Path:
    return pathlib.Path(TESTS_ROOT) / "data" / f"test_data.{schema}.dbn.zst"


def get_test_data(schema: Schema) -> bytes:
    with open(get_test_data_path(schema=schema), "rb") as f:
        return f.read()
