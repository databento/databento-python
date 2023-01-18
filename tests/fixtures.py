import os

from databento.common.enums import Schema


TESTS_ROOT = os.path.dirname(os.path.abspath(__file__))


def get_test_data_path(schema: Schema) -> str:
    return os.path.join(TESTS_ROOT, "data", f"test_data.{schema}.dbn.zst")


def get_test_data(schema: Schema) -> bytes:
    with open(get_test_data_path(schema=schema), "rb") as f:
        return f.read()
