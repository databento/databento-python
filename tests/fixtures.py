import pathlib

from databento.common.enums import Schema


TESTS_ROOT = pathlib.Path(__file__).absolute().parent


def get_test_data_path(schema: Schema) -> pathlib.Path:
    """
    Return the path of stub DBN data for testing.

    Parameters
    ----------
    schema : Schema
        The schema of the stub data path to request.

    See Also
    --------
    get_test_data

    """
    return pathlib.Path(TESTS_ROOT) / "data" / f"test_data.{schema}.dbn.zst"


def get_test_data(schema: Schema) -> bytes:
    """
    Return bytes of stub DBN data for testing.

    Parameters
    ----------
    schema : Schema
        The schema of the stub data to request.

    See Also
    --------
    get_test_data_path

    """
    with open(get_test_data_path(schema=schema), "rb") as f:
        return f.read()
