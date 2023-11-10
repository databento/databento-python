import json

import pytest
from databento.historical.http import check_backend_warnings
from requests import Response


@pytest.mark.parametrize(
    "header_field",
    [
        "X-Warning",
        "x-warning",
    ],
)
@pytest.mark.parametrize(
    "category, message, expected_category",
    [
        pytest.param("Warning", "this is a test", "BentoWarning"),
        pytest.param(
            "DeprecationWarning",
            "you're too old!",
            "BentoDeprecationWarning",
        ),
        pytest.param("Warning", "edge: case", "BentoWarning"),
        pytest.param("UnknownWarning", "", "BentoWarning"),
    ],
)
def test_backend_warning(
    header_field: str,
    category: str,
    message: str,
    expected_category: str,
) -> None:
    """
    Test that a backend warning in a response header is correctly parsed as a
    type of BentoWarning.
    """
    # Arrange
    response = Response()
    expected = f'["{category}: {message}"]'
    response.headers[header_field] = expected

    # Act
    with pytest.warns() as warnings:
        check_backend_warnings(response)

    # Assert
    assert len(warnings) == 1
    assert warnings.list[0].category.__name__ == expected_category
    assert str(warnings.list[0].message) == message


@pytest.mark.parametrize(
    "header_field",
    [
        "X-Warning",
        "x-warning",
    ],
)
def test_multiple_backend_warning(
    header_field: str,
) -> None:
    """
    Test that multiple backend warnings in a response header are supported.
    """
    # Arrange
    response = Response()
    backend_warnings = [
        "Warning: this is a test",
        "DeprecationWarning: you're too old!",
    ]
    response.headers[header_field] = json.dumps(backend_warnings)

    # Act
    with pytest.warns() as warnings:
        check_backend_warnings(response)

    # Assert
    assert len(warnings) == len(backend_warnings)
    assert warnings.list[0].category.__name__ == "BentoWarning"
    assert str(warnings.list[0].message) == "this is a test"
    assert warnings.list[1].category.__name__ == "BentoDeprecationWarning"
    assert str(warnings.list[1].message) == "you're too old!"
