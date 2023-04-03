import sys
from typing import Type
from unittest.mock import MagicMock

import aiohttp
import pytest
import requests
from databento.common.error import BentoClientError, BentoServerError
from databento.historical.http import check_http_error, check_http_error_async


@pytest.mark.parametrize(
    "status_code, expected_exception, message",
    [
        pytest.param(404, BentoClientError, r"", id="404"),
        pytest.param(408, BentoClientError, r"timed out.$", id="408"),
        pytest.param(500, BentoServerError, r"", id="500"),
        pytest.param(504, BentoServerError, r"timed out.$", id="504"),
    ],
)
def test_check_http_status(
    status_code: int,
    expected_exception: Type[Exception],
    message: str,
) -> None:
    """
    Test that responses with the given status code raise the expected exception.
    """
    response = requests.Response()
    response.status_code = status_code
    with pytest.raises(expected_exception) as exc:
        check_http_error(response)

    exc.match(message)


@pytest.mark.asyncio
@pytest.mark.skipif(sys.version_info < (3, 8), reason="no async support for MagicMock")
@pytest.mark.parametrize(
    "status_code, expected_exception, message",
    [
        pytest.param(404, BentoClientError, r"", id="404"),
        pytest.param(408, BentoClientError, r"timed out.$", id="408"),
        pytest.param(500, BentoServerError, r"", id="500"),
        pytest.param(504, BentoServerError, r"timed out.$", id="504"),
    ],
)
async def test_check_http_status_async(
    status_code: int,
    expected_exception: Type[Exception],
    message: str,
) -> None:
    """
    Test that responses with the given status code raise the expected exception.
    """
    response = MagicMock(spec=aiohttp.ClientResponse)
    response.status = status_code
    with pytest.raises(expected_exception) as exc:
        await check_http_error_async(response)

    exc.match(message)


def test_client_error_str_and_repr() -> None:
    # Arrange, Act
    error = BentoClientError(
        http_status=400,
        http_body=None,
        message="Bad Request",
    )

    # Assert
    assert str(error) == "400 Bad Request"
    assert (
        repr(error)
        == "BentoClientError(request_id=None, http_status=400, message=Bad Request)"  # noqa
    )


def test_server_error_str_and_repr() -> None:
    # Arrange, Act
    error = BentoServerError(
        http_status=500,
        http_body=None,
        message="Internal Server Error",
    )

    # Assert
    assert str(error) == "500 Internal Server Error"
    assert (
        repr(error)
        == "BentoServerError(request_id=None, http_status=500, message=Internal Server Error)"  # noqa
    )
