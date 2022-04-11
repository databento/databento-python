from databento.historical.error import BentoClientError, BentoServerError


class TestHistoricalHttpError:
    def test_client_error_str_and_repr(self) -> None:
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

    def test_server_error_str_and_repr(self) -> None:
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
