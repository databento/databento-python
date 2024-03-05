from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class BentoError(Exception):
    """
    Represents a Databento specific error.
    """


class BentoHttpError(BentoError):
    """
    Represents a Databento specific HTTP error.
    """

    def __init__(
        self,
        http_status: int,
        http_body: bytes | str | None = None,
        json_body: dict[str, Any] | None = None,
        message: str | None = None,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)

        if http_body and isinstance(http_body, bytes):
            try:
                http_body = http_body.decode("utf-8")
            except UnicodeDecodeError:
                http_body = (
                    "<Could not decode body as utf-8. Please report to support@databento.com>"
                )

        self.http_status = http_status
        self.http_body = http_body
        self.json_body = json_body

        try:
            json_message = self.json_body["detail"]["message"]
            json_case = self.json_body["detail"]["case"]
            json_docs = self.json_body["detail"]["docs"]
        except (TypeError, KeyError):
            self.message = message
        else:
            composed_message = [
                json_case,
                json_message,
            ]
            if json_docs:
                composed_message.append(f"documentation: {json_docs}")

            self.message = "\n".join(composed_message)

        self.headers = headers or {}
        self.request_id = self.headers.get("request-id", None)

    def __str__(self) -> str:
        msg = self.message or "<empty message>"
        msg = f"{self.http_status} {msg}"
        if self.request_id is not None:
            return f"Request {self.request_id}: {msg}"
        else:
            return msg

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"request_id={self.request_id}, "
            f"http_status={self.http_status}, "
            f"message={self.message})"
        )


class BentoServerError(BentoHttpError):
    """
    Represents a Databento specific server side 500 series HTTP error.
    """

    def __init__(
        self,
        http_status: int,
        http_body: bytes | str | None = None,
        json_body: dict[str, Any] | None = None,
        message: str | None = None,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            http_status=http_status,
            http_body=http_body,
            json_body=json_body,
            message=message,
            headers=headers,
        )


class BentoClientError(BentoHttpError):
    """
    Represents a Databento specific client side 400 series HTTP error.
    """

    def __init__(
        self,
        http_status: int,
        http_body: bytes | str | None = None,
        json_body: dict[str, Any] | None = None,
        message: str | None = None,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            http_status=http_status,
            http_body=http_body,
            json_body=json_body,
            message=message,
            headers=headers,
        )


class BentoWarning(Warning):
    """
    Represents a Databento specific warning.
    """


class BentoDeprecationWarning(BentoWarning):
    """
    Represents a Databento deprecation warning.
    """
