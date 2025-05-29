from __future__ import annotations

import json
import warnings
from collections.abc import Iterable
from collections.abc import Mapping
from io import BytesIO
from os import PathLike
from typing import IO
from typing import Any
from typing import Final

import aiohttp
import requests
from aiohttp import ClientResponse
from aiohttp import ContentTypeError
from requests import JSONDecodeError
from requests import Response
from requests.auth import HTTPBasicAuth

from databento.common.constants import HTTP_STREAMING_READ_SIZE
from databento.common.dbnstore import DBNStore
from databento.common.error import BentoClientError
from databento.common.error import BentoDeprecationWarning
from databento.common.error import BentoError
from databento.common.error import BentoServerError
from databento.common.error import BentoWarning
from databento.common.system import USER_AGENT


WARNING_HEADER_FIELD: Final = "X-Warning"


class BentoHttpAPI:
    """
    The base class for all Databento HTTP API endpoints.
    """

    TIMEOUT = 100

    def __init__(self, key: str, gateway: str):
        self._key = key
        self._gateway = gateway
        self._headers = {"accept": "application/json", "user-agent": USER_AGENT}

    def _check_api_key(self) -> None:
        if self._key == "YOUR_API_KEY":
            raise ValueError(
                "The API key is currently set to 'YOUR_API_KEY'. "
                "Please replace this value with a key from your user portal https://databento.com/portal/keys",
            )

    def _get(
        self,
        url: str,
        params: Iterable[tuple[str, str | None]] | None = None,
        basic_auth: bool = False,
    ) -> Response:
        self._check_api_key()

        with requests.get(
            url=url,
            params=params,
            headers=self._headers,
            auth=HTTPBasicAuth(username=self._key, password="") if basic_auth else None,
            timeout=(self.TIMEOUT, self.TIMEOUT),
        ) as response:
            check_backend_warnings(response)
            check_http_error(response)
            return response

    async def _get_json_async(
        self,
        url: str,
        params: Iterable[tuple[str, str | None]] | None = None,
        basic_auth: bool = False,
    ) -> Any:
        self._check_api_key()
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url=url,
                params=params,
                headers=self._headers,
                auth=(
                    aiohttp.BasicAuth(login=self._key, password="", encoding="utf-8")
                    if basic_auth
                    else None
                ),
                timeout=self.TIMEOUT,
            ) as response:
                check_backend_warnings(response)
                await check_http_error_async(response)
                return await response.json()

    def _post(
        self,
        url: str,
        data: Mapping[str, object | None] | None = None,
        params: Iterable[tuple[str, str | None]] | None = None,
        basic_auth: bool = False,
    ) -> Response:
        self._check_api_key()

        with requests.post(
            url=url,
            data=data,
            params=params,
            headers=self._headers,
            auth=HTTPBasicAuth(username=self._key, password="") if basic_auth else None,
            timeout=(self.TIMEOUT, self.TIMEOUT),
        ) as response:
            check_backend_warnings(response)
            check_http_error(response)
            return response

    def _stream(
        self,
        url: str,
        data: dict[str, object | None],
        basic_auth: bool,
        path: PathLike[str] | str | None = None,
    ) -> DBNStore:
        self._check_api_key()

        with requests.post(
            url=url,
            data=data,
            headers=self._headers,
            auth=HTTPBasicAuth(username=self._key, password="") if basic_auth else None,
            timeout=(self.TIMEOUT, self.TIMEOUT),
            stream=True,
        ) as response:
            check_backend_warnings(response)
            check_http_error(response)

            if path is None:
                writer: IO[bytes] = BytesIO()
            else:
                writer = open(path, "x+b")

            try:
                for chunk in response.iter_content(chunk_size=HTTP_STREAMING_READ_SIZE):
                    writer.write(chunk)
            except Exception as exc:
                raise BentoError(f"Error streaming response: {exc}") from None

            if path is None:
                writer.seek(0)
                return DBNStore.from_bytes(writer)

            writer.close()
            return DBNStore.from_file(path)

    async def _stream_async(
        self,
        url: str,
        data: dict[str, object | None] | None,
        basic_auth: bool,
        path: PathLike[str] | str | None = None,
    ) -> DBNStore:
        self._check_api_key()

        async with aiohttp.ClientSession() as session:
            async with session.post(
                url=url,
                data=data,
                headers=self._headers,
                auth=(
                    aiohttp.BasicAuth(login=self._key, password="", encoding="utf-8")
                    if basic_auth
                    else None
                ),
                timeout=self.TIMEOUT,
            ) as response:
                check_backend_warnings(response)
                await check_http_error_async(response)

                if path is None:
                    writer: IO[bytes] = BytesIO()
                else:
                    writer = open(path, "x+b")

                try:
                    async for chunk in response.content.iter_chunks():
                        writer.write(chunk[0])
                except Exception as exc:
                    raise BentoError(f"Error streaming response: {exc}") from None

                if path is None:
                    writer.seek(0)
                    return DBNStore.from_bytes(writer)

                writer.close()
                return DBNStore.from_file(path)


def is_400_series_error(status: int) -> bool:
    return status // 100 == 4


def is_500_series_error(status: int) -> bool:
    return status // 100 == 5


def check_backend_warnings(response: Response | ClientResponse) -> None:
    if WARNING_HEADER_FIELD not in response.headers:  # type: ignore [arg-type]
        return

    backend_warnings = json.loads(
        response.headers[WARNING_HEADER_FIELD],  # type: ignore [arg-type]
    )

    for bw in backend_warnings:
        type_, _, message = bw.partition(": ")
        if type_ == "DeprecationWarning":
            category = BentoDeprecationWarning
        else:
            category = BentoWarning  # type: ignore [assignment]

        warnings.warn(message, category=category, stacklevel=4)


def check_http_error(response: Response) -> None:
    if is_500_series_error(response.status_code):
        try:
            json_body = response.json()
            message = json_body.get("detail")
        except JSONDecodeError:
            json_body = None
            message = None
        if response.status_code == 504:
            message = "The remote gateway timed out."
        raise BentoServerError(
            http_status=response.status_code,
            http_body=response.content,
            json_body=json_body,
            message=message,
            headers=response.headers,
        )
    elif is_400_series_error(response.status_code):
        try:
            json_body = response.json()
            message = json_body.get("detail")
        except JSONDecodeError:
            json_body = None
            message = None
        if response.status_code == 408:
            message = "The request transmission timed out."
        raise BentoClientError(
            http_status=response.status_code,
            http_body=response.content,
            json_body=json_body,
            message=message,
            headers=response.headers,
        )


async def check_http_error_async(response: ClientResponse) -> None:
    if is_500_series_error(response.status):
        try:
            json_body = await response.json()
            http_body = await response.read()
            message = json_body.get("detail", "")
        except ContentTypeError:
            http_body = None
            json_body = None
            message = ""

        if response.status == 504:
            message = "The remote gateway timed out."
        raise BentoServerError(
            http_status=response.status,
            http_body=http_body,
            json_body=json_body,
            message=message,
            headers=response.headers,
        )

    if is_400_series_error(response.status):
        try:
            json_body = await response.json()
            http_body = await response.read()
            message = json_body.get("detail", "")
        except ContentTypeError:
            http_body = None
            json_body = None
            message = ""
        if response.status == 408:
            message = "The request transmission timed out."
        raise BentoClientError(
            http_status=response.status,
            http_body=http_body,
            json_body=json_body,
            message=message,
            headers=response.headers,
        )
