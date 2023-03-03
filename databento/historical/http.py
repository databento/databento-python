import sys
from io import BufferedIOBase
from json.decoder import JSONDecodeError
from typing import Any, List, Optional, Tuple

import aiohttp
import requests
from aiohttp import ClientResponse
from databento.historical.error import BentoClientError, BentoServerError
from databento.version import __version__
from requests import Response
from requests.auth import HTTPBasicAuth


_32KB = 1024 * 32  # 32_768


class BentoHttpAPI:
    """The base class for all Databento HTTP API endpoints."""

    TIMEOUT = 100

    def __init__(self, key: str, gateway: str):
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
        user_agent = f"Databento/{__version__} Python/{python_version}"

        self._key = key
        self._gateway = gateway
        self._headers = {"accept": "application/json", "user-agent": user_agent}

    def _check_api_key(self) -> None:
        if self._key == "YOUR_API_KEY":
            raise ValueError(
                "The API key is currently set to 'YOUR_API_KEY'. "
                "Please replace this value with a valid API key. "
                "You can find these through your Databento user portal.",
            )

    def _get(
        self,
        url: str,
        params: Optional[List[Tuple[str, Optional[str]]]] = None,
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
            check_http_error(response)
            return response

    async def _get_json_async(
        self,
        url: str,
        params: Optional[List[Tuple[str, Optional[str]]]] = None,
        basic_auth: bool = False,
    ) -> Any:
        self._check_api_key()
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url=url,
                params=params,
                headers=self._headers,
                auth=aiohttp.BasicAuth(login=self._key, password="", encoding="utf-8")
                if basic_auth
                else None,
                timeout=self.TIMEOUT,
            ) as response:
                await check_http_error_async(response)
                return await response.json()

    def _post(
        self,
        url: str,
        params: Optional[List[Tuple[str, Optional[str]]]] = None,
        basic_auth: bool = False,
    ) -> Response:
        self._check_api_key()

        with requests.post(
            url=url,
            params=params,
            headers=self._headers,
            auth=HTTPBasicAuth(username=self._key, password="") if basic_auth else None,
            timeout=(self.TIMEOUT, self.TIMEOUT),
        ) as response:
            check_http_error(response)
            return response

    def _stream(
        self,
        url: str,
        params: List[Tuple[str, Optional[str]]],
        basic_auth: bool,
        writer: BufferedIOBase,
    ) -> None:
        self._check_api_key()

        with requests.get(
            url=url,
            params=params,
            headers=self._headers,
            auth=HTTPBasicAuth(username=self._key, password="") if basic_auth else None,
            timeout=(self.TIMEOUT, self.TIMEOUT),
            stream=True,
        ) as response:
            check_http_error(response)

            for chunk in response.iter_content(chunk_size=_32KB):
                writer.write(chunk)

    async def _stream_async(
        self,
        url: str,
        params: List[Tuple[str, Optional[str]]],
        basic_auth: bool,
        writer: BufferedIOBase,
    ) -> None:
        self._check_api_key()

        async with aiohttp.ClientSession() as session:
            async with session.get(
                url=url,
                params=[x for x in params if x[1] is not None],
                headers=self._headers,
                auth=aiohttp.BasicAuth(login=self._key, password="", encoding="utf-8")
                if basic_auth
                else None,
                timeout=self.TIMEOUT,
            ) as response:
                await check_http_error_async(response)

                async for chunk in response.content.iter_chunks():
                    writer.write(chunk[0])


def is_400_series_error(status: int) -> bool:
    return status // 100 == 4


def is_500_series_error(status: int) -> bool:
    return status // 100 == 5


def check_http_error(response: Response) -> None:
    if is_500_series_error(response.status_code):
        try:
            json_body = response.json()
            message = json_body.get("detail")
        except JSONDecodeError:
            json_body = None
            message = None
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
        raise BentoClientError(
            http_status=response.status_code,
            http_body=response.content,
            json_body=json_body,
            message=message,
            headers=response.headers,
        )


async def check_http_error_async(response: ClientResponse) -> None:
    if is_500_series_error(response.status):
        json_body = await response.json()
        http_body = await response.read()
        raise BentoServerError(
            http_status=response.status,
            http_body=http_body,
            json_body=json_body,
            message=json_body["detail"],
            headers=response.headers,
        )
    elif is_400_series_error(response.status):
        json_body = await response.json()
        http_body = await response.read()
        raise BentoClientError(
            http_status=response.status,
            http_body=http_body,
            json_body=json_body,
            message=json_body["detail"],
            headers=response.headers,
        )
