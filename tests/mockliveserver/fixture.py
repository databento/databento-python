import asyncio
import contextlib
import os
import pathlib
import sys
from asyncio.subprocess import Process
from collections.abc import AsyncGenerator
from collections.abc import Generator
from typing import Callable
from typing import TypeVar

import pytest
import pytest_asyncio
from databento.common.publishers import Dataset
from databento.live.gateway import GatewayControl
from databento_dbn import Schema

from tests import TESTS_ROOT


class MockLiveServerInterface:
    """
    Process wrapper for communicating with the mock live server.
    """

    _GC = TypeVar("_GC", bound=GatewayControl)

    def __init__(
        self,
        process: Process,
        host: str,
        port: int,
        echo_file: pathlib.Path,
    ):
        self._process = process
        self._host = host
        self._port = port
        self._echo_fd = open(echo_file)

    @property
    def host(self) -> str:
        """
        The mock live server host.

        Returns
        -------
        str

        """
        return self._host

    @property
    def port(self) -> int:
        """
        The mock live server port.

        Returns
        -------
        int

        """
        return self._port

    @property
    def stdout(self) -> asyncio.StreamReader:
        if self._process.stdout is not None:
            return self._process.stdout
        raise RuntimeError("no stream reader for stdout")

    async def _send_command(
        self,
        command: str,
        timeout: float = 1.0,
    ) -> None:
        if self._process.stdin is None:
            raise RuntimeError("cannot write command to mock live server")
        self._process.stdin.write(
            f"{command.strip()}\n".encode(),
        )

        try:
            line = await asyncio.wait_for(self.stdout.readline(), timeout)
        except asyncio.TimeoutError:
            raise RuntimeError("timeout waiting for command acknowledgement")

        line_str = line.decode("utf-8")

        if line_str.startswith(f"ack: {command}"):
            return
        elif line_str.startswith(f"nack: {command}"):
            raise RuntimeError(f"received nack for command: {command}")

        raise RuntimeError(f"invalid response from server: {line_str!r}")

    async def active_count(self) -> None:
        """
        Send the "active_count" command.
        """
        await self._send_command("active_count")

    async def add_key(self, api_key: str) -> None:
        """
        Send the "add_key" command.

        Parameters
        ----------
        api_key : str
            The API key to add.

        """
        await self._send_command(f"add_key {api_key}")

    async def add_dbn(self, dataset: Dataset, schema: Schema, path: pathlib.Path) -> None:
        """
        Send the "add_dbn" command.

        Parameters
        ----------
        dataset : Dataset
            The DBN dataset.
        schema : Schema
            The DBN schema.
        path : pathlib.Path
            The path to the DBN file.

        """
        await self._send_command(f"add_dbn {dataset} {schema} {path.resolve()}")

    async def del_key(self, api_key: str) -> None:
        """
        Send the "del_key" command.

        Parameters
        ----------
        api_key : str
            The API key to delete.

        """
        await self._send_command(f"del_key {api_key}")

    async def disconnect(self, session_id: str) -> None:
        """
        Send the "disconnect" command.

        Parameters
        ----------
        session_id : str
            The live session ID to disconnect.

        """
        await self._send_command(f"disconnect {session_id}")

    async def close(self) -> None:
        """
        Send the "close" command.
        """
        await self._send_command("close")

    def kill(self) -> None:
        """
        Kill the mock live server.
        """
        self._process.kill()

    @contextlib.contextmanager
    def test_context(self) -> Generator[None, None, None]:
        self._echo_fd.seek(0, os.SEEK_END)
        yield

    @contextlib.asynccontextmanager
    async def api_key_context(self, api_key: str) -> AsyncGenerator[str, None]:
        await self.add_key(api_key)
        yield api_key
        await self.del_key(api_key)

    async def wait_for_start(self) -> None:
        await self.active_count()

    async def wait_for_message_of_type(
        self,
        message_type: type[_GC],
        timeout: float = 1.0,
    ) -> _GC:
        """
        Wait for a message of a given type.

        Parameters
        ----------
        message_type : type[_GC]
            The type of GatewayControl message to wait for.
        timeout: float, default 1.0
            The maximum number of seconds to wait.

        Returns
        -------
        _GC

        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while self._process.returncode is None:
            line = await asyncio.wait_for(
                loop.run_in_executor(None, self._echo_fd.readline),
                timeout=max(
                    0,
                    deadline - loop.time(),
                ),
            )
            try:
                return message_type.parse(line)
            except ValueError:
                continue
        raise RuntimeError("Mock server is closed.")


@pytest_asyncio.fixture(name="mock_live_server", scope="module")
async def fixture_mock_live_server(
    unused_tcp_port_factory: Callable[[], int],
    tmp_path_factory: pytest.TempPathFactory,
) -> AsyncGenerator[MockLiveServerInterface, None]:
    port = unused_tcp_port_factory()
    echo_file = tmp_path_factory.mktemp("mockliveserver") / "echo.txt"
    echo_file.touch()

    process = await asyncio.subprocess.create_subprocess_exec(
        "python3",
        "-m",
        "tests.mockliveserver",
        "127.0.0.1",
        "--port",
        str(port),
        "--echo",
        echo_file.resolve(),
        "--verbose",
        executable=sys.executable,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=sys.stderr,
    )

    interface = MockLiveServerInterface(
        process=process,
        host="127.0.0.1",
        port=port,
        echo_file=echo_file,
    )

    await interface.wait_for_start()

    for dataset in Dataset:
        for schema in Schema.variants():
            path = TESTS_ROOT / "data" / dataset / f"test_data.{schema}.dbn.zst"
            if path.exists():
                await interface.add_dbn(dataset, schema, path)

    yield interface

    process.terminate()
    await process.wait()
