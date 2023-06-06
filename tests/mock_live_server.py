from __future__ import annotations

import argparse
import asyncio
import logging
import os
import pathlib
import random
import string
import sys
import threading
import time
from io import BytesIO
from typing import Callable, Dict, List, NewType, Optional, Type, TypeVar

import zstandard
from databento.common import cram
from databento.common.enums import Schema
from databento.live.gateway import (
    AuthenticationRequest,
    AuthenticationResponse,
    ChallengeRequest,
    GatewayControl,
    Greeting,
    SessionStart,
    SubscriptionRequest,
    parse_gateway_message,
)
from databento.live.protocol import singledispatchmethod


LIVE_SERVER_VERSION: str = "1.0.0"

G = TypeVar("G", bound=GatewayControl)
MessageQueue = NewType("MessageQueue", "asyncio.Queue[GatewayControl]")


logger = logging.getLogger(__name__)


class MockLiveServerProtocol(asyncio.BufferedProtocol):
    """
    The connection protocol to mock the Databento Live Subscription Gateway.

    Attributes
    ----------
    cram_challenge : str
        The CRAM challenge string that will be used
        to authenticate users.
    is_authenticated : bool
        Flag indicating the user has been authenticated.
    is_streaming : bool
        Flag indicating streaming has begun.
    peer : str
        The peer ip and port in the format
        {ip}:{port}.
    version : str
        The server version string.

    See Also
    --------
    `asyncio.BufferedProtocol`
    """

    def __init__(
        self,
        version: str,
        user_api_keys: Dict[str, str],
        message_queue: MessageQueue,
        dbn_path: pathlib.Path,
    ) -> None:
        self.__transport: asyncio.Transport
        self._buffer: bytearray
        self._data: BytesIO
        self._message_queue: MessageQueue = message_queue
        self._cram_challenge: str = "".join(
            random.choice(string.ascii_letters) for _ in range(32)
        )
        self._message_queue = message_queue
        self._peer: str = ""
        self._version: str = version
        self._is_authenticated: bool = False
        self._is_streaming: bool = False

        self._dbn_path = dbn_path
        self._user_api_keys = user_api_keys

    @property
    def cram_challenge(self) -> str:
        """
        Return the CRAM challenge string that will be used
        to authenticate users.

        Returns
        -------
        str

        """
        return self._cram_challenge

    @property
    def is_authenticated(self) -> bool:
        """
        Return True if the user has been authenticated.

        Returns
        -------
        bool

        """
        return self._is_authenticated

    @property
    def is_streaming(self) -> bool:
        """
        Return True if the streaming has begun.

        Returns
        -------
        bool

        """
        return self._is_streaming

    @property
    def peer(self) -> str:
        """
        Return the peer IP and port in the format {ip}:{port}.

        Returns
        -------
        str

        """
        return self._peer

    @property
    def user_api_keys(self) -> Dict[str, str]:
        """
        Return a dictionary of user api keys for testing.
        The keys to this dictionary are the bucket_ids.
        The value shoud be a single user API key.

        Returns
        -------
        Dict[str, str]

        """
        return self._user_api_keys

    @property
    def session_id(self) -> str:
        """
        A mock session_id for this protocol.

        Returns
        -------
        str

        """
        return str(hash(self))

    @property
    def version(self) -> str:
        """
        Return the server version string.

        Returns
        -------
        str

        """
        return self._version

    def connection_lost(
        self,
        exception: Exception,
    ) -> None:
        """
        Event handler when the connection is lost.

        Parameters
        ----------
        exception : Exception
            The exception that closed the connection.

        See Also
        --------
        asyncio.BufferedProtocol

        """
        logger.info("%s disconnected", self._peer)
        return super().connection_lost(exception)

    def connection_made(
        self,
        transport: asyncio.BaseTransport,
    ) -> None:
        """
        Event handler when the connection is made.

        Parameters
        ----------
        transport : asyncio.BaseTransport
            The transport for the new connection.

        See Also
        --------
        asyncio.BufferedProtocol

        """
        if not isinstance(transport, asyncio.Transport):
            raise RuntimeError(f"cannot write to {transport}")

        self.__transport = transport
        self._buffer = bytearray(1024)
        self._data = BytesIO()
        self._schemas: List[Schema] = []

        peer_host, peer_port = transport.get_extra_info("peername")
        self._peer = f"{peer_host}:{peer_port}"
        logger.info("%s connected to %s", type(self).__name__, self._peer)

        # Print server version
        greeting = Greeting(lsg_version=self.version)
        logger.debug("sending greeting to %s", self._peer)
        self.__transport.write(bytes(greeting))

        # Print CRAM challenge
        logger.debug("sending authentication challenge to %s", self._peer)
        cram_challenge = ChallengeRequest(cram=self.cram_challenge)
        self.__transport.write(bytes(cram_challenge))

    def get_buffer(self, _: int) -> bytearray:
        """
        Get the receive buffer.
        This protocol allocates the buffer at initialization,
        because of this the size_hint is unused.

        Parameters
        ----------
        size_hint : int
            (unused)

        Returns
        -------
        bytearray

        See Also
        --------
        asyncio.BufferedProtocol

        """
        return self._buffer

    def buffer_updated(self, nbytes: int) -> None:
        """
        Call when the buffer has data to read.

        Parameters
        ----------
        nbytes : int
            The number of bytes available for reading.

        See Also
        --------
        asyncio.BufferedProtocol

        """
        logger.debug("%d bytes from %s", nbytes, self.peer)

        self._data.write(self._buffer[:nbytes])
        buffer_lines = (self._data.getvalue()).splitlines(keepends=True)

        if not buffer_lines[-1].endswith(b"\n"):
            # Save this for the next call
            self._data = BytesIO(buffer_lines.pop(-1))
        else:
            self._data = BytesIO()

        for line in buffer_lines:
            try:
                message = parse_gateway_message(line.decode("utf-8"))
            except ValueError as val_err:
                logger.exception(val_err)
                continue
            else:
                self._message_queue.put_nowait(message)
                self.handle_client_message(message)

    def eof_received(self) -> bool:
        """
        Call when the EOF has been received.

        See Also
        --------
        asyncio.BufferedProtocol
        """
        logger.info("received eof from %s", self.peer)
        return bool(super().eof_received())

    @singledispatchmethod
    def handle_client_message(self, message: GatewayControl) -> None:
        raise TypeError(f"Unhandled client message {message}")

    @handle_client_message.register(AuthenticationRequest)
    def _(self, message: AuthenticationRequest) -> None:
        logger.info("received CRAM response: %s", message.auth)
        if self.is_authenticated:
            logger.error("authentication request sent when already authenticated")
            self.__transport.close()
            return
        if self.is_streaming:
            logger.error("authentication request sent while streaming")
            self.__transport.close()
            return

        _, bucket_id = message.auth.split("-")

        try:
            # First, get the user's API key
            user_api_key = self.user_api_keys.get(bucket_id)
            if user_api_key is None:
                raise KeyError("Could not resolve API key.")

            # Next, compute the expected response
            expected_response = cram.get_challenge_response(
                self.cram_challenge,
                user_api_key,
            )
            if message.auth != expected_response:
                raise ValueError(
                    f"Expected `{expected_response}` but was `{message.auth}`",
                )
        except (KeyError, ValueError) as exc:
            logger.error(
                "could not authenticate user",
                exc_info=exc,
            )
            auth_fail = AuthenticationResponse(success="0", error=str(exc))
            self.__transport.write(bytes(auth_fail))
            self.__transport.write_eof()
        else:
            # Establish a new user session
            self._is_authenticated = True
            auth_success = AuthenticationResponse(
                success="1",
                session_id=self.session_id,
            )
            self.__transport.write(bytes(auth_success))

    @handle_client_message.register(SubscriptionRequest)
    def _(self, message: SubscriptionRequest) -> None:
        logger.info("received subscription request: %s", str(message).strip())
        if not self.is_authenticated:
            logger.error("subscription request sent while unauthenticated")
            self.__transport.close()

        if self.is_streaming:
            logger.error("subscription request sent while streaming")
            self.__transport.close()

        self._schemas.append(Schema(message.schema))

    @handle_client_message.register(SessionStart)
    def _(self, message: SessionStart) -> None:
        logger.info("received session start request: %s", str(message).strip())
        self._is_streaming = True

        for schema in self._schemas:
            for test_data_path in self._dbn_path.glob(f"*{schema}.dbn.zst"):
                decompressor = zstandard.ZstdDecompressor().stream_reader(
                    test_data_path.read_bytes(),
                )
                logger.info(
                    "streaming %s for %s schema",
                    test_data_path.name,
                    schema,
                )
                self.__transport.write(decompressor.readall())

        logger.info(
            "data streaming for %d schema(s) completed",
            len(self._schemas),
        )

        self.__transport.write_eof()
        self.__transport.close()


class MockLiveServer:
    """
    A mock of the Databento Live Subscription Gateway.
    This is used for unit testing instead of connecting to the
    actual gateway.

    Attributes
    ----------
    host : str
        The host of the mock server.
    port : int
        The port of the mock server.
    server : asyncio.base_events.Server
        The mock server object.

    Methods
    -------
    create(host="localhost", port=0)
        Factory method to create a new MockLiveServer instance.
        This is the prefered way to create an instance of
        this class.

    See Also
    --------
    `asyncio.create_server`

    """

    def __init__(self) -> None:
        self._server: asyncio.base_events.Server
        self._host: str
        self._port: int
        self._dbn_path: pathlib.Path
        self._user_api_keys: Dict[str, str]
        self._message_queue: MessageQueue
        self._thread: threading.Thread
        self._loop: asyncio.AbstractEventLoop

    @property
    def host(self) -> str:
        """
        Return the host of the mock server.

        Returns
        -------
        str

        """
        return self._host

    @property
    def port(self) -> int:
        """
        Return the port of the mock server.

        Returns
        -------
        int

        """
        return self._port

    @property
    def server(self) -> asyncio.base_events.Server:
        """
        Return the mock server object.

        Returns
        -------
        asyncio.base_events.Server

        """
        return self._server

    @classmethod
    def _protocol_factory(
        cls,
        user_api_keys: Dict[str, str],
        message_queue: MessageQueue,
        version: str,
        dbn_path: pathlib.Path,
    ) -> Callable[[], MockLiveServerProtocol]:
        def factory() -> MockLiveServerProtocol:
            return MockLiveServerProtocol(
                version=version,
                user_api_keys=user_api_keys,
                message_queue=message_queue,
                dbn_path=dbn_path,
            )

        return factory

    @classmethod
    async def create(
        cls,
        host: str = "localhost",
        port: int = 0,
        dbn_path: pathlib.Path = pathlib.Path.cwd(),
    ) -> "MockLiveServer":
        """
        Create a mock server instance. This factory method is the
        preferred way to create an instance of MockLiveServer.

        Parameters
        ----------
        host : str
            The hostname for the mock server.
            Defaults to "localhost"
        port : int
            The port to bind for the mock server.
            Defaults to 0 which will bind to an open port.
        dbn_path : pathlib.Path (default: cwd)
            A path to DBN files for streaming.
            The files must contain the schema name and end with
            `.dbn.zst`.
            See `tests/data` for examples.

        Returns
        -------
        MockLiveServer

        """
        logger.info(
            "creating %s with host=%s port=%s dbn_path=%s",
            cls.__name__,
            host,
            port,
            dbn_path,
        )

        loop = asyncio.new_event_loop()
        thread = threading.Thread(
            name="MockLiveServer",
            target=loop.run_forever,
            args=(),
            daemon=True,
        )
        thread.start()

        user_api_keys: Dict[str, str] = {}
        message_queue: MessageQueue = asyncio.Queue()  # type: ignore

        # We will add an API key from DATABENTO_API_KEY if it exists
        env_key = os.environ.get("DATABENTO_API_KEY")
        if env_key is not None:
            bucket_id = env_key[-cram.BUCKET_ID_LENGTH :]
            user_api_keys[bucket_id] = env_key

        server = asyncio.run_coroutine_threadsafe(
            loop.create_server(
                protocol_factory=cls._protocol_factory(
                    user_api_keys=user_api_keys,
                    message_queue=message_queue,
                    version=LIVE_SERVER_VERSION,
                    dbn_path=dbn_path,
                ),
                host=host,
                port=port,
                start_serving=False,
            ),
            loop=loop,
        ).result()

        mock_live_server = cls()

        # Initialize the MockLiveServer instance
        mock_live_server._server = server
        mock_live_server._host, mock_live_server._port = server.sockets[
            -1
        ].getsockname()
        mock_live_server._user_api_keys = user_api_keys
        mock_live_server._message_queue = message_queue
        mock_live_server._thread = thread
        mock_live_server._loop = loop

        return mock_live_server

    async def get_message(self, timeout: Optional[float]) -> GatewayControl:
        """
        Return the next gateway message received from the client.

        Parameters
        ----------
        timeout : float, optional
            Duration in seconds to wait before timing out.

        Returns
        -------
        GatewayControl

        Raises
        ------
        asyncio.TimeoutError
            If the timeout duration is reached, if specified.

        """
        return await asyncio.wait_for(
            self._message_queue.get(),
            timeout=timeout,
        )

    async def get_message_of_type(
        self,
        message_type: Type[G],
        timeout: float,
    ) -> G:
        """
        Return the next gateway message that is an instance of message_type
        received from the client. Messages that are removed from the
        queue until a match is found or the timeout expires, if specified.

        Parameters
        ----------
        message_type : Type[GatewayControl]
            The type of GatewayControl message to wait for.
        timeout : float, optional
            Duration in seconds to wait before timing out.

        Returns
        -------
        GatewayControl

        Raises
        ------
        asyncio.TimeoutError
            If the timeout duration is reached, if specified.

        """
        start_time = self._loop.time()
        end_time = self._loop.time() + timeout
        while start_time < end_time:
            remaining_time = abs(end_time - self._loop.time())

            message = asyncio.run_coroutine_threadsafe(
                asyncio.wait_for(
                    self._message_queue.get(),
                    timeout=remaining_time,
                ),
                loop=self._loop,
            ).result()

            if isinstance(message, message_type):
                return message

        raise asyncio.TimeoutError()

    async def start(self) -> None:
        """
        Start the mock server.

        """
        logger.info(
            "starting %s on %s:%s",
            self.__class__.__name__,
            self.host,
            self.port,
        )
        await self.server.start_serving()

    async def stop(self) -> None:
        """
        Stop the mock server.

        """
        logger.info(
            "stopping %s on %s:%s",
            self.__class__.__name__,
            self.host,
            self.port,
        )

        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "host",
        help="the hostname to bind; defaults to `localhost`",
        nargs="?",
        default="localhost",
    )
    parser.add_argument(
        "port",
        help="the port to bind; defaults to an open port",
        nargs="?",
        default=0,
    )
    parser.add_argument(
        "-d",
        "--dbn-path",
        metavar="DBN",
        action="store",
        help="path to a directory containing DBN files to stream",
    )

    params = parser.parse_args(sys.argv[1:])

    # Setup console logging handler
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Start MockLiveServer
    loop = asyncio.get_event_loop()
    mock_live_server = loop.run_until_complete(
        MockLiveServer.create(
            host=params.host,
            port=params.port,
            dbn_path=pathlib.Path(params.dbn_path),
        ),
    )
    loop.run_until_complete(mock_live_server.start())

    # Serve Forever
    try:
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit) as exit_exc:
        logger.fatal("Terminating on %s", type(exit_exc).__name__)
    finally:
        exit(0)
