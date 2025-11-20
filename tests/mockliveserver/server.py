from __future__ import annotations

import asyncio
import enum
import logging
import random
import string
from collections.abc import Generator
from collections.abc import Mapping
from functools import singledispatchmethod
from io import BytesIO
from io import FileIO
from typing import Any
from typing import Final

from databento_dbn import Schema

from databento.common import cram
from databento.common.publishers import Dataset
from databento.live.gateway import AuthenticationRequest
from databento.live.gateway import AuthenticationResponse
from databento.live.gateway import ChallengeRequest
from databento.live.gateway import GatewayControl
from databento.live.gateway import Greeting
from databento.live.gateway import SessionStart
from databento.live.gateway import SubscriptionRequest
from databento.live.gateway import parse_gateway_message

from .source import ReplayProtocol


SERVER_VERSION: Final = "0.4.2"
READ_BUFFER_SIZE: Final = 32 * 2**10

logger = logging.getLogger(__name__)


class SessionState(enum.Enum):
    NEW = enum.auto()
    NOT_AUTHENTICATED = enum.auto()
    AUTHENTICATED = enum.auto()
    STREAMING = enum.auto()
    CLOSED = enum.auto()


class SessionMode(enum.Enum):
    FILE_REPLAY = enum.auto()


def session_id_generator(start: int = 0) -> Generator[int, None, None]:
    while True:
        yield start
        start += 1


class MockLiveServerProtocol(asyncio.BufferedProtocol):
    session_id_generator = session_id_generator(0)

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        mode: SessionMode,
        api_key_table: Mapping[str, set[str]],
        file_replay_table: Mapping[tuple[Dataset, Schema], ReplayProtocol],
        echo_stream: FileIO,
    ) -> None:
        self._loop = loop
        self._mode = mode
        self._api_key_table = api_key_table
        self._file_replay_table = file_replay_table
        self._echo_stream = echo_stream

        self._transport: asyncio.Transport | None = None
        self._buffer = bytearray(READ_BUFFER_SIZE)
        self._cram = "".join(random.choice(string.ascii_letters) for _ in range(32))  # noqa: S311
        self._peer: str | None = None
        self._session_id: str | None = None

        self._data = BytesIO()
        self._state = SessionState.NEW
        self._dataset: Dataset | None = None
        self._subscriptions: list[SubscriptionRequest] = []
        self._replay_tasks: set[asyncio.Task[None]] = set()

    @property
    def mode(self) -> SessionMode:
        return self._mode

    @property
    def dataset(self) -> Dataset:
        if self._dataset is None:
            raise RuntimeError("No dataset set")
        return self._dataset

    @property
    def state(self) -> SessionState:
        return self._state

    @state.setter
    def state(self, value: SessionState) -> None:
        logger.debug(
            "session state changed from %s to %s for %s",
            self._state.name,
            value.name,
            self.peer,
        )
        self._state = value

    @property
    def session_id(self) -> str:
        if self._session_id is None:
            self._session_id = f"mock-{next(self.session_id_generator)}"
            logger.info("assigned session id %s for %s", self._session_id, self.peer)
        return self._session_id

    @property
    def transport(self) -> asyncio.Transport:
        if self._transport is None:
            raise RuntimeError("No transport set")
        return self._transport

    @property
    def buffer(self) -> bytearray:
        return self._buffer

    @property
    def peer(self) -> str | None:
        return self._peer

    def get_authentication_response(
        self,
        success: bool,
        session_id: str = "0",
    ) -> AuthenticationResponse:
        return AuthenticationResponse(
            success="0" if not success else "1",
            error="Authentication failed." if not success else None,
            session_id=None if not success else str(session_id),
        )

    def get_challenge(self) -> ChallengeRequest:
        return ChallengeRequest(
            cram=self._cram,
        )

    def get_greeting(self) -> Greeting:
        return Greeting(lsg_version=SERVER_VERSION)

    def send_gateway_message(self, message: GatewayControl) -> None:
        logger.info(
            "sending %s message to %s",
            message.__class__.__name__,
            self.peer,
        )
        self.transport.write(bytes(message))

    def hangup(self, reason: str | None = None, is_error: bool = False) -> None:
        if reason is not None:
            if is_error:
                logger.error(reason)
            else:
                logger.info(reason)
        logger.info("sending eof to %s", self.peer)
        self.transport.write_eof()

    @singledispatchmethod
    def handle_client_message(self, message: GatewayControl) -> None:
        logger.error("unhandled client message %s", message.__class__.__name__)

    @handle_client_message.register(AuthenticationRequest)
    def _(self, message: AuthenticationRequest) -> None:
        logger.debug("received challenge response %s from %s", message.auth, self.peer)
        if self.state != SessionState.NOT_AUTHENTICATED:
            self.hangup(
                reason="authentication request sent when already authenticated",
                is_error=True,
            )
            return

        _, bucket_id = message.auth.split("-")

        for api_key in self._api_key_table.get(bucket_id, []):
            logger.debug("checking key %s", api_key)
            expected_response = cram.get_challenge_response(
                self._cram,
                api_key,
            )
            if message.auth == expected_response:
                break
        else:
            logger.error("failed authentication for %s", self.peer)
            self.send_gateway_message(self.get_authentication_response(success=False))
            return

        self.state = SessionState.AUTHENTICATED
        self._dataset = Dataset(message.dataset)
        self.send_gateway_message(
            self.get_authentication_response(
                success=True,
                session_id=self.session_id,
            ),
        )

    @handle_client_message.register(SubscriptionRequest)
    def _(self, message: SubscriptionRequest) -> None:
        logger.info("received subscription request %s from %s", str(message).strip(), self.peer)
        if self.state == SessionState.NOT_AUTHENTICATED:
            self.hangup(
                reason="subscription received while unauthenticated",
                is_error=True,
            )

        self._subscriptions.append(message)

    @handle_client_message.register(SessionStart)
    def _(self, message: SessionStart) -> None:
        logger.info("received session start request %s from %s", str(message).strip(), self.peer)
        if self.state == SessionState.NOT_AUTHENTICATED:
            self.hangup(
                reason="session start received while unauthenticated",
                is_error=True,
            )

        if self.mode == SessionMode.FILE_REPLAY:
            task = self._loop.create_task(self._file_replay_task())
            self._replay_tasks.add(task)
            task.add_done_callback(self._replay_done_callback)
        else:
            logger.error("unsupported session mode %s", self.mode)

    def buffer_updated(self, nbytes: int) -> None:
        logger.debug("%d bytes from %s", nbytes, self.peer)

        self._data.write(self._buffer[:nbytes])
        buffer_lines = (self._data.getvalue()).splitlines(keepends=True)

        if not buffer_lines[-1].endswith(b"\n"):
            self._data = BytesIO(buffer_lines.pop(-1))
        else:
            self._data = BytesIO()

        for line in buffer_lines:
            try:
                message = parse_gateway_message(line.decode("utf-8"))
            except ValueError as val_err:
                self.hangup(
                    reason=str(val_err),
                    is_error=True,
                )
            else:
                self._echo_stream.write(bytes(message))
                self.handle_client_message(message)

        return super().buffer_updated(nbytes)

    def connection_made(self, transport: asyncio.transports.BaseTransport) -> None:
        if not isinstance(transport, asyncio.Transport):
            raise RuntimeError(f"cannot write to {transport}")

        self._transport = transport

        peer_host, peer_port, *_ = transport.get_extra_info("peername")
        self._peer = f"{peer_host}:{peer_port}"

        logger.info("incoming connection from %s", self.peer)
        self.send_gateway_message(self.get_greeting())
        self.send_gateway_message(self.get_challenge())

        self.state = SessionState.NOT_AUTHENTICATED

        return super().connection_made(transport)

    def connection_lost(self, exc: Exception | None) -> None:
        logger.info("disconnected %s", self.peer)
        self.state = SessionState.CLOSED
        return super().connection_lost(exc)

    def eof_received(self) -> bool | None:
        logger.info("eof received from %s", self.peer)
        return super().eof_received()

    def get_buffer(self, sizehint: int) -> bytearray:
        if sizehint > len(self.buffer):
            logger.warning("requested buffer size %d is larger than current size", sizehint)
        return self.buffer

    def _replay_done_callback(self, task: asyncio.Task[Any]) -> None:
        self._replay_tasks.remove(task)

        replay_exception = task.exception()
        if replay_exception is not None:
            logger.error("exception while replaying DBN files: %s", replay_exception)

        if self._replay_tasks:
            logger.debug("%d replay tasks remain", len(self._replay_tasks))
        else:
            self.hangup(reason="all replay tasks completed")

    async def _file_replay_task(self) -> None:
        for subscription in self._subscriptions:
            schema = (
                Schema.from_str(subscription.schema)
                if isinstance(subscription.schema, str)
                else subscription.schema
            )
            replay = self._file_replay_table[(self.dataset, schema)]
            logger.info("starting replay %s for %s", replay.name, self.peer)
            for chunk in replay:
                self.transport.write(chunk)
                await asyncio.sleep(0)
            logger.info("replay of %s completed for %s", replay.name, self.peer)
