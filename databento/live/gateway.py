import asyncio
import dataclasses
import logging
from functools import partial, singledispatchmethod
from io import BytesIO
from typing import Optional, Type, TypeVar, Union

from databento.common import cram
from databento.common.enums import Compression, Dataset, Encoding, Schema, SType


logger = logging.getLogger(__name__)

T = TypeVar("T", bound="GatewayControl")

MIN_BUFFER_SIZE = 16 * 1024  # 16kB


@dataclasses.dataclass
class GatewayControl:
    """
    Base class for gateway control messages.
    """

    @classmethod
    def parse(cls: Type[T], line: str) -> T:
        """
        Parse a a message of type `T` from a string.

        Returns
        -------
        T

        Raises
        ------
        ValueError
            If the line fails to parse.

        """
        if not line.endswith("\n"):
            raise ValueError(f"`{line.strip()}` does not end with a newline")

        tokens = line[:-1].split("|")  # split excluding trailing new line
        splitter = partial(str.split, sep="=", maxsplit=1)
        data_dict = {k: v for k, v in map(splitter, tokens)}

        try:
            return cls(**data_dict)
        except TypeError as type_err:
            raise ValueError(
                f"`{line.strip()} is not a parsible {cls.__name__}",
            ) from type_err

    def __str__(self) -> str:
        tokens = "|".join(
            f"{k}={str(v)}"
            for k, v in dataclasses.asdict(self).items()
            if v is not None
        )
        return f"{tokens}\n"

    def __bytes__(self) -> bytes:
        return str(self).encode("utf-8")


@dataclasses.dataclass
class Greeting(GatewayControl):
    """
    A greeting message is sent by the gateway upon connection.
    """

    lsg_version: str


@dataclasses.dataclass
class ChallengeRequest(GatewayControl):
    """
    A challenge request is sent by the gateway upon connection.
    """

    cram: str


@dataclasses.dataclass
class AuthenticationResponse(GatewayControl):
    """
    An authentication response is sent by the gateway after a
    valid authentication request is sent to the gateway.
    """

    success: str
    error: Optional[str] = None
    session_id: Optional[str] = None


@dataclasses.dataclass
class AuthenticationRequest(GatewayControl):
    """
    An authentication request is sent to the gateway after a
    challenge response is received. This is required to authenticate
    a user.
    """

    auth: str
    dataset: Union[Dataset, str]
    encoding: Encoding = Encoding.DBN
    details: Optional[str] = None
    ts_out: str = "1"
    compression: Compression = Compression.NONE


@dataclasses.dataclass
class SubscriptionRequest(GatewayControl):
    """
    A subscription request is sent to the gateway upon request from
    the client.
    """

    schema: Union[Schema, str]
    stype_in: SType
    symbols: str
    start: Optional[int] = None


@dataclasses.dataclass
class SessionStart(GatewayControl):
    """
    A session start message is sent to the gateway upon request from
    the client.
    """

    start_session: str = "0"


def parse_gateway_message(line: str) -> GatewayControl:
    """
    Parse a gateway message from a string.

    Returns
    -------
    GatewayControl

    Raises
    ------
    ValueError
        If `line` is not a parsible GatewayControl message.

    """
    for message_cls in GatewayControl.__subclasses__():
        try:
            return message_cls.parse(line)
        except ValueError:
            continue
    raise ValueError(f"`{line.strip()}` is not a parsible gateway message")


class GatewayProtocol(asyncio.BufferedProtocol):
    """
    The gateway protocol for the Databento Live Subscription Gateway.
    This protocol supports sending and responding to plain text gateway
    messages.

    See Also
    --------
    `asyncio.BufferedProtocol`

    """

    def __init__(
        self,
        key: str,
        dataset: Union[Dataset, str],
        ts_out: bool,
    ) -> None:
        self.__key: str = key
        self.__dataset: Union[Dataset, str] = dataset
        self.__transport: asyncio.Transport
        self._buffer: bytearray
        self._data: BytesIO = BytesIO()
        self._ts_out: str = str(int(ts_out))

        self._authenticated: "asyncio.Future[str]" = asyncio.Future()

    @property
    def authenticated(self) -> "asyncio.Future[str]":
        """
        A Future property that will complete when CRAM
        authentication is completed or contain an
        exception when it fails.

        Returns
        -------
        asyncio.Future

        """
        return self._authenticated

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        if not isinstance(transport, asyncio.Transport):
            raise TypeError("Connection does not support read-write operations.")
        self.__transport = transport

    def get_buffer(self, size_hint: int) -> bytearray:
        self._buffer = bytearray(max(size_hint, MIN_BUFFER_SIZE))
        return self._buffer

    def buffer_updated(self, nbytes: int) -> None:
        logger.debug("read %d bytes from remote", nbytes)

        self._data.seek(0, 2)  # seek to the end
        self._data.write(self._buffer[:nbytes])
        buffer_lines = self._data.getvalue().splitlines(keepends=True)

        for line in buffer_lines:
            if line.endswith(b"\n"):
                try:
                    message = parse_gateway_message(line.decode("utf-8"))
                except ValueError:
                    logger.exception("could not parse gateway message: %s", line)
                else:
                    self.handle_gateway_message(message)
                    self._data = BytesIO()
            else:
                self._data = BytesIO(line)
                break

    @singledispatchmethod
    def handle_gateway_message(self, message: GatewayControl) -> None:
        """
        Dispatch for GatewayControl messages.

        Parameters
        ----------
        message : GatewayControl
            The message to dispatch.

        """
        logger.error("unhandled gateway message: %s", type(message).__name__)

    @handle_gateway_message.register(Greeting)
    def _(self, message: Greeting) -> None:
        logger.debug("greeting received by remote gateway v%s", message.lsg_version)

    @handle_gateway_message.register(ChallengeRequest)
    def _(self, message: ChallengeRequest) -> None:
        logger.debug("received CRAM challenge: %s", message.cram)
        response = cram.get_challenge_response(message.cram, self.__key)
        auth_request = AuthenticationRequest(
            auth=response,
            dataset=self.__dataset,
            ts_out=self._ts_out,
        )
        logger.debug("sending CRAM challenge response: %s", str(auth_request).strip())
        self.__transport.write(bytes(auth_request))

    @handle_gateway_message.register(AuthenticationResponse)
    def _(self, message: AuthenticationResponse) -> None:
        if message.success == "0":
            self._authenticated.set_exception(ValueError(message.error))
            logger.error("CRAM authentication failed: %s", message.error)
        else:
            self._authenticated.set_result(str(message.session_id))
            logger.debug(
                "CRAM authenticated session id assigned `%s`",
                message.session_id,
            )
