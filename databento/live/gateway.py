from __future__ import annotations

import dataclasses
import logging
from io import BytesIO
from operator import attrgetter
from typing import TypeVar

from databento_dbn import Encoding
from databento_dbn import Schema
from databento_dbn import SType

from databento.common.publishers import Dataset
from databento.common.system import USER_AGENT


logger = logging.getLogger(__name__)

T = TypeVar("T", bound="GatewayControl")


@dataclasses.dataclass
class GatewayControl:
    """
    Base class for gateway control messages.
    """

    @classmethod
    def parse(cls: type[T], line: str) -> T:
        """
        Parse a message of type `T` from a string.

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

        split_tokens = [t.partition("=") for t in line[:-1].split("|")]
        data_dict = {k: v for k, _, v in split_tokens}

        try:
            return cls(**data_dict)
        except TypeError:
            raise ValueError(
                f"`{line.strip()} is not a parsible {cls.__name__}",
            ) from None

    def __str__(self) -> str:
        fields = tuple(map(attrgetter("name"), dataclasses.fields(self)))
        values = tuple(getattr(self, f) for f in fields)
        tokens = "|".join(f"{k}={v}" for k, v in zip(fields, values) if v is not None)
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
    An authentication response is sent by the gateway after a valid
    authentication request is sent to the gateway.
    """

    success: str
    error: str | None = None
    session_id: str | None = None


@dataclasses.dataclass
class AuthenticationRequest(GatewayControl):
    """
    An authentication request is sent to the gateway after a challenge response
    is received.

    This is required to authenticate a user.

    """

    auth: str
    dataset: Dataset | str
    encoding: Encoding = Encoding.DBN
    details: str | None = None
    ts_out: str = "0"
    client: str = USER_AGENT


@dataclasses.dataclass
class SubscriptionRequest(GatewayControl):
    """
    A subscription request is sent to the gateway upon request from the client.
    """

    schema: Schema | str
    stype_in: SType
    symbols: str
    start: int | None = None


@dataclasses.dataclass
class SessionStart(GatewayControl):
    """
    A session start message is sent to the gateway upon request from the
    client.
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


class GatewayDecoder:
    """
    Decoder for gateway control messages.
    """

    def __init__(self) -> None:
        self.__buffer = BytesIO()

    @property
    def buffer(self) -> BytesIO:
        """
        The internal buffer for decoding messages.

        Returns
        -------
        BytesIO

        """
        return self.__buffer

    def write(self, data: bytes) -> None:
        """
        Write data to the decoder's buffer. This will make the data available
        for decoding.

        Parameters
        ----------
        data : bytes
            The data to write.

        """
        self.__buffer.seek(0, 2)  # seek to end
        self.__buffer.write(data)

    def decode(self) -> list[GatewayControl]:
        """
        Decode messages from the decoder's buffer. This will consume decoded
        data from the buffer.

        Returns
        -------
        list[GatewayControl]

        """
        self.__buffer.seek(0)  # rewind
        buffer_lines = self.__buffer.getvalue().splitlines(keepends=True)

        cursor = 0
        messages = []
        for line in buffer_lines:
            if not line.endswith(b"\n"):
                break
            try:
                message = parse_gateway_message(line.decode("utf-8"))
            except ValueError:
                logger.exception("could not parse gateway message: %s", line)
                raise
            else:
                cursor += len(line)
                messages.append(message)

        self.__buffer = BytesIO(self.__buffer.getvalue()[cursor:])
        return messages
