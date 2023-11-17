from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from functools import singledispatchmethod
from numbers import Number
from typing import Final

import databento_dbn
from databento_dbn import Schema
from databento_dbn import SType
from databento_dbn import VersionUpgradePolicy

from databento.common import cram
from databento.common.constants import ALL_SYMBOLS
from databento.common.error import BentoError
from databento.common.iterator import chunk
from databento.common.parsing import optional_datetime_to_unix_nanoseconds
from databento.common.parsing import optional_symbols_list_to_list
from databento.common.publishers import Dataset
from databento.common.types import DBNRecord
from databento.common.validation import validate_enum
from databento.common.validation import validate_semantic_string
from databento.live.gateway import AuthenticationRequest
from databento.live.gateway import AuthenticationResponse
from databento.live.gateway import ChallengeRequest
from databento.live.gateway import GatewayControl
from databento.live.gateway import GatewayDecoder
from databento.live.gateway import Greeting
from databento.live.gateway import SessionStart
from databento.live.gateway import SubscriptionRequest


RECV_BUFFER_SIZE: Final = 64 * 2**10  # 64kb
SYMBOL_LIST_BATCH_SIZE: Final = 64

logger = logging.getLogger(__name__)


class DatabentoLiveProtocol(asyncio.BufferedProtocol):
    """
    A BufferedProtocol implementation for the Databento live subscription
    gateway. This protocol will handle all gateway control messaging.

    This class can be used directly with `asyncio.loop.create_connection`
    to provide low-level control of the connection. For simple use cases
    it is recommended to create a subclass and implement your own methods
    for `received_metadata` and `received_record`.

    Parameters
    ----------
    api_key : str
        The user API key for authentication.
    dataset : Dataset, or str
        The dataset for authentication.
    ts_out : bool, default False
        Flag for requesting `ts_out` to be appending to all records in the session.

    See Also
    --------
    asyncio.BufferedProtocol

    """

    def __init__(
        self,
        api_key: str,
        dataset: Dataset | str,
        ts_out: bool = False,
    ) -> None:
        self.__api_key = api_key
        self.__transport: asyncio.Transport | None = None
        self.__buffer: bytearray = bytearray(RECV_BUFFER_SIZE)

        self._dataset = validate_semantic_string(dataset, "dataset")
        self._ts_out = ts_out

        self._dbn_decoder = databento_dbn.DBNDecoder(
            upgrade_policy=VersionUpgradePolicy.UPGRADE,
        )
        self._gateway_decoder = GatewayDecoder()

        self._authenticated: asyncio.Future[int] = asyncio.Future()
        self._disconnected: asyncio.Future[None] = asyncio.Future()
        self._started = asyncio.Event()

    @property
    def authenticated(self) -> asyncio.Future[int]:
        """
        Future that completes when authentication with the gateway is
        completed.

        The result will contain the session id if successful.
        The exception will contain a BentoError if authentication
        fails for any reason.

        Returns
        -------
        asyncio.Future[int]

        """
        return self._authenticated

    @property
    def disconnected(self) -> asyncio.Future[None]:
        """
        Future that completes when the connection to the gateway is lost or
        closed.

        The result will contain None if the disconnection was graceful.
        The result will contain an Exception otherwise.

        Returns
        -------
        asyncio.Future[None]

        """
        return self._disconnected

    @property
    def started(self) -> asyncio.Event:
        """
        Event that is set when the session has started streaming. This occurs
        when the SessionStart message is sent to the gateway.

        Returns
        -------
        asyncio.Event

        """
        return self._started

    @property
    def transport(self) -> asyncio.Transport:
        """
        Transport that publishes to this DatbentoLiveProtocol.

        Returns
        -------
        asyncio.Transport

        Raises
        ------
        ValueError
            If the protocol is not connected.

        """
        if self.__transport is None:
            raise ValueError("protocol is not connected")
        return self.__transport

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """
        Override of `connection_made`.

        See Also
        --------
        asycnio.BufferedProtocol.connection_made

        """
        logger.debug("established connection to gateway")
        if not isinstance(transport, asyncio.Transport):
            raise TypeError("Connection does not support read-write operations.")
        self.__transport = transport
        return super().connection_made(transport)

    def connection_lost(self, exc: Exception | None) -> None:
        """
        Override of `connection_lost`.

        See Also
        --------
        asycnio.BufferedProtocol.connection_lost

        """
        if not self.disconnected.done():
            if exc is None:
                logger.info("connection closed")
                self.disconnected.set_result(None)
            else:
                logger.error("connection lost", exc_info=exc)
                self.disconnected.set_exception(exc)
        super().connection_lost(exc)

    def eof_received(self) -> bool | None:
        """
        Override of `eof_received`.

        See Also
        --------
        asycnio.BufferedProtocol.eof_received

        """
        logger.info("received EOF from remote")
        return super().eof_received()

    def get_buffer(self, sizehint: int) -> bytearray:
        """
        Override of `get_buffer`.

        See Also
        --------
        asycnio.BufferedProtocol.get_buffer

        """
        if len(self.__buffer) < sizehint:
            self.__buffer = bytearray(sizehint)
        return self.__buffer

    def buffer_updated(self, nbytes: int) -> None:
        """
        Override of `buffer_updated`.

        See Also
        --------
        asycnio.BufferedProtocol.buffer_updated

        """
        logger.debug("read %d bytes from remote gateway", nbytes)
        data = self.__buffer[:nbytes]

        if self.started.is_set():
            self._process_dbn(data)
        else:
            self._process_gateway(data)

        super().buffer_updated(nbytes)

    def received_metadata(self, metadata: databento_dbn.Metadata) -> None:
        """
        Call when the protocol receives a Metadata header. This is always sent
        by the gateway before any data records.

        Parameters
        ----------
        metadata : databento_dbn.Metadata

        """
        pass

    def received_record(self, record: DBNRecord) -> None:
        """
        Handle when the protocol receives a data record.

        Parameters
        ----------
        record : DBNRecord

        """
        pass

    def subscribe(
        self,
        schema: Schema | str,
        symbols: Iterable[str] | Iterable[Number] | str | Number = ALL_SYMBOLS,
        stype_in: SType | str = SType.RAW_SYMBOL,
        start: str | int | None = None,
    ) -> None:
        """
        Send a SubscriptionRequest to the gateway.

        Parameters
        ----------
        schema : Schema or str
            The schema to subscribe to.
        symbols : Iterable[str | Number] or str or Number, default 'ALL_SYMBOLS'
            The symbols to subscribe to.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
        start : str or int, optional
            UNIX nanosecond epoch timestamp to start streaming from. Must be
            within 24 hours.

        """
        logger.info(
            "sending subscription to %s:%s %s start=%s",
            schema,
            stype_in,
            symbols,
            start if start is not None else "now",
        )
        stype_in_valid = validate_enum(stype_in, SType, "stype_in")
        symbols_list = optional_symbols_list_to_list(symbols, stype_in_valid)

        for batch in chunk(symbols_list, SYMBOL_LIST_BATCH_SIZE):
            batch_str = ",".join(batch)
            message = SubscriptionRequest(
                schema=validate_enum(schema, Schema, "schema"),
                stype_in=stype_in_valid,
                symbols=batch_str,
                start=optional_datetime_to_unix_nanoseconds(start),
            )

            self.transport.write(bytes(message))

    def start(
        self,
    ) -> None:
        """
        Send SessionStart to the gateway.
        """
        logger.debug("sending start")
        message = SessionStart()
        self.started.set()
        self.transport.write(bytes(message))

    def _process_dbn(self, data: bytes) -> None:
        if self.__transport is None:
            raise ValueError("not connected")

        try:
            self._dbn_decoder.write(bytes(data))
            records = self._dbn_decoder.decode()
        except ValueError:
            pass  # expected for partial records
        except Exception:
            logger.exception("error decoding DBN record")
            self.__transport.close()
            raise
        else:
            for record in records:
                logger.debug("dispatching %s", type(record).__name__)
                if isinstance(record, databento_dbn.Metadata):
                    self.received_metadata(record)
                    continue

                if isinstance(record, databento_dbn.ErrorMsg):
                    logger.error(
                        "gateway error: %s",
                        record.err,
                    )
                    self.disconnected.set_exception(
                        BentoError(record.err),
                    )
                if isinstance(record, databento_dbn.SystemMsg):
                    if record.is_heartbeat:
                        logger.debug("gateway heartbeat")
                    else:
                        logger.info(
                            "gateway message: %s",
                            record.msg,
                        )
                self.received_record(record)

    def _process_gateway(self, data: bytes) -> None:
        try:
            self._gateway_decoder.write(data)
            controls = self._gateway_decoder.decode()
        except ValueError:
            logger.exception("error decoding control message")
            self.transport.close()
            raise
        for control in controls:
            self._handle_gateway_message(control)

    @singledispatchmethod
    def _handle_gateway_message(self, message: GatewayControl) -> None:
        """
        Dispatch for GatewayControl messages.

        Parameters
        ----------
        message : GatewayControl
            The message to dispatch.

        """
        logger.error("unhandled gateway message: %s", type(message).__name__)

    @_handle_gateway_message.register(Greeting)
    def _(self, message: Greeting) -> None:
        logger.debug("greeting received by remote gateway v%s", message.lsg_version)

    @_handle_gateway_message.register(ChallengeRequest)
    def _(self, message: ChallengeRequest) -> None:
        logger.debug("received CRAM challenge: %s", message.cram)
        response = cram.get_challenge_response(message.cram, self.__api_key)
        auth_request = AuthenticationRequest(
            auth=response,
            dataset=self._dataset,
            ts_out=str(int(self._ts_out)),
        )
        logger.debug("sending CRAM challenge response: %s", str(auth_request).strip())
        self.transport.write(bytes(auth_request))

    @_handle_gateway_message.register(AuthenticationResponse)
    def _(self, message: AuthenticationResponse) -> None:
        if message.success == "0":
            logger.error("CRAM authentication failed: %s", message.error)
            self.authenticated.set_exception(
                BentoError(f"User authentication failed: {message.error}"),
            )
            self.transport.close()
        else:
            if message.session_id is None:
                session_id = 0
            else:
                session_id = int(message.session_id)

            logger.debug(
                "CRAM authenticated session id assigned `%s`",
                session_id,
            )
            self.authenticated.set_result(session_id)
