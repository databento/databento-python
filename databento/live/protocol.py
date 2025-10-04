from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from functools import singledispatchmethod
from typing import Final

import databento_dbn
from databento_dbn import DBNError
from databento_dbn import Metadata
from databento_dbn import Schema
from databento_dbn import SType
from databento_dbn import SystemCode
from databento_dbn import VersionUpgradePolicy

from databento.common import cram
from databento.common.constants import ALL_SYMBOLS
from databento.common.error import BentoError
from databento.common.iterator import chunk
from databento.common.parsing import optional_datetime_to_unix_nanoseconds
from databento.common.parsing import symbols_list_to_list
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
SYMBOL_LIST_BATCH_SIZE: Final = 500

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
    dataset : Dataset or str
        The dataset for authentication.
    ts_out : bool, default False
        Flag for requesting `ts_out` to be appended to all records in the session.
    heartbeat_interval_s: int, optional
        The interval in seconds at which the gateway will send heartbeat records if no
        other data records are sent.

    See Also
    --------
    asyncio.BufferedProtocol

    """

    def __init__(
        self,
        api_key: str,
        dataset: Dataset | str,
        ts_out: bool = False,
        heartbeat_interval_s: int | None = None,
    ) -> None:
        self.__api_key = api_key
        self.__transport: asyncio.Transport | None = None
        self.__buffer: bytearray = bytearray(RECV_BUFFER_SIZE)

        self._dataset = validate_semantic_string(dataset, "dataset")
        self._ts_out = ts_out
        self._heartbeat_interval_s = heartbeat_interval_s

        self._dbn_decoder = databento_dbn.DBNDecoder(
            upgrade_policy=VersionUpgradePolicy.UPGRADE_TO_V3,
        )
        self._gateway_decoder = GatewayDecoder()

        self._authenticated: asyncio.Future[str | None] = asyncio.Future()
        self._disconnected: asyncio.Future[None] = asyncio.Future()
        self._metadata_received: asyncio.Future[Metadata] = asyncio.Future()
        self._error_msgs: list[str] = []
        self._started: bool = False

    @property
    def authenticated(self) -> asyncio.Future[str | None]:
        """
        Future that completes when authentication with the gateway is
        completed.

        The result will contain the session ID if successful.
        The exception will contain a BentoError if authentication
        fails for any reason.

        Returns
        -------
        asyncio.Future[str | None]

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
    def metadata_received(self) -> asyncio.Future[Metadata]:
        """
        Future that completes when the session metadata had been received.

        Returns
        -------
        asyncio.Future[Metadata]

        """
        return self._metadata_received

    @property
    def is_streaming(self) -> bool:
        """
        True if the session has started streaming. This occurs when the
        SessionStart message is sent to the gateway.

        Returns
        -------
        bool

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
        asyncio.BufferedProtocol.connection_made

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
        asyncio.BufferedProtocol.connection_lost

        """
        super().connection_lost(exc)
        if not self.disconnected.done():
            if self._error_msgs:
                error_msg = ", ".join(self._error_msgs)
                if len(self._error_msgs) > 1:
                    error_msg = f"The following errors occurred: {error_msg}"
                self._error_msgs.clear()

                logger.error("gateway error: %s", exc)
                self.disconnected.set_exception(BentoError(error_msg))
            elif exc is not None:
                logger.error("connection lost: %s", exc)
                self.disconnected.set_exception(exc)
            else:
                logger.info("connection closed")
                self.disconnected.set_result(None)

    def eof_received(self) -> bool | None:
        """
        Override of `eof_received`.

        See Also
        --------
        asyncio.BufferedProtocol.eof_received

        """
        logger.info("received EOF from remote")
        return super().eof_received()

    def get_buffer(self, sizehint: int) -> bytearray:
        """
        Override of `get_buffer`.

        See Also
        --------
        asyncio.BufferedProtocol.get_buffer

        """
        if len(self.__buffer) < sizehint:
            self.__buffer = bytearray(sizehint)
        return self.__buffer

    def buffer_updated(self, nbytes: int) -> None:
        """
        Override of `buffer_updated`.

        See Also
        --------
        asyncio.BufferedProtocol.buffer_updated

        """
        logger.debug("read %d bytes from remote gateway", nbytes)
        data = self.__buffer[:nbytes]

        if self.authenticated.done():
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
        self._metadata_received.set_result(metadata)

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
        symbols: Iterable[str | int] | str | int = ALL_SYMBOLS,
        stype_in: SType | str = SType.RAW_SYMBOL,
        start: str | int | None = None,
        snapshot: bool = False,
        subscription_id: int | None = None,
    ) -> list[SubscriptionRequest]:
        """
        Send a SubscriptionRequest to the gateway. Returns a list of all
        subscription requests sent to the gateway.

        Parameters
        ----------
        schema : Schema or str
            The schema to subscribe to.
        symbols : Iterable[str | int] or str or int, default 'ALL_SYMBOLS'
            The symbols to subscribe to.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
        start : str or int, optional
            UNIX nanosecond epoch timestamp to start streaming from. Must be
            within 24 hours.
        snapshot: bool, default to 'False'
            Request subscription with snapshot. The `start` parameter must be `None`.
        subscription_id : int, optional
            A numerical identifier to associate with this subscription.

        Returns
        -------
        list[SubscriptionRequest]

        """
        logger.info(
            "sending subscription to %s:%s %s start=%s snapshot=%s",
            schema,
            stype_in,
            symbols,
            start if start is not None else "now",
            snapshot,
        )

        stype_in_valid = validate_enum(stype_in, SType, "stype_in")
        symbols_list = symbols_list_to_list(symbols, stype_in_valid)

        subscriptions: list[SubscriptionRequest] = []
        chunked_symbols = list(chunk(symbols_list, SYMBOL_LIST_BATCH_SIZE))
        last_chunk_idx = len(chunked_symbols) - 1
        for i, batch in enumerate(chunked_symbols):
            batch_str = ",".join(batch)
            message = SubscriptionRequest(
                schema=validate_enum(schema, Schema, "schema"),
                stype_in=stype_in_valid,
                symbols=batch_str,
                start=optional_datetime_to_unix_nanoseconds(start),
                snapshot=int(snapshot),
                id=subscription_id,
                is_last=int(i == last_chunk_idx),
            )
            subscriptions.append(message)

        self.transport.writelines(map(bytes, subscriptions))
        return subscriptions

    def start(
        self,
    ) -> None:
        """
        Send SessionStart to the gateway.
        """
        logger.debug("sending start")
        message = SessionStart()
        self._started = True
        self.transport.write(bytes(message))

    def _process_dbn(self, data: bytes) -> None:
        if self.__transport is None:
            raise ValueError("not connected")

        try:
            self._dbn_decoder.write(bytes(data))
            records = self._dbn_decoder.decode()
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
                    self._error_msgs.append(record.err)
                elif isinstance(record, databento_dbn.SystemMsg):
                    if record.is_heartbeat():
                        logger.debug("gateway heartbeat")
                    else:
                        try:
                            msg_code = record.code
                        except DBNError:
                            msg_code = None
                        if msg_code == SystemCode.SLOW_READER_WARNING:
                            logger.warning(
                                record.msg,
                            )
                        else:
                            logger.debug(
                                "gateway message: %s",
                                record.msg,
                            )
                self.received_record(record)

    def _process_gateway(self, data: bytes) -> None:
        try:
            self._gateway_decoder.write(data)
            controls = self._gateway_decoder.decode()
        except Exception:
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
            heartbeat_interval_s=self._heartbeat_interval_s,
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
            session_id = message.session_id

            logger.debug(
                "CRAM authenticated session id assigned `%s`",
                session_id,
            )
            self.authenticated.set_result(session_id)
