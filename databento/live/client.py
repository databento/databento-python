import asyncio
import atexit
import logging
import os
import threading
from concurrent import futures
from typing import IO, Callable, Iterable, Optional, Tuple, Union

from databento.common.cram import BUCKET_ID_LENGTH
from databento.common.enums import Dataset, Schema, SType
from databento.common.error import BentoError
from databento.common.parsing import (
    optional_datetime_to_unix_nanoseconds,
    optional_symbols_list_to_string,
)
from databento.common.symbology import ALL_SYMBOLS
from databento.common.validation import validate_enum, validate_semantic_string
from databento.live.data import RecordPipeline
from databento.live.dbn import DBNProtocol, DBNStruct
from databento.live.gateway import GatewayProtocol, SessionStart, SubscriptionRequest


logger = logging.getLogger(__name__)


AUTH_TIMEOUT_SECONDS: int = 5
CONNECT_TIMEOUT_SECONDS: int = 5

UserCallback = Callable[[DBNStruct], None]


@atexit.register
def _() -> None:
    loop = Live._loop
    thread = Live._thread

    logger.info("shutting down event loop in %s", thread.name)

    # Stop the loop and block
    loop.stop()
    if thread.is_alive():
        thread.join(timeout=1)


class Connection:
    """
    A single TCP connection to the live service gateway.

    Parameters
    ----------
    gateway : str, optional
        The remote gateway to connect to; for advanced use.
    port : int, optional
        The remote port to connect to; for advanced use.
    protocol_factory : Callable[[], GatewayProtocol]
        A factory function for the GatewayProtocol.
    record_pipeline : RecordPipeline
        The record pipeline to attach this connection to.
    timeout : float, optional
        A duration in seconds to wait for a connection to be made before
        aborting.

    Raises
    ------
    ValueError
        If the user API key is invalid.
        If a specified parameter is invalid.
    BentoError
        If the connection to the gateway fails.

    """

    def __init__(
        self,
        gateway: str,
        port: int,
        protocol_factory: Callable[[], GatewayProtocol],
        record_pipeline: RecordPipeline,
        timeout: float = CONNECT_TIMEOUT_SECONDS,
    ) -> None:
        self._gateway = gateway
        self._port = port

        transport, protocol = asyncio.run_coroutine_threadsafe(
            coro=self._connect(
                protocol_factory=protocol_factory,
                record_pipeline=record_pipeline,
                timeout=timeout,
            ),
            loop=Live._loop,
        ).result()

        self._transport: asyncio.Transport = transport
        self._protocol: DBNProtocol = protocol

    @property
    def gateway(self) -> str:
        return self._gateway

    @property
    def port(self) -> int:
        return self._port

    def abort(self) -> None:
        self._transport.abort()

    def close(self) -> None:
        if self._transport.can_write_eof():
            self._transport.write_eof()
        self._transport.close

    def is_closed(self) -> bool:
        return self._transport.is_closing()

    def write(self, message: bytes) -> None:
        self._transport.write(message)

    async def _connect(
        self,
        protocol_factory: Callable[[], GatewayProtocol],
        record_pipeline: RecordPipeline,
        timeout: float,
    ) -> Tuple[asyncio.Transport, DBNProtocol]:
        logger.info("connecting to remote gateway")
        try:
            transport, protocol = await asyncio.wait_for(
                Live._loop.create_connection(
                    protocol_factory=protocol_factory,
                    host=self.gateway,
                    port=self.port,
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError as exc:
            raise BentoError(
                f"Connection to {self.gateway}:{self.port} timed out after "
                f"{timeout} second(s).",
            )
        except OSError as exc:
            raise BentoError(
                f"Connection to {self.gateway}:{self.port} failed.",
            )

        logger.debug(
            "connected to %s:%d",
            self.gateway,
            self.port,
        )

        try:
            await asyncio.wait_for(
                protocol.authenticated,
                timeout=timeout,
            )
        except asyncio.TimeoutError as exc:
            raise BentoError(
                f"Authentication with {self.gateway}:{self.port} timed out after "
                f"{CONNECT_TIMEOUT_SECONDS} second(s).",
            )
        except ValueError as exc:
            raise BentoError(f"User authentication failed: {str(exc)}")

        logger.info(
            "authentication with remote gateway completed",
        )

        dbn_protocol = DBNProtocol(
            loop=Live._loop,
            transport=transport,
            client_callback=record_pipeline._publish,
        )
        transport.set_protocol(dbn_protocol)

        logger.debug("ready to receive DBN stream")

        return transport, dbn_protocol


class Live:
    """
    A managed TCP connection to the Databento Live Subscription Gateway.

    Parameters
    ----------
    key : str, optional
        The user API key for authentication.
    gateway : str, optional
        The remote gateway to connect to; for advanced use.
    port : int, optional
        The remote port to connect to; for advanced use.
    ts_out: bool, default True
        If set, DBN records will be timestamped when they are sent by the
        gateway.

    Raises
    ------
    ValueError
        If the user API key is invalid.
        If a specified parameter is invalid.
    BentoError
        If the connection to the gateway fails.

    """

    _loop = asyncio.new_event_loop()
    _thread = threading.Thread(
        target=_loop.run_forever,
        name="databento_live",
        daemon=True,
    )

    def __init__(
        self,
        key: Optional[str] = None,
        gateway: Optional[str] = None,
        port: Optional[int] = None,
        ts_out: bool = True,
    ) -> None:
        if key is None:
            key = os.environ.get("DATABENTO_API_KEY")
        if key is None or not isinstance(key, str) or key.isspace():
            raise ValueError(f"invalid API key, was {key}")
        self._key: str = key

        if gateway is not None:
            gateway = validate_semantic_string(gateway, "gateway")
        self._gateway: Optional[str] = gateway

        if port is None:
            self._port: int = 13000
        else:
            if not isinstance(port, int):
                raise ValueError(f"port must be a valid integer, was `{port}`")
            self._port = port

        self._connection: Optional[Connection] = None
        self._dataset: Optional[Union[Dataset, str]] = None
        self._record_pipeline = RecordPipeline(loop=Live._loop)
        self._started: bool = False
        self._ts_out = ts_out

        if not Live._thread.is_alive():
            Live._thread.start()

    def __aiter__(self) -> DBNProtocol:
        return iter(self)

    def __iter__(self) -> DBNProtocol:
        logger.debug("starting iteration")
        if self._connection is None:
            raise ValueError("cannot iterate before connecting")
        if not isinstance(self._connection._protocol, DBNProtocol):
            raise ValueError("cannot iterate before starting")
        return iter(self._connection._protocol)

    def __repr__(self) -> str:
        name = self.__class__.__name__
        return (
            f"<{name}(dataset={self.dataset}, "
            f"key=****{self._key[-BUCKET_ID_LENGTH:]}>"
        )

    @property
    def dataset(self) -> Optional[str]:
        """
        Return the dataset for this live client.

        Returns
        -------
        str or None

        """
        return self._dataset

    @property
    def gateway(self) -> Optional[str]:
        """
        Return the gateway for this live client.

        Returns
        -------
        str

        """
        return self._gateway

    def is_connected(self) -> bool:
        """
        True if the live client is connected.

        Returns
        -------
        bool

        """
        if self._connection is None:
            return False
        return not self._connection.is_closed()

    @property
    def key(self) -> str:
        """
        Returns the user API key for this live client.

        Returns
        -------
        str

        """
        return self._key

    @property
    def port(self) -> int:
        """
        Return the port for this live client.

        Returns
        -------
        int

        """
        return self._port

    @property
    def ts_out(self) -> bool:
        """
        Returns the value of the ts_out flag.

        Returns
        -------
        bool

        """
        return self._ts_out

    def add_callback(
        self,
        func: UserCallback,
    ) -> None:
        """
        Add a callback for handling records.

        Parameters
        ----------
        func : Callable[[DBNStruct], None]
            A callback to register for handling live records as they arrive.

        Raises
        ------
        ValueError
            If `func` is not callable.

        See Also
        --------
        Live.add_stream

        """
        if not callable(func):
            raise ValueError(f"{func} is not callable")

        self._record_pipeline.add_callback(func)

    def add_stream(self, stream: IO[bytes]) -> None:
        """
        Add an IO stream to write records to.

        Parameters
        ----------
        stream : IO[bytes]
            The IO stream to write to when handling live records as they arrive.

        Raises
        ------
        ValueError
            If `stream` is not a writable byte stream.

        See Also
        --------
        Live.add_callback

        """
        if not hasattr(stream, "write"):
            raise ValueError(f"{type(stream).__name__} does not support write()")

        if not hasattr(stream, "writable") or not stream.writable():
            raise ValueError(f"{type(stream).__name__} is not a writable stream")

        self._record_pipeline.add_stream(stream)

    def start(
        self,
    ) -> None:
        """
        Start the live client session.

        Raises
        ------
        ValueError
            If `start()` is called before a subscription has been made.
            If `start()` is called after streaming has already started.

        See Also
        --------
        Live.stop

        """
        logger.info("starting live client")
        if self._connection is None:
            raise ValueError("cannot start a live client before it has connected")
        if not self.is_connected():
            raise ValueError("cannot start a live client after it is closed")
        if self._started:
            raise ValueError("client is already started")

        # Send the start message
        request = SessionStart()
        logger.debug(
            "sending session start: %s",
            str(request).strip(),
        )
        self._connection._transport.write(bytes(request))
        self._started = True

    def stop(self) -> None:
        """
        Stop the live client session as soon as possible.
        Once stopped, a client cannot be restarted.

        Raises
        ------
        ValueError
            If `stop()` is called before a connection has been made.

        See Also
        --------
        Live.start

        """
        logger.info("stopping live client")
        if self._connection is None:
            raise ValueError("cannot stop a live client before it has connected")

        if not self.is_connected():
            return  # we're already stopped

        self._connection.close()

    def subscribe(
        self,
        dataset: Union[Dataset, str],
        schema: Union[Schema, str],
        symbols: Union[Iterable[str], Iterable[int], str, int] = ALL_SYMBOLS,
        stype_in: Union[SType, str] = SType.RAW_SYMBOL,
        start: Optional[Union[str, int]] = None,
        timeout: float = CONNECT_TIMEOUT_SECONDS,
    ) -> None:
        """
        Subscribe to a data stream.
        Multiple subscription requests can be made for a streaming session.
        Once one subscription has been made, future subscriptions must all
        belong to the same dataset.

        When creating the first subscription this method will also create
        the TCP connection to the remote gateway. All subscriptions must
        have the same dataset.

        Parameters
        ----------
        dataset : Dataset, str
            The dataset for the subscription.
        schema : Schema or str
            The schema to subscribe to.
        symbols : Iterable[Union[str, int]] or str, default 'ALL_SYMBOLS'
            The symbols to subscribe to.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
        start : str or int, optional
            UNIX nanosecond epoch timestamp to start streaming from. Must be
            within 24 hours.
        timeout : float, optional
            A duration in seconds to wait for a connection to be made before
            aborting.

        Raises
        ------
        ValueError
            If a dataset is given that does not match the previous datasets.
        BentoError
            If creating the connection times out.
            If creating the connection fails.
            If authentication with the gateway times out.
        ValueError
            If authentication with the gateway fails.

        See Also
        --------
        Live.start

        """
        logger.info(
            "subscribing to %s:%s %s start=%s",
            schema,
            stype_in,
            symbols,
            start if start is not None else "now",
        )
        dataset = validate_semantic_string(dataset, "dataset")
        schema = validate_enum(schema, Schema, "schema")
        stype_in = validate_enum(stype_in, SType, "stype_in")
        symbols = optional_symbols_list_to_string(symbols, stype_in)
        start = optional_datetime_to_unix_nanoseconds(start)

        if self.dataset is not None and self.dataset != dataset:
            raise ValueError(
                f"Cannot subscribe to dataset `{dataset}` "
                f"because subscriptions to `{self.dataset}` have already been made.",
            )

        connection = self._get_connection(
            dataset=dataset,
            timeout=timeout,
        )

        request = SubscriptionRequest(
            schema=schema,
            stype_in=stype_in,
            symbols=symbols,
            start=start,
        )

        logger.debug(
            "sending session subscription: %s",
            str(request).strip(),
        )

        connection.write(bytes(request))

    def terminate(self) -> None:
        """
        Terminate the live client session and stop processing records as soon as
        possible. Once stopped, a client cannot be restarted.

        See Also
        --------
        Live.stop

        """
        logger.info("terminating live client")
        if self._connection is None:
            raise ValueError("cannot terminate a live client before it is connected")

        self._connection.abort()
        self._record_pipeline.abort()

    def block_for_close(
        self,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Block until the session closes or a timeout is reached. A session will
        close after `stop()` is called or the remote gateway disconnects.

        Parameters
        ----------
        timeout : float, optional
            The duration in seconds to wait for the live client to close.
            If unspecified or None, wait forever.

        Raises
        ------
        BentoError
            If the connection is terminated unexpectedly.
        ValueError
            If the client has never connected.

        See Also
        --------
        wait_for_close

        """
        if self._connection is None:
            raise ValueError("cannot block_for_close before connecting")

        if not self.is_connected():
            return

        try:
            asyncio.run_coroutine_threadsafe(
                self._shutdown(),
                loop=Live._loop,
            ).result(timeout=timeout)
        except (futures.TimeoutError, KeyboardInterrupt) as exc:
            logger.info("terminating session due to %s", type(exc).__name__)
            self.terminate()
            if isinstance(exc, KeyboardInterrupt):
                raise
        except Exception as exc:
            logger.exception("exception encountered blocking for close")
            raise BentoError("connection lost")

    async def wait_for_close(
        self,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Coroutine to wait until the session closes or a timeout is reached.
        A session will close after `stop()` is called or the remote gateway
        disconnects.

        Parameters
        ----------
        timeout : float, optional
            The duration in seconds to wait for the live client to close.
            If unspecified or None, wait forever.

        Raises
        ------
        BentoError
            If the connection is terminated unexpectedly.
        ValueError
            If the client has never connected.

        See Also
        --------
        block_for_close

        """
        if self._connection is None:
            raise ValueError("cannot wait_for_close before connecting")

        if not self.is_connected():
            return

        waiter = asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(
                self._shutdown(),
                loop=Live._loop,
            ),
        )

        try:
            await asyncio.wait_for(waiter, timeout=timeout)
        except (asyncio.TimeoutError, KeyboardInterrupt) as exc:
            logger.info("terminating session due to %s", type(exc).__name__)
            self.terminate()
        except Exception as exc:
            logger.exception("exception encountered waiting for close")
            raise BentoError("connection lost")

    def _get_connection(
        self,
        dataset: Union[str, Dataset],
        timeout: float = CONNECT_TIMEOUT_SECONDS,
    ) -> Connection:
        """
        Get a valid connection for a dataset. This will set the dataset
        attribute.
        """
        if self._connection is None:
            logger.debug("client dataset assigned to %s", dataset)
            self._dataset = dataset
            if self.gateway is None:
                subdomain = self._dataset.lower().replace(".", "-")
                gateway: str = f"{subdomain}.lsg.databento.com"
                logger.debug("using gateway for dataset %s", self._dataset)
            else:
                gateway = self.gateway
                logger.debug("using user specified gateway: %s", gateway)

            self._connection = Connection(
                gateway=gateway,
                port=self.port,
                protocol_factory=self._protocol_factory(dataset),
                record_pipeline=self._record_pipeline,
                timeout=timeout,
            )

        return self._connection

    def _protocol_factory(self, dataset: str) -> Callable[[], GatewayProtocol]:
        def factory() -> GatewayProtocol:
            return GatewayProtocol(
                key=self._key,
                dataset=dataset,
                ts_out=self.ts_out,
            )

        return factory

    async def _shutdown(self) -> None:
        """
        Coroutine to wait for a graceful shutdown.
        This waits for protocol disconnection and all records to complete
        processing.
        """
        if self._connection is not None:
            await self._connection._protocol.disconnected
        await self._record_pipeline.wait_for_processing()
