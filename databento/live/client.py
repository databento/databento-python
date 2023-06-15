from __future__ import annotations

import asyncio
import logging
import os
import queue
import threading
from collections.abc import Iterable
from concurrent import futures
from numbers import Number
from typing import IO, Callable

import databento_dbn

from databento.common.cram import BUCKET_ID_LENGTH
from databento.common.enums import Dataset
from databento.common.enums import Schema
from databento.common.enums import SType
from databento.common.error import BentoError
from databento.common.parsing import optional_datetime_to_unix_nanoseconds
from databento.common.parsing import optional_symbols_list_to_string
from databento.common.symbology import ALL_SYMBOLS
from databento.common.validation import validate_enum
from databento.common.validation import validate_semantic_string
from databento.live import DBNRecord
from databento.live.session import DEFAULT_REMOTE_PORT
from databento.live.session import DBNQueue
from databento.live.session import Session
from databento.live.session import SessionMetadata
from databento.live.session import _SessionProtocol


logger = logging.getLogger(__name__)

UserCallback = Callable[[DBNRecord], None]

DEFAULT_QUEUE_SIZE = 2048


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
    ts_out: bool, default False
        If set, DBN records will be timestamped when they are sent by the
        gateway.

    """

    _loop = asyncio.new_event_loop()
    _thread = threading.Thread(
        target=_loop.run_forever,
        name="databento_live",
        daemon=True,
    )

    def __init__(
        self,
        key: str | None = None,
        gateway: str | None = None,
        port: int = DEFAULT_REMOTE_PORT,
        ts_out: bool = False,
    ) -> None:
        if key is None:
            key = os.environ.get("DATABENTO_API_KEY")
        if key is None or not isinstance(key, str) or key.isspace():
            raise ValueError(f"invalid API key, was {key}")
        self._key: str = key

        if gateway is not None:
            gateway = validate_semantic_string(gateway, "gateway")
        self._gateway: str | None = gateway

        if not isinstance(port, int):
            raise ValueError(f"port must be a valid integer, was `{port}`")
        self._port = port

        self._dataset: Dataset | str = ""
        self._ts_out = ts_out

        self._dbn_queue: DBNQueue = DBNQueue(maxsize=DEFAULT_QUEUE_SIZE)
        self._metadata: SessionMetadata = SessionMetadata()
        self._user_callbacks: list[UserCallback] = []
        self._user_streams: list[IO[bytes]] = []

        def factory() -> _SessionProtocol:
            return _SessionProtocol(
                api_key=self._key,
                dataset=self._dataset,
                dbn_queue=self._dbn_queue,
                user_callbacks=self._user_callbacks,
                user_streams=self._user_streams,
                loop=self._loop,
                metadata=self._metadata,
                ts_out=self._ts_out,
            )

        self._session: Session = Session(
            loop=self._loop,
            protocol_factory=factory,
            user_gateway=self._gateway,
            port=self._port,
            ts_out=ts_out,
        )

        if not Live._thread.is_alive():
            Live._thread.start()

    def __aiter__(self) -> Live:
        return iter(self)

    async def __anext__(self) -> DBNRecord:
        try:
            return next(self)
        except StopIteration:
            raise StopAsyncIteration

    def __iter__(self) -> Live:
        logger.debug("starting iteration")
        self._dbn_queue._enabled.set()
        return self

    def __next__(self) -> DBNRecord:
        if self._dbn_queue is None:
            raise ValueError("iteration has not started")

        while not self._session.is_disconnected() or self._dbn_queue._qsize() > 0:
            try:
                record = self._dbn_queue.get(block=False)
            except queue.Empty:
                continue
            else:
                logger.debug(
                    "yielding %s record from next",
                    type(record).__name__,
                )
                self._dbn_queue.task_done()
                return record
            finally:
                if not self._dbn_queue.half_full() and not self._session.is_reading():
                    logger.debug(
                        "resuming reading with %d pending records",
                        self._dbn_queue._qsize(),
                    )
                    self._session.resume_reading()

        self._dbn_queue._enabled.clear()
        raise StopIteration

    def __repr__(self) -> str:
        name = self.__class__.__name__
        return (
            f"<{name}(dataset={self.dataset}, "
            f"key=****{self._key[-BUCKET_ID_LENGTH:]}>"
        )

    @property
    def dataset(self) -> str:
        """
        Return the dataset for this live client. If no subscriptions have been
        made an empty string will be returned.

        Returns
        -------
        str

        """
        return self._dataset

    @property
    def gateway(self) -> str | None:
        """
        Return the gateway for this live client.

        Returns
        -------
        str or None

        """
        return self._gateway

    @property
    def metadata(self) -> databento_dbn.Metadata | None:
        """
        The DBN metadata header for this session, or `None` if the
        metadata has not been received yet.

        Returns
        -------
        databento_dbn.Metadata or None

        """
        if not self._metadata:
            return None
        return self._metadata.data

    def is_connected(self) -> bool:
        """
        Return True if the live client is connected.

        Returns
        -------
        bool

        """
        return not self._session.is_disconnected()

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
        func : Callable[[DBNRecord], None]
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
        callback_name = getattr(func, "__name__", str(func))
        logger.info("adding user callback %s", callback_name)
        self._user_callbacks.append(func)

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

        stream_name = getattr(stream, "name", str(stream))
        logger.info("adding user stream %s", stream_name)
        if self.metadata is not None:
            stream.write(bytes(self.metadata))
        self._user_streams.append(stream)

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
        if not self.is_connected():
            raise ValueError("cannot start a live client after it is closed")
        if self._session.is_started():
            raise ValueError("client is already started")

        self._session.start()

    def stop(self) -> None:
        """
        Stop the live client session as soon as possible. Once stopped, a
        client cannot be restarted.

        Raises
        ------
        ValueError
            If `stop()` is called before a connection has been made.

        See Also
        --------
        Live.start

        """
        logger.info("stopping live client")
        if self._session is None:
            raise ValueError("cannot stop a live client before it has connected")

        if not self.is_connected():
            return  # we're already stopped

        self._session.close()

    def subscribe(
        self,
        dataset: Dataset | str,
        schema: Schema | str,
        symbols: Iterable[str] | Iterable[Number] | str | Number = ALL_SYMBOLS,
        stype_in: SType | str = SType.RAW_SYMBOL,
        start: str | int | None = None,
    ) -> None:
        """
        Subscribe to a data stream. Multiple subscription requests can be made
        for a streaming session. Once one subscription has been made, future
        subscriptions must all belong to the same dataset.

        When creating the first subscription this method will also create
        the TCP connection to the remote gateway. All subscriptions must
        have the same dataset.

        Parameters
        ----------
        dataset : Dataset, str
            The dataset for the subscription.
        schema : Schema or str
            The schema to subscribe to.
        symbols : Iterable[str | Number] or str or Number, default 'ALL_SYMBOLS'
            The symbols to subscribe to.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
        start : str or int, optional
            UNIX nanosecond epoch timestamp to start streaming from. Must be
            within 24 hours.

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

        if not self.dataset:
            self._dataset = dataset
        elif self.dataset != dataset:
            raise ValueError(
                f"Cannot subscribe to dataset `{dataset}` "
                f"because subscriptions to `{self.dataset}` have already been made.",
            )
        self._session.subscribe(
            dataset=dataset,
            schema=schema,
            stype_in=stype_in,
            symbols=symbols,
            start=start,
        )

    def terminate(self) -> None:
        """
        Terminate the live client session and stop processing records as soon
        as possible.

        Raises
        ------
        ValueError
            If the client is not connected.

        See Also
        --------
        Live.stop

        """
        logger.info("terminating live client")
        if self._session is None:
            raise ValueError("cannot terminate a live client before it is connected")
        self._session.abort()

    def block_for_close(
        self,
        timeout: float | None = None,
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
            raise BentoError("connection lost") from exc

    async def wait_for_close(
        self,
        timeout: float | None = None,
    ) -> None:
        """
        Coroutine to wait until the session closes or a timeout is reached. A
        session will close after `stop()` is called or the remote gateway
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
            if isinstance(exc, KeyboardInterrupt):
                raise
        except Exception as exc:
            logger.exception("exception encountered waiting for close")
            raise BentoError("connection lost") from exc

    async def _shutdown(self) -> None:
        """
        Coroutine to wait for a graceful shutdown.

        This waits for protocol disconnection and all records to
        complete processing.

        """
        if self._session is None:
            return
        await self._session.wait_for_close()
