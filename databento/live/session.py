from __future__ import annotations

import asyncio
import dataclasses
import logging
import queue
import struct
import threading
from collections.abc import Iterable
from numbers import Number
from typing import IO, Callable

import databento_dbn
from databento_dbn import Schema
from databento_dbn import SType

from databento.common.error import BentoError
from databento.common.publishers import Dataset
from databento.common.symbology import ALL_SYMBOLS
from databento.live import AUTH_TIMEOUT_SECONDS
from databento.live import CONNECT_TIMEOUT_SECONDS
from databento.live import DBNRecord
from databento.live import ExceptionCallback
from databento.live import RecordCallback
from databento.live.protocol import DatabentoLiveProtocol


logger = logging.getLogger(__name__)


DEFAULT_REMOTE_PORT = 13000


class DBNQueue(queue.Queue):  # type: ignore [type-arg]
    """
    Queue for DBNRecords that can only be pushed to when enabled.

    Parameters
    ----------
    maxsize : int
        The `maxsize` for the Queue.

    """

    def __init__(self, maxsize: int) -> None:
        super().__init__(maxsize)
        self._enabled = threading.Event()

    @property
    def enabled(self) -> bool:
        """
        True if the Queue will allow pushing.

        A queue should only be enabled when it has a consumer.

        """
        return self._enabled.is_set()

    def half_full(self) -> bool:
        """
        Return True when the queue has reached half capacity.
        """
        with self.mutex:
            return self._qsize() > self.maxsize // 2


@dataclasses.dataclass
class SessionMetadata:
    """
    Container for session Metadata.

    Parameters
    ----------
    data : databento_dbn.Metadata, optional
        The encapsulated metadata.

    """

    data: databento_dbn.Metadata | None = dataclasses.field(default=None)

    def __bool__(self) -> bool:
        return self.data is not None

    def check(self, other: databento_dbn.Metadata) -> None:
        """
        Verify the Metadata is compatible with another Metadata message. This
        is used to ensure DBN streams are compatible with one another.

        Parameters
        ----------
        other : databento_dbn.Metadata

        Raises
        ------
        ValueError
            If the Metadatas are incompatible

        """
        checked_attributes = [
            "version",
            "dataset",
            "schema",
            "limit",
            "stype_in",
            "stype_out",
            "ts_out",
            "symbols",
            "partial",
            "not_found",
            "mappings",
        ]
        diffs = []
        for attribute in checked_attributes:
            if getattr(self.data, attribute) != getattr(other, attribute):
                diffs.append(attribute)
        if diffs:
            raise ValueError(f"incompatible metadata fields {', '.join(diffs)}")


class _SessionProtocol(DatabentoLiveProtocol):
    def __init__(
        self,
        api_key: str,
        dataset: Dataset | str,
        dbn_queue: DBNQueue,
        user_callbacks: dict[RecordCallback, ExceptionCallback | None],
        user_streams: dict[IO[bytes], ExceptionCallback | None],
        loop: asyncio.AbstractEventLoop,
        metadata: SessionMetadata,
        ts_out: bool = False,
    ):
        super().__init__(api_key, dataset, ts_out)

        self._dbn_queue = dbn_queue
        self._loop = loop
        self._metadata: SessionMetadata = metadata
        self._tasks: set[asyncio.Task[None]] = set()
        self._user_callbacks = user_callbacks
        self._user_streams = user_streams

    def received_metadata(self, metadata: databento_dbn.Metadata) -> None:
        if not self._metadata:
            self._metadata.data = metadata
            for stream, exc_callback in self._user_streams.items():
                task = self._loop.create_task(
                    self._stream_task(stream, metadata, exc_callback),
                )
                task.add_done_callback(self._tasks.remove)
                self._tasks.add(task)
        else:
            self._metadata.check(metadata)
        return super().received_metadata(metadata)

    def received_record(self, record: DBNRecord) -> None:
        for callback, exc_callback in self._user_callbacks.items():
            task = self._loop.create_task(
                self._callback_task(callback, record, exc_callback),
            )
            task.add_done_callback(self._tasks.remove)
            self._tasks.add(task)

        for stream, exc_callback in self._user_streams.items():
            task = self._loop.create_task(
                self._stream_task(stream, record, exc_callback),
            )
            task.add_done_callback(self._tasks.remove)
            self._tasks.add(task)

        if self._dbn_queue.enabled:
            try:
                self._dbn_queue.put_nowait(record)
            except queue.Full:
                logger.error(
                    "record queue is full; dropped %s record ts_event=%s",
                    type(record).__name__,
                    record.ts_event,
                )
            else:
                if self._dbn_queue.half_full():
                    logger.warning(
                        "record queue is full; %d record(s) to be processed",
                        self._dbn_queue._qsize(),
                    )
                    self.transport.pause_reading()

        return super().received_record(record)

    async def _callback_task(
        self,
        record_callback: RecordCallback,
        record: DBNRecord,
        exception_callback: ExceptionCallback | None,
    ) -> None:
        try:
            record_callback(record)
        except Exception as exc:
            logger.error(
                "error dispatching %s to `%s` callback",
                type(record).__name__,
                getattr(record_callback, "__name__", str(record_callback)),
                exc_info=exc,
            )
            if exception_callback is not None:
                self._loop.call_soon_threadsafe(exception_callback, exc)

    async def _stream_task(
        self,
        stream: IO[bytes],
        record: databento_dbn.Metadata | DBNRecord,
        exc_callback: ExceptionCallback | None,
    ) -> None:
        has_ts_out = self._metadata.data and self._metadata.data.ts_out
        try:
            stream.write(bytes(record))
            if not isinstance(record, databento_dbn.Metadata) and has_ts_out:
                stream.write(struct.pack("Q", record.ts_out))
        except Exception as exc:
            stream_name = getattr(stream, "name", str(stream))
            logger.error(
                "error writing %s to `%s` stream",
                type(record).__name__,
                stream_name,
                exc_info=exc,
            )
            if exc_callback is not None:
                self._loop.call_soon_threadsafe(exc_callback, exc)

    async def wait_for_processing(self) -> None:
        while self._tasks:
            logger.info(
                "waiting for %d record(s) to process",
                len(self._tasks),
            )
            await asyncio.gather(*self._tasks)


class Session:
    """
    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop to create the connection in and submit tasks to.
    protocol_factory : Callable[[], _SessionProtocol]
        The factory to use for creating connections in this session.
    user_gateway : str, optional
        Override for the remote gateway.
    port : int, optional
        Override for the remote port.
    ts_out : bool, default False
        Flag for requesting `ts_out` to be appending to all records in the session.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        protocol_factory: Callable[[], _SessionProtocol],
        user_gateway: str | None = None,
        port: int = DEFAULT_REMOTE_PORT,
        ts_out: bool = False,
    ) -> None:
        self._lock = threading.RLock()
        self._loop = loop
        self._ts_out = ts_out
        self._protocol_factory = protocol_factory

        self._transport: asyncio.Transport | None = None
        self._protocol: _SessionProtocol | None = None

        self._user_gateway: str | None = user_gateway
        self._port = port

    def is_authenticated(self) -> bool:
        """
        Return true if the session's connection is authenticated.

        Returns
        -------
        bool

        """
        with self._lock:
            if self._protocol is None:
                return False
            try:
                self._protocol.authenticated.result()
            except (asyncio.InvalidStateError, asyncio.CancelledError, BentoError):
                return False
            return True

    def is_disconnected(self) -> bool:
        """
        Return true if the session's connection is closed.

        Returns
        -------
        bool

        """
        with self._lock:
            if self._protocol is None:
                return True
            return self._protocol.disconnected.done()

    def is_reading(self) -> bool:
        """
        Return true if the session's connection is reading.

        Returns
        -------
        bool

        """
        with self._lock:
            if self._transport is None:
                return False
            return self._transport.is_reading()

    def is_started(self) -> bool:
        """
        Return true if the session's connection has started streaming.

        Returns
        -------
        bool

        """
        with self._lock:
            if self._protocol is None:
                return False
            return self._protocol.started.is_set()

    @property
    def metadata(self) -> databento_dbn.Metadata | None:
        """
        Return the current session's Metadata.

        Returns
        -------
        databento_dbn.Metadata

        """
        with self._lock:
            if self._protocol is None:
                return None
            return self._protocol._metadata.data

    def abort(self) -> None:
        """
        Abort the current connection immediately. Buffered data will be lost.

        See Also
        --------
        Session.close

        """
        with self._lock:
            if self._transport is None:
                return
            self._transport.abort()
            self._protocol = None

    def close(self) -> None:
        """
        Close the current connection.
        """
        with self._lock:
            if self._transport is None:
                return
            if self._transport.can_write_eof():
                self._loop.call_soon_threadsafe(self._transport.write_eof)
            self._loop.call_soon_threadsafe(self._transport.close)

    def subscribe(
        self,
        dataset: Dataset | str,
        schema: Schema | str,
        symbols: Iterable[str] | Iterable[Number] | str | Number = ALL_SYMBOLS,
        stype_in: SType | str = SType.RAW_SYMBOL,
        start: str | int | None = None,
    ) -> None:
        """
        Send a subscription request on the current connection. This will create
        a new connection if there is no active connection to the gateway.

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

        """
        with self._lock:
            if self._protocol is None:
                self._connect(
                    dataset=dataset,
                    port=self._port,
                    loop=self._loop,
                )

            self._protocol.subscribe(
                schema=schema,
                symbols=symbols,
                stype_in=stype_in,
                start=start,
            )

    def resume_reading(self) -> None:
        """
        Resume reading from the connection.
        """
        with self._lock:
            if self._transport is None:
                return
            self._loop.call_soon_threadsafe(self._transport.resume_reading)

    def start(self) -> None:
        """
        Send the start message on the current connection.

        Raises
        ------
        ValueError
            If there is no connection.

        """
        with self._lock:
            if self._protocol is None:
                raise ValueError("session is not connected")
            self._protocol.start()

    async def wait_for_close(self) -> None:
        """
        Coroutine to wait for the current connection to disconnect and for all
        records to be processed.
        """
        if self._protocol is None:
            return

        await self._protocol.authenticated
        await self._protocol.disconnected
        await self._protocol.wait_for_processing()

        try:
            self._protocol.authenticated.result()
        except Exception as exc:
            raise BentoError(exc)

        try:
            self._protocol.disconnected.result()
        except Exception as exc:
            raise BentoError(exc)

        self._protocol = self._transport = None

    def _connect(
        self,
        dataset: Dataset | str,
        port: int,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        with self._lock:
            if not self.is_disconnected():
                return
            if self._user_gateway is None:
                subdomain = dataset.lower().replace(".", "-")
                gateway = f"{subdomain}.lsg.databento.com"
                logger.debug("using default gateway for dataset %s", dataset)
            else:
                gateway = self._user_gateway
                logger.debug("using user specified gateway: %s", gateway)

            self._transport, self._protocol = asyncio.run_coroutine_threadsafe(
                coro=self._connect_task(
                    gateway=gateway,
                    port=port,
                ),
                loop=loop,
            ).result()

    async def _connect_task(
        self,
        gateway: str,
        port: int,
    ) -> tuple[asyncio.Transport, _SessionProtocol]:
        logger.info("connecting to remote gateway")
        try:
            transport, protocol = await asyncio.wait_for(
                self._loop.create_connection(
                    protocol_factory=self._protocol_factory,
                    host=gateway,
                    port=port,
                ),
                timeout=CONNECT_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            raise BentoError(
                f"Connection to {gateway}:{port} timed out after "
                f"{CONNECT_TIMEOUT_SECONDS} second(s).",
            ) from None
        except OSError as exc:
            raise BentoError(
                f"Connection to {gateway}:{port} failed: {exc}",
            ) from None

        logger.debug(
            "connected to %s:%d",
            gateway,
            port,
        )

        try:
            await asyncio.wait_for(
                protocol.authenticated,
                timeout=AUTH_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            raise BentoError(
                f"Authentication with {gateway}:{port} timed out after "
                f"{AUTH_TIMEOUT_SECONDS} second(s).",
            ) from None
        except ValueError as exc:
            raise BentoError(f"User authentication failed: {exc!s}") from None

        logger.info(
            "authentication with remote gateway completed",
        )

        return transport, protocol
