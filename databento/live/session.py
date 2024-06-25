from __future__ import annotations

import asyncio
import dataclasses
import logging
import queue
import struct
import threading
from collections.abc import Iterable
from typing import IO
from typing import Callable
from typing import Final

import databento_dbn
from databento_dbn import Schema
from databento_dbn import SType

from databento.common.constants import ALL_SYMBOLS
from databento.common.error import BentoError
from databento.common.publishers import Dataset
from databento.common.types import DBNRecord
from databento.common.types import ExceptionCallback
from databento.common.types import RecordCallback
from databento.live.protocol import DatabentoLiveProtocol


logger = logging.getLogger(__name__)

AUTH_TIMEOUT_SECONDS: Final = 30.0
CONNECT_TIMEOUT_SECONDS: Final = 10.0
DBN_QUEUE_CAPACITY: Final = 2**20
DEFAULT_REMOTE_PORT: Final = 13000


class DBNQueue(queue.SimpleQueue):  # type: ignore [type-arg]
    """
    Queue for DBNRecords that can only be pushed to when enabled.
    """

    def __init__(self) -> None:
        super().__init__()
        self._enabled = threading.Event()

    def is_enabled(self) -> bool:
        """
        Return True if the Queue will allow pushing; False otherwise.

        A queue should only be enabled when it has a consumer.

        """
        return self._enabled.is_set()

    def is_full(self) -> bool:
        """
        Return True when the queue has reached capacity; False otherwise.
        """
        return self.qsize() > DBN_QUEUE_CAPACITY

    def enable(self) -> None:
        """
        Enable the DBN queue for pushing.
        """
        self._enabled.set()

    def disable(self) -> None:
        """
        Disable the DBN queue for pushing.
        """
        self._enabled.clear()

    def put(self, item: DBNRecord, block: bool = True, timeout: float | None = None) -> None:
        """
        Put an item on the queue if the queue is enabled.

        Parameters
        ----------
        item: DBNRecord
            The DBNRecord to put into the queue
        block: bool, default True
            Block if necessary until a free slot is available or the `timeout` is reached
        timeout: float | None, default None
            The maximum amount of time to block, when `block` is True, for the queue to become enabled.

        Raises
        ------
        BentoError
            If the queue is not enabled.
            If the queue is not enabled within `timeout` seconds.

        See Also
        --------
        queue.SimpleQueue.put

        """
        if self._enabled.wait(timeout):
            return super().put(item, block, timeout)
        if timeout is not None:
            raise BentoError(f"queue is not enabled after {timeout} second(s)")
        raise BentoError("queue is not enabled")

    def put_nowait(self, item: DBNRecord) -> None:
        """
        Put an item on the queue, if the queue is enabled, without blocking.

        Parameters
        ----------
        item: DBNRecord
            The DBNRecord to put into the queue

        Raises
        ------
        BentoError
            If the queue is not enabled.

        See Also
        --------
        queue.SimpleQueue.put_nowait

        """
        if self.is_enabled():
            return super().put_nowait(item)
        raise BentoError("queue is not enabled")


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
        heartbeat_interval_s: int | None = None,
    ):
        super().__init__(api_key, dataset, ts_out, heartbeat_interval_s)

        self._dbn_queue = dbn_queue
        self._loop = loop
        self._metadata: SessionMetadata = metadata
        self._user_callbacks = user_callbacks
        self._user_streams = user_streams

    def received_metadata(self, metadata: databento_dbn.Metadata) -> None:
        if self._metadata:
            self._metadata.check(metadata)
        else:
            metadata_bytes = metadata.encode()
            for stream, exc_callback in self._user_streams.items():
                try:
                    stream.write(metadata_bytes)
                except Exception as exc:
                    stream_name = getattr(stream, "name", str(stream))
                    logger.error(
                        "error writing %d bytes to `%s` stream",
                        len(metadata_bytes),
                        stream_name,
                        exc_info=exc,
                    )
                    if exc_callback is not None:
                        exc_callback(exc)

            self._metadata.data = metadata
        return super().received_metadata(metadata)

    def received_record(self, record: DBNRecord) -> None:
        self._dispatch_writes(record)
        self._dispatch_callbacks(record)
        if self._dbn_queue.is_enabled():
            self._queue_for_iteration(record)

        return super().received_record(record)

    def _dispatch_callbacks(self, record: DBNRecord) -> None:
        for callback, exc_callback in self._user_callbacks.items():
            try:
                callback(record)
            except Exception as exc:
                logger.error(
                    "error dispatching %s to `%s` callback",
                    type(record).__name__,
                    getattr(callback, "__name__", str(callback)),
                    exc_info=exc,
                )
                if exc_callback is not None:
                    exc_callback(exc)

    def _dispatch_writes(self, record: DBNRecord) -> None:
        if hasattr(record, "ts_out"):
            ts_out_bytes = struct.pack("Q", record.ts_out)
        else:
            ts_out_bytes = b""

        record_bytes = bytes(record) + ts_out_bytes

        for stream, exc_callback in self._user_streams.items():
            try:
                stream.write(record_bytes)
            except Exception as exc:
                stream_name = getattr(stream, "name", str(stream))
                logger.error(
                    "error writing %d bytes to `%s` stream",
                    len(record_bytes),
                    stream_name,
                    exc_info=exc,
                )
                if exc_callback is not None:
                    exc_callback(exc)

    def _queue_for_iteration(self, record: DBNRecord) -> None:
        self._dbn_queue.put(record)
        # DBNQueue has no max size; so check if it's above capacity, and if so, pause reading
        if self._dbn_queue.is_full():
            logger.warning(
                "record queue is full; %d record(s) to be processed",
                self._dbn_queue.qsize(),
            )
            self.transport.pause_reading()


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
        self._session_id: str | None = None

    @property
    def session_id(self) -> str | None:
        """
        Return the authenticated session ID. A None value indicates no session
        has started.

        Returns
        -------
        str | None

        """
        return self._session_id

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
        Return true if the session's connection has started streaming, false
        otherwise.

        Returns
        -------
        bool

        """
        with self._lock:
            if self._protocol is None:
                return False
            return self._protocol.is_started

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
            else:
                self._loop.call_soon_threadsafe(self._transport.close)

    def subscribe(
        self,
        dataset: Dataset | str,
        schema: Schema | str,
        symbols: Iterable[str | int] | str | int = ALL_SYMBOLS,
        stype_in: SType | str = SType.RAW_SYMBOL,
        start: str | int | None = None,
        snapshot: bool = False,
    ) -> None:
        """
        Send a subscription request on the current connection. This will create
        a new connection if there is no active connection to the gateway.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset for the subscription.
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
                snapshot=snapshot,
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

        try:
            self._protocol.authenticated.result()
        except Exception as exc:
            raise BentoError(exc) from None

        try:
            self._protocol.disconnected.result()
        except Exception as exc:
            raise BentoError(exc) from None

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
            session_id = await asyncio.wait_for(
                protocol.authenticated,
                timeout=AUTH_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            raise BentoError(
                f"Authentication with {gateway}:{port} timed out after "
                f"{AUTH_TIMEOUT_SECONDS} second(s).",
            ) from None

        self._session_id = session_id
        logger.info(
            "authenticated session %s",
            self.session_id,
        )

        return transport, protocol
