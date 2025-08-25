from __future__ import annotations

import asyncio
import dataclasses
import logging
import queue
import struct
import threading
from collections.abc import Iterable
from functools import partial
from typing import IO
from typing import Final

import databento_dbn
import pandas as pd
from databento_dbn import Schema
from databento_dbn import SType

from databento.common.constants import ALL_SYMBOLS
from databento.common.enums import ReconnectPolicy
from databento.common.error import BentoError
from databento.common.publishers import Dataset
from databento.common.types import DBNRecord
from databento.common.types import ExceptionCallback
from databento.common.types import ReconnectCallback
from databento.common.types import RecordCallback
from databento.live.gateway import SubscriptionRequest
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
        user_callbacks: list[tuple[RecordCallback, ExceptionCallback | None]],
        user_streams: list[tuple[IO[bytes], ExceptionCallback | None]],
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
        self._last_ts_event: int | None = None

    def received_metadata(self, metadata: databento_dbn.Metadata) -> None:
        if self._metadata:
            self._metadata.check(metadata)
        else:
            metadata_bytes = metadata.encode()
            for stream, exc_callback in self._user_streams:
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
        self._last_ts_event = record.ts_event

        return super().received_record(record)

    def _dispatch_callbacks(self, record: DBNRecord) -> None:
        for callback, exc_callback in self._user_callbacks:
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

        for stream, exc_callback in self._user_streams:
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


class LiveSession:
    """
    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop to create the connection in and submit tasks to.
    api_key: str
        The Databento API key for authentication
    ts_out : bool, default False
        Flag for requesting `ts_out` to be appending to all records in the session.
    heartbeat_interval_s: int, optional
        The interval in seconds at which the gateway will send heartbeat records if no
        other data records are sent.
    user_gateway : str, optional
        Override for the remote gateway.
    user_port : int, optional
        Override for the remote port.
    reconnect_policy: ReconnectPolicy | str, optional
        The reconnect policy for the live session.
            - "none": the client will not reconnect (default)
            - "reconnect": the client will reconnect automatically
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        api_key: str,
        ts_out: bool = False,
        heartbeat_interval_s: int | None = None,
        user_gateway: str | None = None,
        user_port: int = DEFAULT_REMOTE_PORT,
        reconnect_policy: ReconnectPolicy | str = ReconnectPolicy.NONE,
    ) -> None:
        self._dbn_queue = DBNQueue()
        self._lock = threading.RLock()
        self._loop = loop
        self._metadata = SessionMetadata()
        self._user_gateway: str | None = user_gateway
        self._user_callbacks: list[tuple[RecordCallback, ExceptionCallback | None]] = []
        self._user_streams: list[tuple[IO[bytes], ExceptionCallback | None]] = []
        self._user_reconnect_callbacks: list[tuple[ReconnectCallback, ExceptionCallback | None]] = (
            []
        )
        self._port: int = user_port

        self._api_key = api_key
        self._ts_out = ts_out
        self._heartbeat_interval_s = heartbeat_interval_s

        self._protocol: _SessionProtocol | None = None
        self._transport: asyncio.Transport | None = None
        self._session_id: str | None = None

        self._subscription_counter = 0
        self._subscriptions: list[SubscriptionRequest] = []
        self._reconnect_policy = ReconnectPolicy(reconnect_policy)
        self._reconnect_task: asyncio.Task[None] | None = None

        self._dataset = ""

    @property
    def api_key(self) -> str:
        return self._api_key

    @property
    def dataset(self) -> str:
        return self._dataset

    @property
    def ts_out(self) -> bool:
        return self._ts_out

    @property
    def heartbeat_interval_s(self) -> int | None:
        return self._heartbeat_interval_s

    @property
    def session_id(self) -> str | None:
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
            if self._reconnect_task is not None:
                return self._reconnect_task.done()
            else:
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

    def resume_reading(self) -> None:
        """
        Resume reading from the connection.
        """
        with self._lock:
            if self._transport is None:
                return
            self._loop.call_soon_threadsafe(self._transport.resume_reading)

    def is_streaming(self) -> bool:
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
            return self._protocol.is_streaming

    def stop(self) -> None:
        """
        Stop the current connection.
        """
        with self._lock:
            if self._transport is None:
                return
            if self._protocol is not None:
                self._protocol.disconnected.add_done_callback(lambda _: self._cleanup())
            self._loop.call_soon_threadsafe(self._transport.close)

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
        if not self.dataset:
            self._dataset = dataset
        elif self.dataset != dataset:
            raise ValueError(
                f"Cannot subscribe to dataset `{dataset}` "
                f"because subscriptions to `{self.dataset}` have already been made.",
            )

        with self._lock:
            if self._protocol is None:
                self._connect(dataset=dataset)

            self._subscription_counter += 1
            self._subscriptions.extend(
                self._protocol.subscribe(
                    schema=schema,
                    symbols=symbols,
                    stype_in=stype_in,
                    start=start,
                    snapshot=snapshot,
                    subscription_id=self._subscription_counter,
                ),
            )

    def terminate(self) -> None:
        with self._lock:
            if self._transport is None:
                return
            self._transport.abort()
            self._cleanup()

    async def wait_for_close(self) -> None:
        """
        Coroutine to wait for the current connection to disconnect and for all
        records to be processed.
        """
        if self._protocol is None:
            return

        try:
            await self._protocol.authenticated
        except Exception as exc:
            raise BentoError(exc) from None

        try:
            if self._reconnect_task is not None:
                await self._reconnect_task
            else:
                await self._protocol.disconnected
        except Exception as exc:
            raise BentoError(exc) from None

        self._cleanup()

    def _cleanup(self) -> None:
        logger.debug("cleaning up session_id=%s", self.session_id)
        self._user_callbacks.clear()
        for item in self._user_streams:
            stream, _ = item
            if not stream.closed:
                stream.flush()

        self._user_callbacks.clear()
        self._user_streams.clear()
        self._user_reconnect_callbacks.clear()
        self._metadata = SessionMetadata()
        self._protocol = self._transport = None
        self._dataset = ""

    def _create_protocol(self, dataset: Dataset | str) -> _SessionProtocol:
        return _SessionProtocol(
            api_key=self.api_key,
            dataset=dataset,
            dbn_queue=self._dbn_queue,
            user_callbacks=self._user_callbacks,
            user_streams=self._user_streams,
            loop=self._loop,
            metadata=self._metadata,
            ts_out=self.ts_out,
            heartbeat_interval_s=self.heartbeat_interval_s,
        )

    def _connect(
        self,
        dataset: Dataset | str,
    ) -> None:
        with self._lock:
            if not self.is_disconnected():
                return
            self._transport, self._protocol = asyncio.run_coroutine_threadsafe(
                coro=self._connect_task(
                    dataset=dataset,
                ),
                loop=self._loop,
            ).result()
            if self._reconnect_policy is not ReconnectPolicy.NONE:
                self._reconnect_task = self._loop.create_task(self._reconnect())

    async def _connect_task(
        self,
        dataset: Dataset | str,
    ) -> tuple[asyncio.Transport, _SessionProtocol]:
        if self._user_gateway is None:
            subdomain = dataset.lower().replace(".", "-")
            gateway = f"{subdomain}.lsg.databento.com"
            logger.debug("using default gateway for dataset %s", dataset)
        else:
            gateway = self._user_gateway
            logger.debug("using user specified gateway: %s", gateway)

        logger.info("connecting to remote gateway")
        try:
            factory = partial(self._create_protocol, dataset=dataset)
            transport, protocol = await asyncio.wait_for(
                self._loop.create_connection(
                    protocol_factory=factory,
                    host=gateway,
                    port=self._port,
                ),
                timeout=CONNECT_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            raise BentoError(
                f"Connection to {gateway}:{self._port} timed out after "
                f"{CONNECT_TIMEOUT_SECONDS} second(s).",
            ) from None
        except OSError as exc:
            raise BentoError(
                f"Connection to {gateway}:{self._port} failed: {exc}",
            ) from None

        logger.debug(
            "connected to %s:%d",
            gateway,
            self._port,
        )

        try:
            session_id = await asyncio.wait_for(
                protocol.authenticated,
                timeout=AUTH_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            raise BentoError(
                f"Authentication with {gateway}:{self._port} timed out after "
                f"{AUTH_TIMEOUT_SECONDS} second(s).",
            ) from None

        self._session_id = session_id
        logger.info(
            "authenticated session %s",
            self.session_id,
        )

        return transport, protocol

    async def _reconnect(self) -> None:
        while True:
            try:
                await self._protocol.disconnected
            except Exception:
                with self._lock:
                    logger.info("reconnecting live session")

                    should_restart = self.is_streaming()
                    if self._protocol._last_ts_event is not None:
                        gap_start = pd.Timestamp(self._protocol._last_ts_event, tz="UTC")
                    elif self._metadata.data is not None:
                        gap_start = pd.Timestamp(self._metadata.data.start, tz="UTC")
                    else:
                        gap_start = pd.Timestamp.utcnow()

                    if self._transport is not None:
                        self._transport.abort()
                    self._transport, self._protocol = await self._connect_task(
                        dataset=self._protocol._dataset,
                    )

                    for sub in self._subscriptions:
                        self._protocol.subscribe(
                            schema=sub.schema,
                            symbols=sub.symbols,
                            stype_in=sub.stype_in,
                            snapshot=bool(sub.snapshot),
                            start=None,
                            subscription_id=sub.id,
                        )

                    if should_restart:
                        self._protocol.start()
                        metadata = await self._protocol._metadata_received
                        gap_end = pd.Timestamp(metadata.start, tz="UTC")
                        logger.debug(
                            "reconnection gap of %f second(s)",
                            (gap_end - gap_start).total_seconds(),
                        )
                        self._dispatch_reconnect_callbacks(
                            gap_start=gap_start,
                            gap_end=gap_end,
                        )
                continue
            else:
                return

    def _dispatch_reconnect_callbacks(
        self,
        gap_start: pd.Timestamp,
        gap_end: pd.Timestamp,
    ) -> None:
        for callback, exc_callback in self._user_reconnect_callbacks:
            try:
                callback(gap_start, gap_end)
            except Exception as exc:
                logger.error(
                    "error dispatching reconnect (%s,%s) to `%s` reconnect callback",
                    gap_start,
                    gap_end,
                    getattr(callback, "__name__", str(callback)),
                    exc_info=exc,
                )
                if exc_callback is not None:
                    exc_callback(exc)
