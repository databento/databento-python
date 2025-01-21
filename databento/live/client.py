from __future__ import annotations

import asyncio
import logging
import os
import pathlib
import queue
import threading
from collections.abc import Iterable
from concurrent import futures
from os import PathLike
from typing import IO

import databento_dbn
from databento_dbn import Schema
from databento_dbn import SType

from databento.common.constants import ALL_SYMBOLS
from databento.common.cram import BUCKET_ID_LENGTH
from databento.common.enums import ReconnectPolicy
from databento.common.error import BentoError
from databento.common.parsing import optional_datetime_to_unix_nanoseconds
from databento.common.publishers import Dataset
from databento.common.types import DBNRecord
from databento.common.types import ExceptionCallback
from databento.common.types import ReconnectCallback
from databento.common.types import RecordCallback
from databento.common.validation import validate_enum
from databento.common.validation import validate_semantic_string
from databento.live.session import DEFAULT_REMOTE_PORT
from databento.live.session import LiveSession
from databento.live.session import SessionMetadata


logger = logging.getLogger(__name__)


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
    heartbeat_interval_s: int, optional
        The interval in seconds at which the gateway will send heartbeat records if no
        other data records are sent.
    reconnect_policy: ReconnectPolicy | str, optional
        The reconnect policy for the live session.
            - "none": the client will not reconnect (default)
            - "reconnect": the client will reconnect automatically

    """

    _loop = asyncio.new_event_loop()
    _lock = threading.Lock()
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
        heartbeat_interval_s: int | None = None,
        reconnect_policy: ReconnectPolicy | str = ReconnectPolicy.NONE,
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
        self._heartbeat_interval_s = heartbeat_interval_s

        self._metadata: SessionMetadata = SessionMetadata()
        self._symbology_map: dict[int, str | int] = {}

        self._session: LiveSession = LiveSession(
            loop=self._loop,
            api_key=key,
            ts_out=ts_out,
            heartbeat_interval_s=heartbeat_interval_s,
            user_gateway=self._gateway,
            user_port=port,
            reconnect_policy=reconnect_policy,
        )

        self._session._user_callbacks.append((self._map_symbol, None))

        with Live._lock:
            if not Live._thread.is_alive():
                Live._thread.start()

    def __del__(self) -> None:
        try:
            self.terminate()
        except (AttributeError, ValueError):
            pass

    def __aiter__(self) -> LiveIterator:
        return iter(self)

    def __iter__(self) -> LiveIterator:
        logger.debug("starting iteration")
        if self._session.is_streaming():
            logger.error("iteration started after session has started")
            raise ValueError(
                "Cannot start iteration after streaming has started, records may be missed. Don't call `Live.start` before iterating.",
            )
        return LiveIterator(self)

    def __repr__(self) -> str:
        name = self.__class__.__name__
        return f"<{name}(dataset={self.dataset}, key=****{self._key[-BUCKET_ID_LENGTH:]}>"

    @property
    def dataset(self) -> str:
        """
        Return the dataset for this live client. If no subscriptions have been
        made an empty string will be returned.

        Returns
        -------
        str

        """
        return self._session.dataset

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
        The DBN metadata header for this session, or `None` if the metadata has
        not been received yet.

        Returns
        -------
        databento_dbn.Metadata or None

        """
        if not self._session._metadata:
            return None
        return self._session._metadata.data

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
    def symbology_map(self) -> dict[int, str | int]:
        """
        Return the symbology map for this client session. A symbol mapping is
        added when the client receives a SymbolMappingMsg.

        This can be used to transform an `instrument_id` in a DBN record
        to the input symbology.

        Returns
        -------
        dict[int, str | int]
            A mapping of the exchange's instrument_id to the subscription symbology.

        """
        return self._symbology_map

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
        record_callback: RecordCallback,
        exception_callback: ExceptionCallback | None = None,
    ) -> None:
        """
        Add a callback for handling records.

        Parameters
        ----------
        record_callback : Callable[[DBNRecord], None]
            A callback to register for handling live records as they arrive.
        exception_callback : Callable[[Exception], None], optional
            An error handling callback to process exceptions that are raised
            in `record_callback`.

        Raises
        ------
        ValueError
            If `record_callback` is not callable.
            If `exception_callback` is not callable.

        See Also
        --------
        Live.add_stream

        """
        if not callable(record_callback):
            raise ValueError(f"{record_callback} is not callable")

        if exception_callback is not None and not callable(exception_callback):
            raise ValueError(f"{exception_callback} is not callable")

        callback_name = getattr(record_callback, "__name__", str(record_callback))
        logger.info("adding user callback %s", callback_name)
        self._session._user_callbacks.append((record_callback, exception_callback))

    def add_stream(
        self,
        stream: IO[bytes] | PathLike[str] | str,
        exception_callback: ExceptionCallback | None = None,
    ) -> None:
        """
        Add an IO stream to write records to.

        Parameters
        ----------
        stream : IO[bytes] or PathLike[str] or str
            The IO stream to write to when handling live records as they arrive.
        exception_callback : Callable[[Exception], None], optional
            An error handling callback to process exceptions that are raised
            when writing to the stream.

        Raises
        ------
        ValueError
            If `stream` is not a writable byte stream.
        OSError
            If `stream` is not a path to a writeable file.
            If `stream` is a path to a file that exists.

        See Also
        --------
        Live.add_callback

        """
        if isinstance(stream, (str, PathLike)):
            stream = pathlib.Path(stream).open("xb")

        if not hasattr(stream, "write"):
            raise ValueError(f"{type(stream).__name__} does not support write()")

        if not hasattr(stream, "writable") or not stream.writable():
            raise ValueError(f"{type(stream).__name__} is not a writable stream")

        if exception_callback is not None and not callable(exception_callback):
            raise ValueError(f"{exception_callback} is not callable")

        stream_name = getattr(stream, "name", str(stream))
        logger.info("adding user stream %s", stream_name)
        if self.metadata is not None:
            stream.write(bytes(self.metadata))
        self._session._user_streams.append((stream, exception_callback))

    def add_reconnect_callback(
        self,
        reconnect_callback: ReconnectCallback,
        exception_callback: ExceptionCallback | None = None,
    ) -> None:
        """
        Add a callback for handling client reconnection events. This will only
        be called when using a reconnection policy other than
        `ReconnectPolicy.NONE` and if the session has been started with
        `Live.start`.

        Two instances of `pandas.Timestamp` will be passed to the callback:
            - The last `ts_event` or `Metadata.start` value from the disconnected session.
            - The `Metadata.start` value of the reconnected session.

        Parameters
        ----------
        reconnect_callback : Callable[[ReconnectCallback], None]
            A callback to register for handling reconnection events.
        exception_callback : Callable[[Exception], None], optional
            An error handling callback to process exceptions that are raised
            in `reconnect_callback`.

        Raises
        ------
        ValueError
            If `reconnect_callback` is not callable.
            If `exception_callback` is not callable.

        """
        if not callable(reconnect_callback):
            raise ValueError(f"{reconnect_callback} is not callable")

        if exception_callback is not None and not callable(exception_callback):
            raise ValueError(f"{exception_callback} is not callable")

        callback_name = getattr(reconnect_callback, "__name__", str(reconnect_callback))
        logger.info("adding user reconnect callback %s", callback_name)
        self._session._user_reconnect_callbacks.append((reconnect_callback, exception_callback))

    def start(
        self,
    ) -> None:
        """
        Start the live client session.

        It is not necessary to call `Live.start` before iterating a `Live` client and doing so will result in an error.

        Raises
        ------
        ValueError
            If `Live.start` is called before a subscription has been made.
            If `Live.start` is called after streaming has already started.
            If `Live.start` is called after the live session has closed.

        See Also
        --------
        Live.stop

        """
        logger.info("starting live client")
        if not self.is_connected():
            if self.dataset == "":
                raise ValueError("cannot start a live client without a subscription")
            raise ValueError("cannot start a live client after it is closed")
        if self._session.is_streaming():
            raise ValueError("client is already started")

        self._session.start()

    def stop(self) -> None:
        """
        Stop the live client session as soon as possible. Once stopped, a
        client cannot be restarted.

        Raises
        ------
        ValueError
            If `Live.stop` is called before a connection has been made.

        See Also
        --------
        Live.start

        """
        logger.info("stopping live client")
        if self._session is None:
            raise ValueError("cannot stop a live client before it has connected")

        if not self.is_connected():
            return  # we're already stopped

        self._session.stop()

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
        symbols : Iterable[str | int] or str or int, default 'ALL_SYMBOLS'
            The symbols to subscribe to.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
        start : str or int, optional
            UNIX nanosecond epoch timestamp to start streaming from (inclusive), based on `ts_event`. Must be within 24 hours except when requesting the mbo or definition schemas.
        snapshot: bool, default to 'False'
            Request subscription with snapshot. The `start` parameter must be `None`.



        Raises
        ------
        ValueError
            If a dataset is given that does not match the previous datasets.
            If snapshot is True and start is not None.
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
            "subscribing to %s:%s %s start=%s snapshot=%s",
            schema,
            stype_in,
            symbols,
            start if start is not None else "now",
            snapshot,
        )
        dataset = validate_semantic_string(dataset, "dataset")
        schema = validate_enum(schema, Schema, "schema")
        stype_in = validate_enum(stype_in, SType, "stype_in")
        start = optional_datetime_to_unix_nanoseconds(start)

        if snapshot and start is not None:
            raise ValueError("Subscription with snapshot expects start=None")

        self._session.subscribe(
            dataset=dataset,
            schema=schema,
            stype_in=stype_in,
            symbols=symbols,
            start=start,
            snapshot=snapshot,
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
        self._session.terminate()

    def block_for_close(
        self,
        timeout: float | None = None,
    ) -> None:
        """
        Block until the session closes or a timeout is reached. A session will
        close after `Live.stop` is called or the remote gateway disconnects.

        If a `timeout` is specified, `Live.stop` will be called when the
        timeout is reached.

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
                self._session.wait_for_close(),
                loop=Live._loop,
            ).result(timeout=timeout)
        except (futures.TimeoutError, KeyboardInterrupt) as exc:
            logger.info("closing session due to %s", type(exc).__name__)
            self.terminate()
            if isinstance(exc, KeyboardInterrupt):
                raise
        except BentoError:
            raise
        except Exception:
            raise BentoError("connection lost") from None

    async def wait_for_close(
        self,
        timeout: float | None = None,
    ) -> None:
        """
        Coroutine to wait until the session closes or a timeout is reached. A
        session will close after `Live.stop` is called or the remote gateway
        disconnects.

        If a `timeout` is specified, `Live.stop` will be called when the
        timeout is reached.

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
                self._session.wait_for_close(),
                loop=Live._loop,
            ),
        )

        try:
            await asyncio.wait_for(waiter, timeout=timeout)
        except (asyncio.TimeoutError, KeyboardInterrupt) as exc:
            logger.info("closing session due to %s", type(exc).__name__)
            self.terminate()
            if isinstance(exc, KeyboardInterrupt):
                raise
        except BentoError:
            raise
        except Exception:
            logger.exception("exception encountered waiting for close")
            raise BentoError("connection lost") from None

    def _map_symbol(self, record: DBNRecord) -> None:
        if isinstance(record, databento_dbn.SymbolMappingMsg):
            out_symbol = record.stype_out_symbol
            instrument_id = record.instrument_id
            self._symbology_map[instrument_id] = record.stype_out_symbol
            logger.info("added symbology mapping %s to %d", out_symbol, instrument_id)


class LiveIterator:
    """
    Iterator class for the `Live` client. Automatically starts the client when
    created and will stop it when destroyed. This provides context-manager-like
    behavior to for loops.

    Parameters
    ----------
    client : Live
        The Live client that spawned this LiveIterator.

    """

    def __init__(self, client: Live):
        client.start()
        self._dbn_queue = client._session._dbn_queue
        self._dbn_queue.enable()
        self._client = client

    @property
    def client(self) -> Live:
        return self._client

    def __iter__(self) -> LiveIterator:
        return self

    def __del__(self) -> None:
        try:
            self.client.terminate()
            logger.debug("iteration aborted")
        except ValueError:
            pass

    async def __anext__(self) -> DBNRecord:
        if not self._dbn_queue.is_enabled():
            raise ValueError("iteration has not started")

        loop = asyncio.get_running_loop()

        try:
            return self._dbn_queue.get_nowait()
        except queue.Empty:
            while True:
                try:
                    return await loop.run_in_executor(
                        None,
                        self._dbn_queue.get,
                        True,
                        0.1,
                    )
                except queue.Empty:
                    if self.client._session.is_disconnected():
                        break
        finally:
            if not self._dbn_queue.is_full() and not self.client._session.is_reading():
                logger.debug(
                    "resuming reading with %d pending records",
                    self._dbn_queue.qsize(),
                )
                self.client._session.resume_reading()

        self._dbn_queue.disable()
        await self.client.wait_for_close()
        logger.debug("async iteration completed")
        raise StopAsyncIteration

    def __next__(self) -> DBNRecord:
        if not self._dbn_queue.is_enabled():
            raise ValueError("iteration has not started")

        while True:
            try:
                record = self._dbn_queue.get(timeout=0.1)
            except queue.Empty:
                if self.client._session.is_disconnected():
                    break
            else:
                return record
            finally:
                if not self._dbn_queue.is_full() and not self.client._session.is_reading():
                    logger.debug(
                        "resuming reading with %d pending records",
                        self._dbn_queue.qsize(),
                    )
                    self.client._session.resume_reading()

        self._dbn_queue.disable()
        self.client.block_for_close()
        logger.debug("iteration completed")
        raise StopIteration
