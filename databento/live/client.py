from __future__ import annotations

import asyncio
import logging
import os
import queue
import threading
from collections.abc import Iterable
from concurrent import futures
from datetime import date
from datetime import datetime
from os import PathLike
from typing import IO

import databento_dbn
import pandas as pd
from databento_dbn import Schema
from databento_dbn import SType

from databento.common.constants import ALL_SYMBOLS
from databento.common.cram import BUCKET_ID_LENGTH
from databento.common.enums import ReconnectPolicy
from databento.common.error import BentoError
from databento.common.parsing import optional_datetime_to_unix_nanoseconds
from databento.common.publishers import Dataset
from databento.common.types import ClientRecordCallback
from databento.common.types import ClientStream
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

        self._session._user_callbacks.append(ClientRecordCallback(self._map_symbol))

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
    def session_id(self) -> str | None:
        """
        Return the session ID for the current session. If `None`, the client is
        not connected.

        Returns
        -------
        str | None

        """
        return self._session.session_id

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
            in `record_callback`. If no exception callback is provided,
            any exceptions encountered will be logged and raised as warnings
            for visibility.

        Raises
        ------
        ValueError
            If `record_callback` is not callable.
            If `exception_callback` is not callable.

        See Also
        --------
        Live.add_stream

        """
        client_callback = ClientRecordCallback(
            fn=record_callback,
            exc_fn=exception_callback,
        )

        logger.info("adding user callback %s", client_callback.callback_name)
        self._session._user_callbacks.append(client_callback)

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
            when writing to the stream. If no exception callback is provided,
            any exceptions encountered will be logged and raised as warnings
            for visibility.

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
        client_stream = ClientStream(stream=stream, exc_fn=exception_callback)

        logger.info("adding user stream %s", client_stream.stream_name)
        if self.metadata is not None:
            client_stream.write(self.metadata.encode())
        self._session._user_streams.append(client_stream)

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
        self._session._user_reconnect_callbacks.append(
            (reconnect_callback, exception_callback),
        )

    def start(
        self,
    ) -> None:
        """
        Start the session.

        It is not necessary to call this method before iterating a `Live` client and doing so
        will result in an error.

        Raises
        ------
        ValueError
            If called before a subscription has been made.
            If called after the session has already started.
            If called after the session has closed.

        See Also
        --------
        Live.stop
        Live.terminate

        """
        logger.info("starting live client")
        if not self.is_connected():
            if self.dataset == "":
                raise ValueError("must call subscribe() before starting live client")
            raise ValueError("cannot start a live client after it is closed")
        if self._session.is_streaming():
            raise ValueError("client is already started")

        self._session.start()

    def stop(self) -> None:
        """
        Stop the session and finish processing received records.

        A client can only be stopped after a successful connection is made with `Live.start`.

        This method does not block waiting for the connection to close.

        The connection will eventually close after calling this method. Once the connection
        is closed, the client can be reused, but the session state is not preserved.

        Raises
        ------
        ValueError
            If called before a connection has started.

        See Also
        --------
        Live.terminate
        Live.block_for_close
        Live.wait_for_close

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
        start: pd.Timestamp | datetime | date | str | int | None = None,
        snapshot: bool = False,
    ) -> None:
        """
        Add a new subscription to the session.

        All subscriptions must be for the same `dataset`.

        Multiple subscriptions for different schemas can be made.

        When creating the first subscription, this method will also create
        the TCP connection to the remote gateway.

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
        start : pd.Timestamp, datetime, date, str or int, optional
            The inclusive start of subscription replay.
            Pass `0` to request all available data.
            Cannot be specified after the session is started.
            See `Intraday Replay` https://databento.com/docs/api-reference-live/basics/intraday-replay.
        snapshot: bool, default to 'False'
            Request subscription with snapshot. The `start` parameter must be `None`.
            Only supported with `mbo` schema.

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
        Terminate the session and stop processing records immediately.

        A client can only be terminated after a connection is started with `Live.start`.

        Once terminated, the client can be reused, but the session state
        is not preserved.

        Raises
        ------
        ValueError
            If called before a connection has started.

        See Also
        --------
        Live.stop
        Live.block_for_close
        Live.wait_for_close

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
        close after the remote gateway disconnects, or after `Live.stop` or
        `Live.terminate` are called.

        If a `timeout` is specified, `Live.terminate` will be called when the
        timeout is reached.

        When this method unblocks, the session is guaranteed to be closed.

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
        Live.wait_for_close

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
        session will close when the remote gateway disconnects, or after
        `Live.stop` or `Live.terminate` are called.

        If a `timeout` is specified, `Live.terminate` will be called when the
        timeout is reached.

        When this method unblocks, the session is guaranteed to be closed.

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
        Live.block_for_close

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
