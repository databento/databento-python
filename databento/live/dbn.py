import asyncio
import logging
import queue
import threading
from concurrent import futures
from typing import Callable, NewType, Optional, Union

import databento_dbn


logger = logging.getLogger(__name__)


DEFAULT_QUEUE_SIZE = 1024
MIN_BUFFER_SIZE = 64 * 1024  # 64kB

DBNRecord = Union[
    databento_dbn.MBOMsg,
    databento_dbn.MBP1Msg,
    databento_dbn.MBP10Msg,
    databento_dbn.TradeMsg,
    databento_dbn.OHLCVMsg,
    databento_dbn.ImbalanceMsg,
    databento_dbn.InstrumentDefMsg,
    databento_dbn.StatMsg,
    databento_dbn.SymbolMappingMsg,
    databento_dbn.SystemMsg,
    databento_dbn.ErrorMsg,
]
DBNStruct = Union[DBNRecord, databento_dbn.Metadata]
DBNQueue = NewType("DBNQueue", "queue.Queue[DBNStruct]")


class DBNProtocol(asyncio.BufferedProtocol):
    """
    The DBN protocol for the Databento Live Subscription Gateway.
    This protocol supports decoding an uncompressed stream of DBN records and
    performing iteration.

    Parameters
    ----------
    client_callback : Callable[[DBNRecord], None]
        The callback to dispatch deserialized DBN records to.

    See Also
    --------
    `asyncio.BufferedProtocol`

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        transport: asyncio.Transport,
        client_callback: Callable[[DBNStruct], None],
    ) -> None:
        self._buffer: bytearray = bytearray()
        self._client_callback = client_callback
        self._dbn_queue: Optional[DBNQueue] = None
        self._decoder: databento_dbn.DbnDecoder = databento_dbn.DbnDecoder()
        self._disconnected: "asyncio.Future[None]" = loop.create_future()
        self._transport_lock = threading.Lock()
        self._transport = transport

    def __aiter__(self) -> "DBNProtocol":
        return iter(self)

    def __iter__(self) -> "DBNProtocol":
        if self._dbn_queue is None:
            self._dbn_queue = queue.Queue(DEFAULT_QUEUE_SIZE * 2)  # type: ignore
        return self

    async def __anext__(self) -> DBNStruct:
        if self._dbn_queue is None:
            raise ValueError("iteration has not started")

        while not self._transport.is_closing() or not self._dbn_queue.empty():
            try:
                await asyncio.sleep(0.001)
                record = self._dbn_queue.get(block=False)
            except queue.Empty:
                continue
            else:
                logger.debug(
                    "yielding %s record from async-next",
                    type(record).__name__,
                )
                self._dbn_queue.task_done()
                return record
            finally:
                if not self.is_queue_full() and not self._transport.is_reading():
                    logger.debug(
                        "resuming reading with %d pending records",
                        self._dbn_queue.qsize(),
                    )
                    with self._transport_lock:
                        self._transport.resume_reading()

        raise StopAsyncIteration()

    def __next__(self) -> DBNStruct:
        if self._dbn_queue is None:
            raise ValueError("iteration has not started")

        while not self._transport.is_closing() or not self._dbn_queue.empty():
            try:
                record = self._dbn_queue.get(timeout=0.001)
            except futures.TimeoutError:
                continue
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
                if not self.is_queue_full() and not self._transport.is_reading():
                    logger.debug(
                        "resuming reading with %d pending records",
                        self._dbn_queue.qsize(),
                    )
                    with self._transport_lock:
                        self._transport.resume_reading()

        raise StopIteration()

    @property
    def disconnected(self) -> "asyncio.Future[None]":
        """
        A future that completes when the connection is lost.
        An exception indicates the session was terminated remotely
        without receiving the EOF.

        Returns
        -------
        asyncio.Future

        """
        return self._disconnected

    def is_queue_full(self) -> bool:
        """
        True is the internal message queue is full.
        Note that the queue is considered full when it is
        at half capacity to cushion large payloads.

        Returns
        -------
        bool

        """
        if self._dbn_queue is None:
            return False
        return self._dbn_queue.qsize() >= (self._dbn_queue.maxsize // 2)

    def get_buffer(self, size_hint: int) -> bytearray:
        self._buffer = bytearray(max(size_hint, MIN_BUFFER_SIZE))
        return self._buffer

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if exc is None:
            logger.debug("connection closed")
            self._disconnected.set_result(None)
        else:
            logger.exception("connection lost", exc_info=exc)
            self._disconnected.set_exception(exc)

    def buffer_updated(self, nbytes: int) -> None:
        logger.debug("read %d bytes from remote", nbytes)
        record_bytes = bytes(self._buffer[:nbytes])

        try:
            self._decoder.write(record_bytes)
        except ValueError:
            logger.critical("could not write to dbn decoder")
            with self._transport_lock:
                self._transport.close()

        try:
            records = self._decoder.decode()
        except ValueError:
            pass
        else:
            for record, ts_out in records:
                header = getattr(record, "hd", object())
                ts_event = getattr(header, "ts_event", None)
                logger.info(
                    "decoded as %s record ts_event=%s ts_out=%s",
                    type(record).__name__,
                    ts_event,
                    ts_out,
                )

                if not isinstance(record, databento_dbn.Metadata):
                    setattr(record, "ts_out", ts_out)

                # Record Dispatch
                self._client_callback(record)

                # Iteration
                if self._dbn_queue is not None:
                    try:
                        self._dbn_queue.put_nowait(record)
                    except queue.Full:
                        logger.error(
                            "record queue is full; dropped %s record ts_event=%s",
                            type(record).__name__,
                            ts_event,
                        )
                    else:
                        if self.is_queue_full():
                            logger.warning(
                                "record queue is full; %d record(s) to be processed",
                                self._dbn_queue.qsize(),
                            )
                            with self._transport_lock:
                                self._transport.pause_reading()
