import asyncio
import logging
from typing import IO, Callable, List, Set

import databento_dbn
from databento.live.dbn import DBNStruct


logger = logging.getLogger(__name__)

DEFAULT_QUEUE_SIZE: int = 2048

UserCallback = Callable[[DBNStruct], None]


class RecordPipeline:
    """
    Manager for dispatching DBN records to callbacks and
    writing to streams.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop the schedule with.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._loop: asyncio.AbstractEventLoop = loop
        self._user_callbacks: List[UserCallback] = []
        self._user_streams: List[IO[bytes]] = []
        self._tasks: Set["asyncio.Task[None]"] = set()
        self._aborted: bool = False

    def __del__(self) -> None:
        # Try to cancel tasks if we get garbage collected
        self.abort()

    def add_callback(
        self,
        func: UserCallback,
    ) -> None:
        callback_name = getattr(func, "__name__", str(func))
        logger.info("adding user callback %s", callback_name)
        self._user_callbacks.append(func)

    def add_stream(self, stream: IO[bytes]) -> None:
        stream_name = getattr(stream, "name", str(stream))
        logger.info("adding user stream %s", stream_name)
        self._user_streams.append(stream)

    async def _callback_task(
        self,
        func: UserCallback,
        record: DBNStruct,
    ) -> None:
        try:
            func(record)
        except Exception as exc:
            logger.error(
                "error dispatching %s to `%s` callback",
                type(record).__name__,
                func.__name__,
                exc_info=exc,
            )

    async def _stream_task(
        self,
        stream: IO[bytes],
        record: DBNStruct,
    ) -> None:
        try:
            stream.write(bytes(record))
        except Exception as exc:
            stream_name = getattr(stream, "name", str(stream))
            logger.error(
                "error writing %s to `%s` stream",
                type(record).__name__,
                stream_name,
                exc_info=exc,
            )

    def _task_complete(
        self,
        task: "asyncio.Task[None]",
    ) -> None:
        self._tasks.remove(task)

    def _publish(self, record: DBNStruct) -> None:
        if self._aborted:
            return  # Do not process if we are aborting
        if isinstance(record, databento_dbn.SystemMsg):
            logger.info("received system message: %s", record.msg)
        elif isinstance(record, databento_dbn.ErrorMsg):
            logger.error("received error message: %s", record.err)

        for stream in self._user_streams:
            task = self._loop.create_task(self._stream_task(stream, record))
            task.add_done_callback(self._task_complete)
            self._tasks.add(task)

        for callback in self._user_callbacks:
            task = self._loop.create_task(self._callback_task(callback, record))
            task.add_done_callback(self._task_complete)
            self._tasks.add(task)

    def abort(self) -> None:
        """
        Abort the processing of records.
        """
        for task in self._tasks:
            task.cancel()

    async def wait_for_processing(self) -> None:
        """
        Coroutine to wait for all tasks in the RecordPipeline to
        complete.
        """
        while self._tasks:
            logger.info(
                "waiting for %d record(s) to process",
                len(self._tasks),
            )
            await asyncio.gather(*self._tasks)
