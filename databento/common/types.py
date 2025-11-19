import datetime as dt
import logging
from collections.abc import Callable
from os import PathLike
import pathlib
from typing import Generic
from typing import IO
from typing import TypedDict
from typing import TypeVar
import warnings

import databento_dbn
import pandas as pd

from databento.common.error import BentoWarning

logger = logging.getLogger(__name__)

DBNRecord = (
    databento_dbn.BBOMsg
    | databento_dbn.CBBOMsg
    | databento_dbn.CMBP1Msg
    | databento_dbn.MBOMsg
    | databento_dbn.MBP1Msg
    | databento_dbn.MBP10Msg
    | databento_dbn.TradeMsg
    | databento_dbn.OHLCVMsg
    | databento_dbn.ImbalanceMsg
    | databento_dbn.InstrumentDefMsg
    | databento_dbn.InstrumentDefMsgV1
    | databento_dbn.InstrumentDefMsgV2
    | databento_dbn.StatMsg
    | databento_dbn.StatMsgV1
    | databento_dbn.StatusMsg
    | databento_dbn.SymbolMappingMsg
    | databento_dbn.SymbolMappingMsgV1
    | databento_dbn.SystemMsg
    | databento_dbn.SystemMsgV1
    | databento_dbn.ErrorMsg
    | databento_dbn.ErrorMsgV1
)

_T = TypeVar("_T")


class Default(Generic[_T]):
    """
    A container for a default value. This is to be used when a callable wants
    to detect if a default parameter value is being used.

    Example
    -------
        def foo(param=Default[int](10)):
            if isinstance(param, Default):
                print(f"param={param.value} (default)")
            else:
                print(f"param={param.value}")

    """

    def __init__(self, value: _T):
        self._value = value

    @property
    def value(self) -> _T:
        """
        The default value.

        Returns
        -------
        _T

        """
        return self._value


class MappingIntervalDict(TypedDict):
    """
    Represents a symbol mapping over a start and end date range interval.

    Parameters
    ----------
    start_date : dt.date
        The start of the mapping period.
    end_date : dt.date
        The end of the mapping period.
    symbol : str
        The symbol value.

    """

    start_date: dt.date
    end_date: dt.date
    symbol: str


RecordCallback = Callable[[DBNRecord], None]
ExceptionCallback = Callable[[Exception], None]
ReconnectCallback = Callable[[pd.Timestamp, pd.Timestamp], None]


class ClientStream:
    def __init__(
        self,
        stream: IO[bytes] | PathLike[str] | str,
        exc_fn: ExceptionCallback | None = None,
        max_warnings: int = 10,
    ) -> None:
        is_managed = False

        if isinstance(stream, (str, PathLike)):
            stream = pathlib.Path(stream).open("xb")
            is_managed = True

        if not hasattr(stream, "write"):
            raise ValueError(f"{type(stream).__name__} does not support write()")

        if not hasattr(stream, "writable") or not stream.writable():
            raise ValueError(f"{type(stream).__name__} is not a writable stream")

        if exc_fn is not None and not callable(exc_fn):
            raise ValueError(f"{exc_fn} is not callable")

        self._stream = stream
        self._exc_fn = exc_fn
        self._max_warnings = max(0, max_warnings)
        self._warning_count = 0
        self._is_managed = is_managed

    @property
    def stream_name(self) -> str:
        return getattr(self._stream, "__name__", str(self._stream))

    @property
    def is_closed(self) -> bool:
        """
        Return `True` if the underlying stream is closed.

        Returns
        -------
        bool

        """
        return self._stream.closed

    @property
    def is_managed(self) -> bool:
        """
        Return `True` if the underlying stream was opened by the
        `ClientStream`. This can be used to determine if the stream should be
        closed automatically.

        Returns
        -------
        bool

        """
        return self._is_managed

    @property
    def exc_callback_name(self) -> str:
        return getattr(self._exc_fn, "__name__", str(self._exc_fn))

    def close(self) -> None:
        """
        Close the underlying stream.
        """
        self._stream.close()

    def flush(self) -> None:
        """
        Flush the underlying stream.
        """
        self._stream.flush()

    def write(self, data: bytes) -> None:
        """
        Write data to the underlying stream. Any exceptions encountered will be
        dispatched to the exception callback, if defined.

        Parameters
        ----------
        data : bytes

        """
        try:
            self._stream.write(data)
        except Exception as exc:
            if self._exc_fn is None:
                self._warn(
                    f"stream '{self.stream_name}' encountered an exception without an exception handler: {repr(exc)}",
                )
            else:
                try:
                    self._exc_fn(exc)
                except Exception as inner_exc:
                    self._warn(
                        f"exception callback '{self.exc_callback_name}' encountered an exception: {repr(inner_exc)}",
                    )
                    raise inner_exc from exc
            raise exc

    def _warn(self, msg: str) -> None:
        logger.warning(msg)
        if self._warning_count < self._max_warnings:
            self._warning_count += 1
            warnings.warn(
                msg,
                BentoWarning,
                stacklevel=3,
            )
            if self._warning_count == self._max_warnings:
                warnings.warn(
                    f"suppressing further warnings for '{self.stream_name}'",
                    BentoWarning,
                    stacklevel=3,
                )


class ClientRecordCallback:
    def __init__(
        self,
        fn: RecordCallback,
        exc_fn: ExceptionCallback | None = None,
        max_warnings: int = 10,
    ) -> None:
        if not callable(fn):
            raise ValueError(f"{fn} is not callable")
        if exc_fn is not None and not callable(exc_fn):
            raise ValueError(f"{exc_fn} is not callable")

        self._fn = fn
        self._exc_fn = exc_fn
        self._max_warnings = max(0, max_warnings)
        self._warning_count = 0

    @property
    def callback_name(self) -> str:
        return getattr(self._fn, "__name__", str(self._fn))

    @property
    def exc_callback_name(self) -> str:
        return getattr(self._exc_fn, "__name__", str(self._exc_fn))

    def call(self, record: DBNRecord) -> None:
        """
        Execute the callback function, passing `record` in as the first
        argument. Any exceptions encountered will be dispatched to the
        exception callback, if defined.

        Parameters
        ----------
        record : DBNRecord

        """
        try:
            self._fn(record)
        except Exception as exc:
            if self._exc_fn is None:
                self._warn(
                    f"callback '{self.callback_name}' encountered an exception without an exception callback: {repr(exc)}",
                )
            else:
                try:
                    self._exc_fn(exc)
                except Exception as inner_exc:
                    self._warn(
                        f"exception callback '{self.exc_callback_name}' encountered an exception: {repr(inner_exc)}",
                    )
                    raise inner_exc from exc
            raise exc

    def _warn(self, msg: str) -> None:
        logger.warning(msg)
        if self._warning_count < self._max_warnings:
            self._warning_count += 1
            warnings.warn(
                msg,
                BentoWarning,
                stacklevel=3,
            )
            if self._warning_count == self._max_warnings:
                warnings.warn(
                    f"suppressing further warnings for '{self.callback_name}'",
                    BentoWarning,
                    stacklevel=3,
                )
