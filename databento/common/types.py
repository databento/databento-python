import datetime as dt
from typing import Callable
from typing import Generic
from typing import TypedDict
from typing import TypeVar
from typing import Union

import databento_dbn
import pandas as pd


DBNRecord = Union[
    databento_dbn.BBOMsg,
    databento_dbn.CBBOMsg,
    databento_dbn.CMBP1Msg,
    databento_dbn.MBOMsg,
    databento_dbn.MBP1Msg,
    databento_dbn.MBP10Msg,
    databento_dbn.TradeMsg,
    databento_dbn.OHLCVMsg,
    databento_dbn.ImbalanceMsg,
    databento_dbn.InstrumentDefMsg,
    databento_dbn.InstrumentDefMsgV1,
    databento_dbn.InstrumentDefMsgV2,
    databento_dbn.StatMsg,
    databento_dbn.StatMsgV1,
    databento_dbn.StatusMsg,
    databento_dbn.SymbolMappingMsg,
    databento_dbn.SymbolMappingMsgV1,
    databento_dbn.SystemMsg,
    databento_dbn.SystemMsgV1,
    databento_dbn.ErrorMsg,
    databento_dbn.ErrorMsgV1,
]

RecordCallback = Callable[[DBNRecord], None]
ExceptionCallback = Callable[[Exception], None]
ReconnectCallback = Callable[[pd.Timestamp, pd.Timestamp], None]

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
