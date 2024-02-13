from typing import Callable, Generic, TypeVar, Union

import databento_dbn


DBNRecord = Union[
    databento_dbn.MBOMsg,
    databento_dbn.MBP1Msg,
    databento_dbn.MBP10Msg,
    databento_dbn.TradeMsg,
    databento_dbn.OHLCVMsg,
    databento_dbn.ImbalanceMsg,
    databento_dbn.InstrumentDefMsg,
    databento_dbn.InstrumentDefMsgV1,
    databento_dbn.StatMsg,
    databento_dbn.SymbolMappingMsg,
    databento_dbn.SymbolMappingMsgV1,
    databento_dbn.SystemMsg,
    databento_dbn.ErrorMsg,
]

RecordCallback = Callable[[DBNRecord], None]
ExceptionCallback = Callable[[Exception], None]

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
