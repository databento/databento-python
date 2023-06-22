from typing import Callable, Union

import databento_dbn


AUTH_TIMEOUT_SECONDS: float = 2
CONNECT_TIMEOUT_SECONDS: float = 5


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

RecordCallback = Callable[[DBNRecord], None]
ExceptionCallback = Callable[[Exception], None]
