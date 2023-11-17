from typing import Callable, Union

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
