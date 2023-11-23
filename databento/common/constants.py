from __future__ import annotations

from typing import Final

import numpy as np
from databento_dbn import ImbalanceMsg
from databento_dbn import InstrumentDefMsg
from databento_dbn import InstrumentDefMsgV1
from databento_dbn import MBOMsg
from databento_dbn import MBP1Msg
from databento_dbn import MBP10Msg
from databento_dbn import OHLCVMsg
from databento_dbn import Schema
from databento_dbn import StatMsg
from databento_dbn import TradeMsg

from databento.common.types import DBNRecord


ALL_SYMBOLS: Final = "ALL_SYMBOLS"


DEFINITION_TYPE_MAX_MAP: Final = {
    x[0]: np.iinfo(x[1]).max
    for x in InstrumentDefMsg._dtypes
    if not isinstance(x[1], str)
}

INT64_NULL: Final = 9223372036854775807

SCHEMA_STRUCT_MAP: Final[dict[Schema, type[DBNRecord]]] = {
    Schema.DEFINITION: InstrumentDefMsg,
    Schema.IMBALANCE: ImbalanceMsg,
    Schema.MBO: MBOMsg,
    Schema.MBP_1: MBP1Msg,
    Schema.MBP_10: MBP10Msg,
    Schema.OHLCV_1S: OHLCVMsg,
    Schema.OHLCV_1M: OHLCVMsg,
    Schema.OHLCV_1H: OHLCVMsg,
    Schema.OHLCV_1D: OHLCVMsg,
    Schema.STATISTICS: StatMsg,
    Schema.TBBO: MBP1Msg,
    Schema.TRADES: TradeMsg,
}

SCHEMA_STRUCT_MAP_V1: Final[dict[Schema, type[DBNRecord]]] = {
    Schema.DEFINITION: InstrumentDefMsgV1,
    Schema.IMBALANCE: ImbalanceMsg,
    Schema.MBO: MBOMsg,
    Schema.MBP_1: MBP1Msg,
    Schema.MBP_10: MBP10Msg,
    Schema.OHLCV_1S: OHLCVMsg,
    Schema.OHLCV_1M: OHLCVMsg,
    Schema.OHLCV_1H: OHLCVMsg,
    Schema.OHLCV_1D: OHLCVMsg,
    Schema.STATISTICS: StatMsg,
    Schema.TBBO: MBP1Msg,
    Schema.TRADES: TradeMsg,
}
