from __future__ import annotations

import numpy as np
from databento_dbn import ImbalanceMsg
from databento_dbn import InstrumentDefMsg
from databento_dbn import MBOMsg
from databento_dbn import MBP1Msg
from databento_dbn import MBP10Msg
from databento_dbn import OHLCVMsg
from databento_dbn import Schema
from databento_dbn import StatMsg
from databento_dbn import TradeMsg

from databento.live import DBNRecord


DEFINITION_TYPE_MAX_MAP = {
    x[0]: np.iinfo(x[1]).max
    for x in InstrumentDefMsg._dtypes
    if not isinstance(x[1], str)
}

SCHEMA_STRUCT_MAP: dict[Schema, type[DBNRecord]] = {
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

DERIV_SCHEMAS = (
    Schema.MBP_1,
    Schema.MBP_10,
    Schema.TBBO,
    Schema.TRADES,
)

SCHEMA_DTYPES_MAP: dict[Schema, list[tuple[str, str]]] = {
    Schema.MBO: MBOMsg._dtypes,
    Schema.MBP_1: MBP1Msg._dtypes,
    Schema.MBP_10: MBP10Msg._dtypes,
    Schema.TBBO: MBP1Msg._dtypes,
    Schema.TRADES: TradeMsg._dtypes,
    Schema.OHLCV_1S: OHLCVMsg._dtypes,
    Schema.OHLCV_1M: OHLCVMsg._dtypes,
    Schema.OHLCV_1H: OHLCVMsg._dtypes,
    Schema.OHLCV_1D: OHLCVMsg._dtypes,
    Schema.DEFINITION: InstrumentDefMsg._dtypes,
    Schema.IMBALANCE: ImbalanceMsg._dtypes,
    Schema.STATISTICS: StatMsg._dtypes,
}

SCHEMA_COLUMNS = {
    Schema.MBO: MBOMsg._ordered_fields,
    Schema.MBP_1: MBP1Msg._ordered_fields,
    Schema.MBP_10: MBP10Msg._ordered_fields,
    Schema.TBBO: MBP1Msg._ordered_fields,
    Schema.TRADES: TradeMsg._ordered_fields,
    Schema.OHLCV_1S: OHLCVMsg._ordered_fields,
    Schema.OHLCV_1M: OHLCVMsg._ordered_fields,
    Schema.OHLCV_1H: OHLCVMsg._ordered_fields,
    Schema.OHLCV_1D: OHLCVMsg._ordered_fields,
    Schema.DEFINITION: InstrumentDefMsg._ordered_fields,
    Schema.IMBALANCE: ImbalanceMsg._ordered_fields,
    Schema.STATISTICS: StatMsg._ordered_fields,
}
