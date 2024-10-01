from typing import Final

import numpy as np
from databento_dbn import BBOMsg
from databento_dbn import CBBOMsg
from databento_dbn import CMBP1Msg
from databento_dbn import ImbalanceMsg
from databento_dbn import InstrumentDefMsg
from databento_dbn import InstrumentDefMsgV1
from databento_dbn import MBOMsg
from databento_dbn import MBP1Msg
from databento_dbn import MBP10Msg
from databento_dbn import OHLCVMsg
from databento_dbn import Schema
from databento_dbn import StatMsg
from databento_dbn import StatusMsg
from databento_dbn import TradeMsg

from databento.common.types import DBNRecord


ALL_SYMBOLS: Final = "ALL_SYMBOLS"


DEFINITION_TYPE_MAX_MAP: Final = {
    x[0]: np.iinfo(x[1]).max for x in InstrumentDefMsg._dtypes if not isinstance(x[1], str)
}

HTTP_STREAMING_READ_SIZE: Final = 2**12

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
    Schema.OHLCV_EOD: OHLCVMsg,
    Schema.STATISTICS: StatMsg,
    Schema.STATUS: StatusMsg,
    Schema.TBBO: MBP1Msg,
    Schema.TRADES: TradeMsg,
    Schema.CMBP_1: CMBP1Msg,
    Schema.CBBO_1S: CBBOMsg,
    Schema.CBBO_1M: CBBOMsg,
    Schema.TCBBO: CBBOMsg,
    Schema.BBO_1S: BBOMsg,
    Schema.BBO_1M: BBOMsg,
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
    Schema.STATUS: StatusMsg,
    Schema.TBBO: MBP1Msg,
    Schema.TRADES: TradeMsg,
    Schema.CMBP_1: CMBP1Msg,
    Schema.CBBO_1S: CBBOMsg,
    Schema.CBBO_1M: CBBOMsg,
    Schema.TCBBO: CBBOMsg,
    Schema.BBO_1S: BBOMsg,
    Schema.BBO_1M: BBOMsg,
}


CORPORATE_ACTIONS_DATETIME_COLUMNS: Final[list[str]] = [
    "ts_record",
    "ts_created",
]

CORPORATE_ACTIONS_DATE_COLUMNS: Final[list[str]] = [
    "event_date",
    "event_created_date",
    "effective_date",
    "ex_date",
    "record_date",
    "listing_date",
    "delisting_date",
    "payment_date",
    "duebills_redemption_date",
    "from_date",
    "to_date",
    "registration_date",
    "start_date",
    "end_date",
    "open_date",
    "close_date",
    "start_subscription_date",
    "end_subscription_date",
    "option_election_date",
    "withdrawal_right_from_date",
    "withdrawal_rights_to_date",
    "notification_date",
    "financial_year_end_date",
    "exp_completion_date",
]

ADJUSTMENT_FACTORS_DATETIME_COLUMNS: Final[list[str]] = [
    "ts_created",
]

ADJUSTMENT_FACTORS_DATE_COLUMNS: Final[list[str]] = [
    "ex_date",
]

SECURITY_MASTER_DATETIME_COLUMNS: Final[list[str]] = [
    "ts_record",
    "ts_effective",
    "ts_created",
]

SECURITY_MASTER_DATE_COLUMNS: Final[list[str]] = [
    "listing_created_date",
    "listing_date",
    "delisting_date",
    "shares_outstanding_date",
]
