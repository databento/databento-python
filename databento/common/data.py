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

################################################################################
# DBN struct schema
################################################################################


def get_deriv_ba_types(level: int) -> list[tuple[str, type | str]]:
    return [
        (f"bid_px_{level:02d}", np.int64),
        (f"ask_px_{level:02d}", np.int64),
        (f"bid_sz_{level:02d}", np.uint32),
        (f"ask_sz_{level:02d}", np.uint32),
        (f"bid_ct_{level:02d}", np.uint32),
        (f"ask_ct_{level:02d}", np.uint32),
    ]


DERIV_SCHEMAS = (
    Schema.MBP_1,
    Schema.MBP_10,
    Schema.TBBO,
    Schema.TRADES,
)

RECORD_HEADER: list[tuple[str, type | str]] = [
    ("length", np.uint8),
    ("rtype", np.uint8),
    ("publisher_id", np.uint16),
    ("instrument_id", np.uint32),
    ("ts_event", np.uint64),
]

MBO_MSG: list[tuple[str, type | str]] = RECORD_HEADER + [
    ("order_id", np.uint64),
    ("price", np.int64),
    ("size", np.uint32),
    ("flags", np.uint8),
    ("channel_id", np.uint8),
    ("action", "S1"),  # 1 byte chararray
    ("side", "S1"),  # 1 byte chararray
    ("ts_recv", np.uint64),
    ("ts_in_delta", np.int32),
    ("sequence", np.uint32),
]

MBP_MSG: list[tuple[str, type | str]] = RECORD_HEADER + [
    ("price", np.int64),
    ("size", np.uint32),
    ("action", "S1"),  # 1 byte chararray
    ("side", "S1"),  # 1 byte chararray
    ("flags", np.uint8),
    ("depth", np.uint8),
    ("ts_recv", np.uint64),
    ("ts_in_delta", np.int32),
    ("sequence", np.uint32),
]


OHLCV_MSG: list[tuple[str, type | str]] = [
    *RECORD_HEADER,
    ("open", np.int64),
    ("high", np.int64),
    ("low", np.int64),
    ("close", np.int64),
    ("volume", np.int64),
]

DEFINITION_MSG: list[tuple[str, type | str]] = RECORD_HEADER + [
    ("ts_recv", np.uint64),
    ("min_price_increment", np.int64),
    ("display_factor", np.int64),
    ("expiration", np.uint64),
    ("activation", np.uint64),
    ("high_limit_price", np.int64),
    ("low_limit_price", np.int64),
    ("max_price_variation", np.int64),
    ("trading_reference_price", np.int64),
    ("unit_of_measure_qty", np.int64),
    ("min_price_increment_amount", np.int64),
    ("price_ratio", np.int64),
    ("inst_attrib_value", np.int32),
    ("underlying_id", np.uint32),
    ("_reserved1", "S4"),
    ("market_depth_implied", np.int32),
    ("market_depth", np.int32),
    ("market_segment_id", np.uint32),
    ("max_trade_vol", np.uint32),
    ("min_lot_size", np.int32),
    ("min_lot_size_block", np.int32),
    ("min_lot_size_round_lot", np.int32),
    ("min_trade_vol", np.uint32),
    ("_reserved2", "S4"),
    ("contract_multiplier", np.int32),
    ("decay_quantity", np.int32),
    ("original_contract_size", np.int32),
    ("_reserved3", "S4"),
    ("trading_reference_date", np.uint16),
    ("appl_id", np.int16),
    ("maturity_year", np.uint16),
    ("decay_start_date", np.uint16),
    ("channel_id", np.uint16),
    ("currency", "S4"),  # 4 byte chararray
    ("settl_currency", "S4"),  # 4 byte chararray
    ("secsubtype", "S6"),  # 6 byte chararray
    ("raw_symbol", "S22"),  # 22 byte chararray
    ("group", "S21"),  # 21 byte chararray
    ("exchange", "S5"),  # 5 byte chararray
    ("asset", "S7"),  # 7 byte chararray
    ("cfi", "S7"),  # 7 byte chararray
    ("security_type", "S7"),  # 7 byte chararray
    ("unit_of_measure", "S31"),  # 31 byte chararray
    ("underlying", "S21"),  # 21 byte chararray
    ("strike_price_currency", "S4"),
    ("instrument_class", "S1"),
    ("_reserved4", "S2"),
    ("strike_price", np.int64),
    ("_reserved5", "S6"),
    ("match_algorithm", "S1"),  # 1 byte chararray
    ("md_security_trading_status", np.uint8),
    ("main_fraction", np.uint8),
    ("price_display_format", np.uint8),
    ("settl_price_type", np.uint8),
    ("sub_fraction", np.uint8),
    ("underlying_product", np.uint8),
    ("security_update_action", "S1"),  # 1 byte chararray
    ("maturity_month", np.uint8),
    ("maturity_day", np.uint8),
    ("maturity_week", np.uint8),
    ("user_defined_instrument", "S1"),  # 1 byte chararray
    ("contract_multiplier_unit", np.int8),
    ("flow_schedule_type", np.int8),
    ("tick_rule", np.uint8),
    ("dummy", "S3"),  # 3 byte chararray (Adjustment filler for 8-bytes alignment)
]

IMBALANCE_MSG: list[tuple[str, type | str]] = [
    *RECORD_HEADER,
    ("ts_recv", np.uint64),
    ("ref_price", np.int64),
    ("auction_time", np.uint64),
    ("cont_book_clr_price", np.int64),
    ("auct_interest_clr_price", np.int64),
    ("ssr_filling_price", np.int64),
    ("ind_match_price", np.int64),
    ("upper_collar", np.int64),
    ("lower_collar", np.int64),
    ("paired_qty", np.uint32),
    ("total_imbalance_qty", np.uint32),
    ("market_imbalance_qty", np.uint32),
    ("unpaired_qty", np.uint32),
    ("auction_type", "S1"),
    ("side", "S1"),
    ("auction_status", np.uint8),
    ("freeze_status", np.uint8),
    ("num_extensions", np.uint8),
    ("unpaired_side", "S1"),
    ("significant_imbalance", "S1"),
    ("dummy", "S1"),
]

STATISTICS_MSG: list[tuple[str, type | str]] = [
    *RECORD_HEADER,
    ("ts_recv", np.uint64),
    ("ts_ref", np.uint64),
    ("price", np.int64),
    ("quantity", np.int32),
    ("sequence", np.uint32),
    ("ts_in_delta", np.int32),
    ("stat_type", np.uint16),
    ("channel_id", np.uint16),
    ("update_action", np.uint8),
    ("stat_flags", np.uint8),
    ("dummy", "S6"),
]


STRUCT_MAP: dict[Schema, list[tuple[str, type | str]]] = {
    Schema.MBO: MBO_MSG,
    Schema.MBP_1: MBP_MSG + get_deriv_ba_types(0),  # 1
    Schema.MBP_10: MBP_MSG
    + get_deriv_ba_types(0)  # 1
    + get_deriv_ba_types(1)  # 2
    + get_deriv_ba_types(2)  # 3
    + get_deriv_ba_types(3)  # 4
    + get_deriv_ba_types(4)  # 5
    + get_deriv_ba_types(5)  # 6
    + get_deriv_ba_types(6)  # 7
    + get_deriv_ba_types(7)  # 8
    + get_deriv_ba_types(8)  # 9
    + get_deriv_ba_types(9),  # 10
    Schema.TBBO: MBP_MSG + get_deriv_ba_types(0),
    Schema.TRADES: MBP_MSG,
    Schema.OHLCV_1S: OHLCV_MSG,
    Schema.OHLCV_1M: OHLCV_MSG,
    Schema.OHLCV_1H: OHLCV_MSG,
    Schema.OHLCV_1D: OHLCV_MSG,
    Schema.DEFINITION: DEFINITION_MSG,
    Schema.IMBALANCE: IMBALANCE_MSG,
    Schema.STATISTICS: STATISTICS_MSG,
}


DEFINITION_CHARARRAY_COLUMNS = [
    "currency",
    "settl_currency",
    "secsubtype",
    "raw_symbol",
    "group",
    "exchange",
    "asset",
    "cfi",
    "security_type",
    "unit_of_measure",
    "underlying",
    "strike_price_currency",
    "instrument_class",
    "match_algorithm",
    "security_update_action",
    "user_defined_instrument",
]

DEFINITION_PRICE_COLUMNS = [
    "min_price_increment",
    "high_limit_price",
    "low_limit_price",
    "max_price_variation",
    "trading_reference_price",
    "min_price_increment_amount",
    "price_ratio",
    "strike_price",
]

DEFINITION_TYPE_MAX_MAP = {
    x[0]: np.iinfo(x[1]).max
    for x in STRUCT_MAP[Schema.DEFINITION]
    if not isinstance(x[1], str)
}

################################################################################
# DBN fields
################################################################################


def get_deriv_ba_fields(level: int) -> list[str]:
    return [
        f"bid_px_{level:02d}",
        f"ask_px_{level:02d}",
        f"bid_sz_{level:02d}",
        f"ask_sz_{level:02d}",
        f"bid_ct_{level:02d}",
        f"ask_ct_{level:02d}",
    ]


MBP_COLUMNS = [
    "ts_recv",
    "ts_event",
    "rtype",
    "publisher_id",
    "instrument_id",
    "action",
    "side",
    "depth",
    "price",
    "size",
    "flags",
    "ts_in_delta",
    "sequence",
]

OHLCV_COLUMNS = [
    "ts_event",
    "rtype",
    "publisher_id",
    "instrument_id",
    "open",
    "high",
    "low",
    "close",
    "volume",
]

DEFINITION_COLUMNS = [
    "ts_recv",
    "ts_event",
    "rtype",
    "publisher_id",
    "instrument_id",
    "raw_symbol",
    "security_update_action",
    "instrument_class",
    "min_price_increment",
    "display_factor",
    "expiration",
    "activation",
    "high_limit_price",
    "low_limit_price",
    "max_price_variation",
    "trading_reference_price",
    "unit_of_measure_qty",
    "min_price_increment_amount",
    "price_ratio",
    "inst_attrib_value",
    "underlying_id",
    "market_depth_implied",
    "market_depth",
    "market_segment_id",
    "max_trade_vol",
    "min_lot_size",
    "min_lot_size_block",
    "min_lot_size_round_lot",
    "min_trade_vol",
    "contract_multiplier",
    "decay_quantity",
    "original_contract_size",
    "trading_reference_date",
    "appl_id",
    "maturity_year",
    "decay_start_date",
    "channel_id",
    "currency",
    "settl_currency",
    "secsubtype",
    "group",
    "exchange",
    "asset",
    "cfi",
    "security_type",
    "unit_of_measure",
    "underlying",
    "strike_price_currency",
    "strike_price",
    "match_algorithm",
    "md_security_trading_status",
    "main_fraction",
    "price_display_format",
    "settl_price_type",
    "sub_fraction",
    "underlying_product",
    "maturity_month",
    "maturity_day",
    "maturity_week",
    "user_defined_instrument",
    "contract_multiplier_unit",
    "flow_schedule_type",
    "tick_rule",
]

STATISTICS_COLUMNS = [
    "ts_recv",
    "ts_event",
    "rtype",
    "publisher_id",
    "instrument_id",
    "ts_ref",
    "price",
    "quantity",
    "sequence",
    "ts_in_delta",
    "stat_type",
    "channel_id",
    "update_action",
    "stat_flags",
]

IMBALANCE_COLUMNS = [
    "ts_recv",
    "ts_event",
    "rtype",
    "publisher_id",
    "instrument_id",
    "ref_price",
    "auction_time",
    "cont_book_clr_price",
    "auct_interest_clr_price",
    "ssr_filling_price",
    "ind_match_price",
    "upper_collar",
    "lower_collar",
    "paired_qty",
    "total_imbalance_qty",
    "market_imbalance_qty",
    "unpaired_qty",
    "auction_type",
    "side",
    "auction_status",
    "freeze_status",
    "num_extensions",
    "unpaired_side",
    "significant_imbalance",
]

MBO_COLUMNS = [
    "ts_recv",
    "ts_event",
    "rtype",
    "publisher_id",
    "instrument_id",
    "action",
    "side",
    "price",
    "size",
    "channel_id",
    "order_id",
    "flags",
    "ts_in_delta",
    "sequence",
]

COLUMNS = {
    Schema.MBO: MBO_COLUMNS,
    Schema.MBP_1: MBP_COLUMNS + get_deriv_ba_fields(0),
    Schema.MBP_10: MBP_COLUMNS
    + get_deriv_ba_fields(0)
    + get_deriv_ba_fields(1)
    + get_deriv_ba_fields(2)
    + get_deriv_ba_fields(3)
    + get_deriv_ba_fields(4)
    + get_deriv_ba_fields(5)
    + get_deriv_ba_fields(6)
    + get_deriv_ba_fields(7)
    + get_deriv_ba_fields(8)
    + get_deriv_ba_fields(9),
    Schema.TBBO: MBP_COLUMNS + get_deriv_ba_fields(0),
    Schema.TRADES: MBP_COLUMNS,
    Schema.OHLCV_1S: OHLCV_COLUMNS,
    Schema.OHLCV_1M: OHLCV_COLUMNS,
    Schema.OHLCV_1H: OHLCV_COLUMNS,
    Schema.OHLCV_1D: OHLCV_COLUMNS,
    Schema.DEFINITION: DEFINITION_COLUMNS,
    Schema.IMBALANCE: IMBALANCE_COLUMNS,
    Schema.STATISTICS: STATISTICS_COLUMNS,
}
