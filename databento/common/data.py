from typing import Dict, List, Tuple, Union

import numpy as np
from databento.common.enums import Schema


################################################################################
# DBZ struct schema
################################################################################


def get_deriv_ba_types(level: int) -> List[Tuple[str, Union[type, str]]]:
    return [
        (f"bid_px_{level:02d}", np.int64),
        (f"ask_px_{level:02d}", np.int64),
        (f"bid_sz_{level:02d}", np.uint32),
        (f"ask_sz_{level:02d}", np.uint32),
        (f"bid_oq_{level:02d}", np.uint32),
        (f"ask_oq_{level:02d}", np.uint32),
    ]


DERIV_SCHEMAS = (
    Schema.MBP_1,
    Schema.MBP_10,
    Schema.TBBO,
    Schema.TRADES,
)


OHLCV_SCHEMAS = (
    Schema.OHLCV_1S,
    Schema.OHLCV_1M,
    Schema.OHLCV_1H,
    Schema.OHLCV_1D,
)


RECORD_HEADER: List[Tuple[str, Union[type, str]]] = [
    ("length", np.uint8),
    ("rtype", np.uint8),
    ("publisher_id", np.uint16),
    ("product_id", np.uint32),
    ("ts_event", np.uint64),
]


MBP_MSG: List[Tuple[str, Union[type, str]]] = [
    ("price", np.int64),
    ("size", np.uint32),
    ("action", "S1"),  # 1 byte chararray
    ("side", "S1"),  # 1 byte chararray
    ("flags", np.int8),
    ("depth", np.uint8),
    ("ts_recv", np.uint64),
    ("ts_in_delta", np.int32),
    ("sequence", np.uint32),
]


OHLCV_MSG: List[Tuple[str, Union[type, str]]] = [
    ("open", np.int64),
    ("high", np.int64),
    ("low", np.int64),
    ("close", np.int64),
    ("volume", np.int64),
]


STRUCT_MAP: Dict[Schema, List[Tuple[str, Union[type, str]]]] = {
    Schema.MBO: RECORD_HEADER
    + [
        ("order_id", np.uint64),
        ("price", np.int64),
        ("size", np.uint32),
        ("flags", np.int8),
        ("channel_id", np.uint8),
        ("action", "S1"),  # 1 byte chararray
        ("side", "S1"),  # 1 byte chararray
        ("ts_recv", np.uint64),
        ("ts_in_delta", np.int32),
        ("sequence", np.uint32),
    ],
    Schema.MBP_1: RECORD_HEADER + MBP_MSG + get_deriv_ba_types(0),  # 1
    Schema.MBP_10: RECORD_HEADER
    + MBP_MSG
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
    Schema.TBBO: RECORD_HEADER + MBP_MSG + get_deriv_ba_types(0),
    Schema.TRADES: RECORD_HEADER + MBP_MSG,
    Schema.OHLCV_1S: RECORD_HEADER + OHLCV_MSG,
    Schema.OHLCV_1M: RECORD_HEADER + OHLCV_MSG,
    Schema.OHLCV_1H: RECORD_HEADER + OHLCV_MSG,
    Schema.OHLCV_1D: RECORD_HEADER + OHLCV_MSG,
    Schema.STATUS: RECORD_HEADER
    + [
        ("ts_recv", np.uint64),
        ("group", "S1"),  # 1 byte chararray
        ("trading_status", np.uint8),
        ("halt_reason", np.uint8),
        ("trading_event", np.uint8),
    ],
    Schema.DEFINITION: RECORD_HEADER
    + [
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
        ("cleared_volume", np.int32),
        ("market_depth_implied", np.int32),
        ("market_depth", np.int32),
        ("market_segment_id", np.uint32),
        ("max_trade_vol", np.uint32),
        ("min_lot_size", np.int32),
        ("min_lot_size_block", np.int32),
        ("min_lot_size_round_lot", np.int32),
        ("min_trade_vol", np.uint32),
        ("open_interest_qty", np.int32),
        ("contract_multiplier", np.int32),
        ("decay_quantity", np.int32),
        ("original_contract_size", np.int32),
        ("related_security_id", np.uint32),
        ("trading_reference_date", np.uint16),
        ("appl_id", np.int16),
        ("maturity_month_year", np.uint16),
        ("decay_start_date", np.uint16),
        ("chan", np.uint16),
        ("currency", "S1"),  # 1 byte chararray
        ("settl_currency", "S1"),  # 1 byte chararray
        ("secsubtype", "S1"),  # 1 byte chararray
        ("symbol", "S1"),  # 1 byte chararray
        ("group", "S1"),  # 1 byte chararray
        ("exchange", "S1"),  # 1 byte chararray
        ("asset", "S1"),  # 1 byte chararray
        ("cfi", "S1"),  # 1 byte chararray
        ("security_type", "S1"),  # 1 byte chararray
        ("unit_of_measure", "S1"),  # 1 byte chararray
        ("underlying", "S1"),  # 1 byte chararray
        ("related", "S1"),  # 1 byte chararray
        ("match_algorithm", "S1"),  # 1 byte chararray
        ("md_security_trading_status", np.uint8),
        ("main_fraction", np.uint8),
        ("price_display_format", np.uint8),
        ("settl_price_type", np.uint8),
        ("sub_fraction", np.uint8),
        ("underlying_product", np.uint8),
        ("security_update_action", "S1"),  # 1 byte chararray
        ("maturity_month_month", np.uint8),
        ("maturity_month_day", np.uint8),
        ("maturity_month_week", np.uint8),
        ("user_defined_instrument", "S1"),  # 1 byte chararray
        ("contract_multiplier_unit", np.int8),
        ("flow_schedule_type", np.int8),
        ("tick_rule", np.uint8),
        ("dummy", "S1"),  # 1 byte chararray
    ],
}


################################################################################
# DBZ fields
################################################################################


def get_deriv_ba_fields(level: int) -> List[str]:
    return [
        f"bid_px_{level:02d}",
        f"ask_px_{level:02d}",
        f"bid_sz_{level:02d}",
        f"ask_sz_{level:02d}",
        f"bid_oq_{level:02d}",
        f"ask_oq_{level:02d}",
    ]


DERIV_HEADER_FIELDS = [
    "ts_event",
    "ts_in_delta",
    "publisher_id",
    "product_id",
    "action",
    "side",
    "depth",
    "flags",
    "price",
    "size",
    "sequence",
]

COLUMNS = {
    Schema.MBO: [
        "ts_event",
        "ts_in_delta",
        "publisher_id",
        "channel_id",
        "product_id",
        "order_id",
        "action",
        "side",
        "flags",
        "price",
        "size",
        "sequence",
    ],
    Schema.MBP_1: DERIV_HEADER_FIELDS + get_deriv_ba_fields(0),
    Schema.MBP_10: DERIV_HEADER_FIELDS
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
    Schema.TBBO: DERIV_HEADER_FIELDS + get_deriv_ba_fields(0),
    Schema.TRADES: DERIV_HEADER_FIELDS,
}
