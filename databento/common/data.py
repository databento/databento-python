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


DBZ_COMMON_HEADER: List[Tuple[str, Union[type, str]]] = [
    ("nwords", np.uint8),
    ("type", np.uint8),
    ("pub_id", np.uint16),
    ("product_id", np.uint32),
    ("ts_event", np.uint64),
]


DBZ_DERIV_TMUP: List[Tuple[str, Union[type, str]]] = [
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


DBZ_DERIV_OHLCV: List[Tuple[str, Union[type, str]]] = [
    ("open", np.int64),
    ("high", np.int64),
    ("low", np.int64),
    ("close", np.int64),
    ("volume", np.int64),
]


DBZ_STRUCT_MAP: Dict[Schema, List[Tuple[str, Union[type, str]]]] = {
    Schema.MBO: DBZ_COMMON_HEADER
    + [
        ("order_id", np.uint64),
        ("price", np.int64),
        ("size", np.uint32),
        ("flags", np.int8),
        ("chan_id", np.uint8),
        ("action", "S1"),  # 1 byte chararray
        ("side", "S1"),  # 1 byte chararray
        ("ts_recv", np.uint64),
        ("ts_in_delta", np.int32),
        ("sequence", np.uint32),
    ],
    Schema.MBP_1: DBZ_COMMON_HEADER + DBZ_DERIV_TMUP + get_deriv_ba_types(0),  # 1
    Schema.MBP_10: DBZ_COMMON_HEADER
    + DBZ_DERIV_TMUP
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
    Schema.TBBO: DBZ_COMMON_HEADER + DBZ_DERIV_TMUP + get_deriv_ba_types(0),
    Schema.TRADES: DBZ_COMMON_HEADER + DBZ_DERIV_TMUP,
    Schema.OHLCV_1S: DBZ_COMMON_HEADER + DBZ_DERIV_OHLCV,
    Schema.OHLCV_1M: DBZ_COMMON_HEADER + DBZ_DERIV_OHLCV,
    Schema.OHLCV_1H: DBZ_COMMON_HEADER + DBZ_DERIV_OHLCV,
    Schema.OHLCV_1D: DBZ_COMMON_HEADER + DBZ_DERIV_OHLCV,
    Schema.STATUS: DBZ_COMMON_HEADER
    + [
        ("ts_recv", np.uint64),
        ("group", "S1"),  # 1 byte chararray
        ("trading_status", np.uint8),
        ("halt_reason", np.uint8),
        ("trading_event", np.uint8),
    ],
    Schema.DEFINITION: DBZ_COMMON_HEADER
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


DBZ_DERIV_HEADER_FIELDS = [
    "ts_event",
    "ts_in_delta",
    "pub_id",
    "product_id",
    "action",
    "side",
    "flags",
    "price",
    "size",
    "sequence",
]

DBZ_COLUMNS = {
    Schema.MBO: [
        "ts_event",
        "ts_in_delta",
        "pub_id",
        "product_id",
        "order_id",
        "action",
        "side",
        "flags",
        "price",
        "size",
        "sequence",
    ],
    Schema.MBP_1: DBZ_DERIV_HEADER_FIELDS + get_deriv_ba_fields(0),
    Schema.MBP_10: DBZ_DERIV_HEADER_FIELDS
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
    Schema.TBBO: DBZ_DERIV_HEADER_FIELDS + get_deriv_ba_fields(0),
    Schema.TRADES: DBZ_DERIV_HEADER_FIELDS,
}


################################################################################
# CSV headers
################################################################################

CSV_DERIV_HEADER = b"ts_recv,ts_event,ts_in_delta,pub_id,product_id,action,side,flags,price,size,sequence"  # noqa
CSV_OHLCV_HEADER = b"ts_event,pub_id,product_id,open,high,low,close,volume"


CSV_HEADERS = {
    Schema.MBO: b"ts_recv,ts_event,ts_in_delta,pub_id,product_id,order_id,action,side,flags,price,size,sequence",  # noqa
    Schema.MBP_1: CSV_DERIV_HEADER + b"," + ",".join(get_deriv_ba_fields(0)).encode(),
    Schema.MBP_10: CSV_DERIV_HEADER
    + b","
    + ",".join(get_deriv_ba_fields(0)).encode()
    + b","
    + ",".join(get_deriv_ba_fields(1)).encode()
    + b","
    + ",".join(get_deriv_ba_fields(2)).encode()
    + b","
    + ",".join(get_deriv_ba_fields(3)).encode()
    + b","
    + ",".join(get_deriv_ba_fields(4)).encode()
    + b","
    + ",".join(get_deriv_ba_fields(5)).encode()
    + b","
    + ",".join(get_deriv_ba_fields(6)).encode()
    + b","
    + ",".join(get_deriv_ba_fields(7)).encode()
    + b","
    + ",".join(get_deriv_ba_fields(8)).encode()
    + b","
    + ",".join(get_deriv_ba_fields(9)).encode(),
    Schema.TBBO: CSV_DERIV_HEADER + b"," + ",".join(get_deriv_ba_fields(0)).encode(),
    Schema.TRADES: CSV_DERIV_HEADER,
    Schema.OHLCV_1S: CSV_OHLCV_HEADER,
    Schema.OHLCV_1M: CSV_OHLCV_HEADER,
    Schema.OHLCV_1H: CSV_OHLCV_HEADER,
    Schema.OHLCV_1D: CSV_OHLCV_HEADER,
    # TODO(cs) Complete headers
}
