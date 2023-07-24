import databento
import pytest
from databento.common.data import COLUMNS
from databento.common.data import SCHEMA_STRUCT_MAP


def test_mbo_fields() -> None:
    """
    Test that columns match the MBO struct.
    """
    struct = SCHEMA_STRUCT_MAP[databento.Schema.MBO]
    columns = COLUMNS[databento.Schema.MBO]

    fields = set(f for f in dir(struct) if not f.startswith("_"))
    fields.remove("hd")
    fields.remove("record_size")
    fields.remove("size_hint")

    difference = fields.symmetric_difference(set(columns))
    assert not difference


@pytest.mark.parametrize(
    "schema,level_count",
    [
        (databento.Schema.TBBO, 1),
        (databento.Schema.MBP_1, 1),
        (databento.Schema.MBP_10, 10),
    ],
)
def test_mbp_fields(
    schema: databento.Schema,
    level_count: int,
) -> None:
    """
    Test that columns match the MBP structs.
    """
    struct = SCHEMA_STRUCT_MAP[schema]
    columns = COLUMNS[schema]

    fields = set(f for f in dir(struct) if not f.startswith("_"))
    fields.remove("hd")
    fields.remove("record_size")
    fields.remove("size_hint")

    difference = fields.symmetric_difference(set(columns))

    assert "levels" in difference

    # bid/ask size, price, ct for each level, plus the levels field
    assert len(difference) == 6 * level_count + 1


@pytest.mark.parametrize(
    "schema",
    [
        databento.Schema.OHLCV_1S,
        databento.Schema.OHLCV_1M,
        databento.Schema.OHLCV_1H,
        databento.Schema.OHLCV_1D,
    ],
)
def test_ohlcv_fields(
    schema: databento.Schema,
) -> None:
    """
    Test that columns match the OHLCV structs.
    """
    struct = SCHEMA_STRUCT_MAP[schema]
    columns = COLUMNS[schema]

    fields = set(f for f in dir(struct) if not f.startswith("_"))
    fields.remove("hd")
    fields.remove("record_size")
    fields.remove("size_hint")

    difference = fields.symmetric_difference(set(columns))
    assert not difference


def test_trades_struct() -> None:
    """
    Test that columns match the Trades struct.
    """
    struct = SCHEMA_STRUCT_MAP[databento.Schema.TRADES]
    columns = COLUMNS[databento.Schema.TRADES]

    fields = set(f for f in dir(struct) if not f.startswith("_"))
    fields.remove("hd")
    fields.remove("record_size")
    fields.remove("size_hint")

    difference = fields.symmetric_difference(set(columns))
    assert not difference


def test_definition_struct() -> None:
    """
    Test that columns match the Definition struct.
    """
    struct = SCHEMA_STRUCT_MAP[databento.Schema.DEFINITION]
    columns = COLUMNS[databento.Schema.DEFINITION]

    fields = set(f for f in dir(struct) if not f.startswith("_"))
    fields.remove("hd")
    fields.remove("record_size")
    fields.remove("size_hint")

    difference = fields.symmetric_difference(set(columns))
    assert not difference


def test_imbalance_struct() -> None:
    """
    Test that columns match the Imbalance struct.
    """
    struct = SCHEMA_STRUCT_MAP[databento.Schema.IMBALANCE]
    columns = COLUMNS[databento.Schema.IMBALANCE]

    fields = set(f for f in dir(struct) if not f.startswith("_"))
    fields.remove("hd")
    fields.remove("record_size")
    fields.remove("size_hint")

    difference = fields.symmetric_difference(set(columns))
    assert not difference


def test_statistics_struct() -> None:
    """
    Test that columns match the Statistics struct.
    """
    struct = SCHEMA_STRUCT_MAP[databento.Schema.STATISTICS]
    columns = COLUMNS[databento.Schema.STATISTICS]

    fields = set(f for f in dir(struct) if not f.startswith("_"))
    fields.remove("hd")
    fields.remove("record_size")
    fields.remove("size_hint")

    difference = fields.symmetric_difference(set(columns))
    assert not difference
