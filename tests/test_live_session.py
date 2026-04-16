import pytest
from databento_dbn import CBBOMsg
from databento_dbn import OHLCVMsg
from databento_dbn import Side

from databento.common.error import BentoError
from databento.live.session import DBN_QUEUE_LAG_THRESHOLD
from databento.live.session import DBN_QUEUE_MAX_LAG_NS
from databento.live.session import DBNQueue


def _record(ts_event: int = 0) -> OHLCVMsg:
    return OHLCVMsg(
        rtype=1,
        publisher_id=1,
        instrument_id=0,
        ts_event=ts_event,
        open=100,
        high=110,
        low=90,
        close=105,
        volume=1000,
    )


def _cbbo_record(ts_event: int, ts_recv: int) -> CBBOMsg:
    return CBBOMsg(
        rtype=0xC1,
        publisher_id=1,
        instrument_id=0,
        ts_event=ts_event,
        price=1_000_000_000,
        size=1,
        side=Side.NONE,
        ts_recv=ts_recv,
    )


def test_dbn_queue_put(
    timeout: float = 0.01,
) -> None:
    """
    Test that DBNQueue.put raises a BentoError if disabled.

    The `timeout` is required, otherwise we will block forever.

    """
    # Arrange
    queue = DBNQueue()
    record = _record()

    # Act, Assert
    with pytest.raises(BentoError):
        queue.put(record, timeout=timeout)

    queue.enable()
    queue.put(record, timeout=timeout)

    queue.disable()
    with pytest.raises(BentoError):
        queue.put(record, timeout=timeout)


def test_dbn_queue_put_nowait() -> None:
    """
    Test that DBNQueue.put_nowait raises a BentoError if disabled.
    """
    # Arrange
    queue = DBNQueue()
    record = _record()

    # Act, Assert
    with pytest.raises(BentoError):
        queue.put_nowait(record)

    queue.enable()
    queue.put_nowait(record)

    queue.disable()
    with pytest.raises(BentoError):
        queue.put_nowait(record)


def test_dbn_queue_is_full_uses_ts_index() -> None:
    queue = DBNQueue()
    queue.enable()

    base_recv = 1_700_000_000_000_000_000
    stale_event_start = 1_600_000_000_000_000_000
    ten_ms = 10_000_000
    for i in range(DBN_QUEUE_LAG_THRESHOLD + 2):
        queue.put_nowait(
            _cbbo_record(
                ts_event=stale_event_start + i * ten_ms,
                ts_recv=base_recv + i,
            ),
        )

    assert queue.qsize() > DBN_QUEUE_LAG_THRESHOLD
    assert not queue.is_full()


def test_dbn_queue_is_full_triggers_on_ts_recv_lag() -> None:
    """
    `ts_recv` lag past `DBN_QUEUE_MAX_LAG_NS` should trip backpressure even
    when `ts_event` is constant.
    """
    queue = DBNQueue()
    queue.enable()

    base_recv = 1_700_000_000_000_000_000
    step = (DBN_QUEUE_MAX_LAG_NS // DBN_QUEUE_LAG_THRESHOLD) + 1
    for i in range(DBN_QUEUE_LAG_THRESHOLD + 2):
        queue.put_nowait(
            _cbbo_record(
                ts_event=0,
                ts_recv=base_recv + i * step,
            ),
        )

    assert queue.is_full()
