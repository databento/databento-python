import pytest
from databento.common.error import BentoError
from databento.live.session import DBNQueue


def test_dbn_queue_put(
    timeout: float = 0.01,
) -> None:
    """
    Test that DBNQueue.put raises a BentoError if disabled.

    The `timeout` is required, otherwise we will block forever.

    """
    # Arrange
    queue = DBNQueue()

    # Act, Assert
    with pytest.raises(BentoError):
        queue.put(None, timeout=timeout)

    queue.enable()
    queue.put(None, timeout=timeout)

    queue.disable()
    with pytest.raises(BentoError):
        queue.put(None, timeout=timeout)


def test_dbn_queue_put_nowait() -> None:
    """
    Test that DBNQueue.put_nowait raises a BentoError if disabled.
    """
    # Arrange
    queue = DBNQueue()

    # Act, Assert
    with pytest.raises(BentoError):
        queue.put_nowait(None)

    queue.enable()
    queue.put_nowait(None)

    queue.disable()
    with pytest.raises(BentoError):
        queue.put_nowait(None)
