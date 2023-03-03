"""Unit tests for Bento compression."""
from io import BytesIO

import pytest
from databento.common.bento import is_dbn, is_zstandard


@pytest.mark.parametrize(
    "data,expected",
    [
        pytest.param(b"DBN", True),
        pytest.param(b"123", False, id="mismatch"),
        pytest.param(b"", False, id="empty"),
    ],
)
def test_is_dbn(data: bytes, expected: bool) -> None:
    """
    Test that buffers that start with DBN are identified
    as DBN files.
    """
    reader = BytesIO(data)
    assert is_dbn(reader) == expected


@pytest.mark.parametrize(
    "data,expected",
    [
        pytest.param(
            0x28_B5_2F_FD_04_58_6C_08_00_02_CE_33_38_30_8F_D3_18_88.to_bytes(18, "big"),
            True,
            id="standard_frame",
        ),
        pytest.param(
            0x50_2A_4D_18_94_00_00_00_44_42_5A_01_47_4C_42_58_2E_4D.to_bytes(18, "big"),
            True,
            id="skippable_frame",
        ),
        pytest.param(
            0x44_42_4E_01_C6_00_00_00_47_4C_42_58_2E_4D_44_50_33_00.to_bytes(
                18,
                "little",
            ),
            False,
            id="mismatch",
        ),
        pytest.param(b"", False, id="empty"),
    ],
)
def test_is_zstandard(data: bytes, expected: bool) -> None:
    """
    Test that buffers that contain ZSTD data are correctly
    identified.
    """
    reader = BytesIO(data)
    assert is_zstandard(reader) == expected
