"""
Unit tests for CRAM.
"""
import pytest
from databento.common import cram


@pytest.mark.parametrize(
    "challenge,key,expected",
    [
        pytest.param(
            "abcd1234",
            "db-unittestapikey1234567890FFFFF",
            "be87ce3d564b64481d4ad1902e2b41b26e3eef62b9de37d87eb4a1d4a5199b6f-FFFFF",
        ),
    ],
)
def test_get_challenge_response(
    challenge: str,
    key: str,
    expected: str,
) -> None:
    """
    A challenge response is of the form {hash}-{bucket_id}.

        - hash is a sha256 of the user's API key and CRAM challenge.
        - bucket_id is the last 5 characters of the user's API key.
    The hash calculated using the sha256 algorithm.
    The digest is the following string {key}|{challenge} where:
        - key is the user's API key
        - challenge is the CRAM challenge, this is salt for the hash.

    """
    response = cram.get_challenge_response(
        challenge=challenge,
        key=key,
    )
    assert response == expected
