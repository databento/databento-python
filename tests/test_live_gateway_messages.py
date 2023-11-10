from __future__ import annotations

import pytest
from databento.common.publishers import Dataset
from databento.live.gateway import AuthenticationRequest
from databento.live.gateway import AuthenticationResponse
from databento.live.gateway import ChallengeRequest
from databento.live.gateway import GatewayControl
from databento.live.gateway import Greeting
from databento.live.gateway import SessionStart
from databento.live.gateway import SubscriptionRequest
from databento_dbn import Encoding
from databento_dbn import Schema
from databento_dbn import SType


ALL_MESSAGES = (
    AuthenticationRequest,
    AuthenticationResponse,
    ChallengeRequest,
    GatewayControl,
    Greeting,
    SubscriptionRequest,
    SessionStart,
)


@pytest.mark.parametrize(
    "line, expected",
    [
        pytest.param(
            "auth=abcd1234|dataset=GLBX.MDP3|encoding=json\n",
            ("abcd1234", "GLBX.MDP3", "json", None, "0"),
        ),
        pytest.param(
            "auth=abcd1234|dataset=GLBX.MDP3|ts_out=1\n",
            (
                "abcd1234",
                "GLBX.MDP3",
                str(Encoding.DBN),
                None,
                "1",
            ),
        ),
        pytest.param(
            "auth=abcd1234|dataset=XNAS.ITCH\n",
            (
                "abcd1234",
                "XNAS.ITCH",
                str(Encoding.DBN),
                None,
                "0",
            ),
        ),
        pytest.param(
            "auth=abcd1234|dataset=GLBX.MDP3|ts_out=1|encoding=csv|extra=key\n",
            ValueError,
        ),
    ],
)
def test_parse_authentication_request(
    line: str,
    expected: tuple[str, ...] | type[Exception],
) -> None:
    """
    Test that a AuthenticationRequest is parsed from a string as expected.
    """
    # Arrange, Act, Assert
    if isinstance(expected, tuple):
        msg = AuthenticationRequest.parse(line)
        assert (
            msg.auth,
            msg.dataset,
            msg.encoding,
            msg.details,
            msg.ts_out,
        ) == expected
    else:
        with pytest.raises(expected):
            AuthenticationRequest.parse(line)


@pytest.mark.parametrize(
    "message,expected",
    [
        pytest.param(
            AuthenticationRequest(
                auth="abcd1234",
                dataset=Dataset.GLBX_MDP3,
                client="unittest",
            ),
            b"auth=abcd1234|dataset=GLBX.MDP3|encoding=dbn|ts_out=0|client=unittest\n",
        ),
        pytest.param(
            AuthenticationRequest(
                auth="abcd1234",
                dataset=Dataset.XNAS_ITCH,
                ts_out="1",
                client="unittest",
            ),
            b"auth=abcd1234|dataset=XNAS.ITCH|encoding=dbn|ts_out=1|client=unittest\n",
        ),
    ],
)
def test_serialize_authentication_request(
    message: AuthenticationRequest,
    expected: str | type[Exception],
) -> None:
    """
    Test that a AuthenticationRequest is serialized as expected.
    """
    # Arrange, Act, Assert
    assert bytes(message) == expected


@pytest.mark.parametrize(
    "line, expected",
    [
        pytest.param("success=1|session_id=1234\n", ("1", "1234")),
        pytest.param("success=0\n", ("0", None)),
        pytest.param("success=0|session_id=1234|extra=key\n", ValueError),
    ],
)
def test_parse_authentication_response(
    line: str,
    expected: tuple[str, ...] | type[Exception],
) -> None:
    """
    Test that a AuthenticationResponse is parsed from a string as expected.
    """
    # Arrange, Act, Assert
    if isinstance(expected, tuple):
        msg = AuthenticationResponse.parse(line)
        assert (msg.success, msg.session_id) == expected
    else:
        with pytest.raises(expected):
            AuthenticationResponse.parse(line)


@pytest.mark.parametrize(
    "message,expected",
    [
        pytest.param(
            AuthenticationResponse(
                success="1",
                session_id="1234",
            ),
            b"success=1|session_id=1234\n",
        ),
        pytest.param(
            AuthenticationResponse(
                success="0",
                error="this is a test",
            ),
            b"success=0|error=this is a test\n",
        ),
    ],
)
def test_serialize_authentication_response(
    message: AuthenticationResponse,
    expected: str | type[Exception],
) -> None:
    """
    Test that a AuthenticationResponse is serialized as expected.
    """
    # Arrange, Act, Assert
    assert bytes(message) == expected


@pytest.mark.parametrize(
    "line, expected",
    [
        pytest.param("cram=abcdefghijk\n", "abcdefghijk"),
        pytest.param("cram=abcdefghijk", ValueError, id="no_newline"),
        pytest.param("cram=abcdefghijk|extra=key\n", ValueError, id="extra_key"),
    ],
)
def test_parse_challenge_request(
    line: str,
    expected: str | type[Exception],
) -> None:
    """
    Test that a ChallengeRequest is parsed from a string as expected.
    """
    # Arrange, Act, Assert
    if isinstance(expected, str):
        msg = ChallengeRequest.parse(line)
        assert msg.cram == expected
    else:
        with pytest.raises(expected):
            ChallengeRequest.parse(line)


@pytest.mark.parametrize(
    "message,expected",
    [
        pytest.param(ChallengeRequest(cram="abcd1234"), b"cram=abcd1234\n"),
    ],
)
def test_serialize_challenge_request(
    message: ChallengeRequest,
    expected: str | type[Exception],
) -> None:
    """
    Test that a ChallengeRequest is serialized as expected.
    """
    # Arrange, Act, Assert
    assert bytes(message) == expected


@pytest.mark.parametrize(
    "line, expected",
    [
        pytest.param("lsg_version=1.2.3\n", "1.2.3"),
        pytest.param("lsg_version=1.2.3", ValueError, id="no_newline"),
        pytest.param("lsg_version=1.2.3|extra=key\n", ValueError, id="extra_key"),
    ],
)
def test_parse_greeting(
    line: str,
    expected: str | type[Exception],
) -> None:
    """
    Test that a Greeting is parsed from a string as expected.
    """
    # Arrange, Act, Assert
    if isinstance(expected, str):
        msg = Greeting.parse(line)
        assert msg.lsg_version == expected
    else:
        with pytest.raises(expected):
            Greeting.parse(line)


@pytest.mark.parametrize(
    "message,expected",
    [
        pytest.param(Greeting(lsg_version="1.2.3"), b"lsg_version=1.2.3\n"),
    ],
)
def test_serialize_greeting(
    message: Greeting,
    expected: str | type[Exception],
) -> None:
    """
    Test that a Greeting is serialized as expected.
    """
    # Arrange, Act, Assert
    assert bytes(message) == expected


@pytest.mark.parametrize(
    "line, expected",
    [
        pytest.param("start_session=0\n", "0"),
        pytest.param("start_session\n", "", id="no_value"),
        pytest.param("start_session=0", ValueError, id="no_newline"),
        pytest.param("start_session=0|extra=key\n", ValueError, id="extra_key"),
    ],
)
def test_parse_session_start(
    line: str,
    expected: str | type[Exception],
) -> None:
    """
    Test that a SessionStart is parsed from a string as expected.
    """
    # Arrange, Act, Assert
    if isinstance(expected, str):
        msg = SessionStart.parse(line)
        assert msg.start_session == expected
    else:
        with pytest.raises(expected):
            SessionStart.parse(line)


@pytest.mark.parametrize(
    "message,expected",
    [
        pytest.param(SessionStart(start_session="0"), b"start_session=0\n"),
    ],
)
def test_serialize_session_start(
    message: SessionStart,
    expected: str | type[Exception],
) -> None:
    """
    Test that a SessionStart is serialized as expected.
    """
    # Arrange, Act, Assert
    assert bytes(message) == expected


@pytest.mark.parametrize(
    "line, expected",
    [
        pytest.param(
            "schema=trades|" "stype_in=instrument_id|" "symbols=1,2,3\n",
            ("trades", "instrument_id", "1,2,3", None),
        ),
        pytest.param(
            "schema=trades|"
            "stype_in=instrument_id|"
            "symbols=1,2,3|"
            "start=1671717080706865759\n",
            ("trades", "instrument_id", "1,2,3", "1671717080706865759"),
        ),
        pytest.param(
            "schema=trades|" "stype_in=instrument_id|" "symbols=1,2,3",
            ValueError,
            id="no_newline",
        ),
        pytest.param(
            "schema=trades|" "stype_in=instrument_id|" "symbols=1,2,3|" "extra=key\n",
            ValueError,
            id="extra_key",
        ),
    ],
)
def test_parse_subscription_request(
    line: str,
    expected: tuple[str, ...] | type[Exception],
) -> None:
    """
    Test that a SubscriptionRequest is parsed from a string as expected.
    """
    # Arrange, Act, Assert
    if isinstance(expected, tuple):
        msg = SubscriptionRequest.parse(line)
        assert (
            msg.schema,
            msg.stype_in,
            msg.symbols,
            msg.start,
        ) == expected
    else:
        with pytest.raises(expected):
            SubscriptionRequest.parse(line)


@pytest.mark.parametrize(
    "message,expected",
    [
        pytest.param(
            SubscriptionRequest(
                schema=Schema.MBO,
                stype_in=SType.INSTRUMENT_ID,
                symbols="1234,5678,90",
            ),
            b"schema=mbo|" b"stype_in=instrument_id|" b"symbols=1234,5678,90\n",
        ),
        pytest.param(
            SubscriptionRequest(
                schema=Schema.MBO,
                stype_in=SType.RAW_SYMBOL,
                symbols="UNI,TTE,ST",
                start=1671717080706865759,
            ),
            b"schema=mbo|"
            b"stype_in=raw_symbol|"
            b"symbols=UNI,TTE,ST|"
            b"start=1671717080706865759\n",
        ),
    ],
)
def test_serialize_subscription_request(
    message: SubscriptionRequest,
    expected: str | type[Exception],
) -> None:
    """
    Test that a SubscriptionRequest is serialized as expected.
    """
    # Arrange, Act, Assert
    assert bytes(message) == expected


@pytest.mark.parametrize(
    "message_type",
    ALL_MESSAGES,
)
@pytest.mark.parametrize(
    "line",
    [
        pytest.param("", id="empty"),
        pytest.param("\n", id="newline"),
        pytest.param("foo=bar\n", id="unknown"),
    ],
)
def test_parse_bad_key(message_type: GatewayControl, line: str) -> None:
    """
    Test that a ValueError is raised when parsing fails for general cases.
    """
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        message_type.parse(line)
