from __future__ import annotations

import re
from re import Pattern
from urllib.parse import ParseResult, urlparse

from pydantic import BaseModel


class Recording(BaseModel):
    url: str
    duration_in_minutes: float

    def get_recording_id_from_url(
        self,
    ) -> str:
        url: str = self.url
        parsed_url: ParseResult = urlparse(
            url=url,
        )
        call_pattern: Pattern[str] = re.compile(r"/calls/(\d+)")
        share_pattern: Pattern[str] = re.compile(r"/share/([A-Za-z0-9_-]+)")

        match parsed_url.path:
            case str() as path_to_match if call_match := call_pattern.match(
                path_to_match,
            ):
                return call_match.group(1)

            case str() as path_to_match if share_match := share_pattern.match(
                path_to_match,
            ):
                return share_match.group(1)

            case _:
                error_msg: str = f"Could not parse fathom url: {url}"
                raise AssertionError(error_msg)


# trunk-ignore-begin(ruff/PLR2004,ruff/S101,pyright/reportArgumentType,ruff/PLC0415)
def test_recording_model_creation() -> None:
    """Test that Recording model can be created with valid data."""
    recording: Recording = Recording(
        url="https://fathom.video/calls/123456",
        duration_in_minutes=45.5,
    )

    assert recording.url == "https://fathom.video/calls/123456"
    assert recording.duration_in_minutes == 45.5


def test_get_recording_id_from_call_url() -> None:
    """Test extracting recording ID from call URL."""
    recording: Recording = Recording(
        url="https://fathom.video/calls/123456",
        duration_in_minutes=30.0,
    )

    recording_id: str = recording.get_recording_id_from_url()
    assert recording_id == "123456"


def test_get_recording_id_from_share_url() -> None:
    """Test extracting recording ID from share URL."""
    recording: Recording = Recording(
        url="https://fathom.video/share/AbC123_-xyz",
        duration_in_minutes=25.0,
    )

    recording_id: str = recording.get_recording_id_from_url()
    assert recording_id == "AbC123_-xyz"


def test_get_recording_id_from_call_url_with_query_params() -> None:
    """Test extracting recording ID from call URL with query parameters."""
    recording: Recording = Recording(
        url="https://fathom.video/calls/789012?utm_source=email&tab=summary",
        duration_in_minutes=60.0,
    )

    recording_id: str = recording.get_recording_id_from_url()
    assert recording_id == "789012"


def test_get_recording_id_from_share_url_with_fragment() -> None:
    """Test extracting recording ID from share URL with fragment."""
    recording: Recording = Recording(
        url="https://fathom.video/share/XyZ789_test#section1",
        duration_in_minutes=40.0,
    )

    recording_id: str = recording.get_recording_id_from_url()
    assert recording_id == "XyZ789_test"


def test_get_recording_id_invalid_url_raises_assertion_error() -> None:
    """Test that invalid URL raises AssertionError."""
    import pytest

    recording: Recording = Recording(
        url="https://fathom.video/invalid/path",
        duration_in_minutes=30.0,
    )

    with pytest.raises(AssertionError) as exc_info:
        recording.get_recording_id_from_url()

    assert "Could not parse fathom url" in str(exc_info.value)
    assert "https://fathom.video/invalid/path" in str(exc_info.value)


def test_get_recording_id_empty_path_raises_assertion_error() -> None:
    """Test that URL with empty path raises AssertionError."""
    import pytest

    recording: Recording = Recording(
        url="https://fathom.video/",
        duration_in_minutes=15.0,
    )

    with pytest.raises(AssertionError) as exc_info:
        recording.get_recording_id_from_url()

    assert "Could not parse fathom url" in str(exc_info.value)


def test_get_recording_id_wrong_pattern_raises_assertion_error() -> None:
    """Test that URL with wrong pattern raises AssertionError."""
    import pytest

    recording: Recording = Recording(
        url="https://fathom.video/meetings/123",
        duration_in_minutes=20.0,
    )

    with pytest.raises(AssertionError) as exc_info:
        recording.get_recording_id_from_url()

    assert "Could not parse fathom url" in str(exc_info.value)


def test_get_recording_id_call_with_non_numeric_id() -> None:
    """Test that call URL with non-numeric ID doesn't match call pattern."""
    import pytest

    recording: Recording = Recording(
        url="https://fathom.video/calls/abc123",
        duration_in_minutes=35.0,
    )

    with pytest.raises(AssertionError) as exc_info:
        recording.get_recording_id_from_url()

    assert "Could not parse fathom url" in str(exc_info.value)


def test_recording_model_validation() -> None:
    """Test that Recording model validates input data."""
    import pytest
    from pydantic import ValidationError

    # Test invalid duration type
    with pytest.raises(ValidationError):
        Recording(
            url="https://fathom.video/calls/123",
            duration_in_minutes="not_a_number",
        )

    # Test missing required fields
    with pytest.raises(ValidationError):
        Recording(  # trunk-ignore(pyright/reportCallIssue)
            url="https://fathom.video/calls/123",
        )


def test_get_recording_id_edge_case_numeric_share() -> None:
    """Test share URL with numeric-only ID."""
    recording: Recording = Recording(
        url="https://fathom.video/share/123456",
        duration_in_minutes=30.0,
    )

    recording_id: str = recording.get_recording_id_from_url()
    assert recording_id == "123456"


def test_get_recording_id_edge_case_long_ids() -> None:
    """Test with very long IDs."""
    # Long numeric ID for calls
    recording_call: Recording = Recording(
        url="https://fathom.video/calls/12345678901234567890",
        duration_in_minutes=30.0,
    )
    assert recording_call.get_recording_id_from_url() == "12345678901234567890"

    # Long alphanumeric ID for shares
    recording_share: Recording = Recording(
        url="https://fathom.video/share/AbCdEf123456_-XyZ789",
        duration_in_minutes=30.0,
    )
    assert recording_share.get_recording_id_from_url() == "AbCdEf123456_-XyZ789"


# trunk-ignore-end(ruff/PLR2004,ruff/S101,pyright/reportArgumentType,ruff/PLC0415)
