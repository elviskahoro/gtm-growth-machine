"""Watch link data parsing for transcript messages."""

from __future__ import annotations

import re
from typing import NamedTuple


class TranscriptMessageWatchLinkData(NamedTuple):
    watch_link: str | None
    remaining_text: str | None

    @classmethod
    def parse_watch_link(
        cls: type[TranscriptMessageWatchLinkData],
        content: str,
    ) -> TranscriptMessageWatchLinkData:
        pattern = r"- WATCH:\s*(https?://[^\s]+)(?:\s+(.*))?"
        match = re.match(pattern, content)
        if match:
            return cls(
                watch_link=match.group(1),
                remaining_text=match.group(2).strip() if match.group(2) else None,
            )

        error_msg: str = f"Invalid watch link: {content}"
        raise ValueError(error_msg)


# trunk-ignore-begin(ruff/S101,ruff/PLC0415)
def test_parse_watch_link_basic_url() -> None:
    """Test parsing a basic watch link with HTTPS URL."""
    content: str = "- WATCH: https://example.com/watch"
    result: TranscriptMessageWatchLinkData = (
        TranscriptMessageWatchLinkData.parse_watch_link(content)
    )

    assert result.watch_link == "https://example.com/watch"
    assert result.remaining_text is None


def test_parse_watch_link_http_url() -> None:
    """Test parsing a watch link with HTTP URL."""
    content: str = "- WATCH: http://example.com/watch"
    result: TranscriptMessageWatchLinkData = (
        TranscriptMessageWatchLinkData.parse_watch_link(content)
    )

    assert result.watch_link == "http://example.com/watch"
    assert result.remaining_text is None


def test_parse_watch_link_with_remaining_text() -> None:
    """Test parsing a watch link with additional text after the URL."""
    content: str = "- WATCH: https://example.com/watch This is additional text"
    result: TranscriptMessageWatchLinkData = (
        TranscriptMessageWatchLinkData.parse_watch_link(content)
    )

    assert result.watch_link == "https://example.com/watch"
    assert result.remaining_text == "This is additional text"


def test_parse_watch_link_with_remaining_text_multiple_words() -> None:
    """Test parsing a watch link with multiple words of remaining text."""
    content: str = "- WATCH: https://example.com/video Some descriptive text here"
    result: TranscriptMessageWatchLinkData = (
        TranscriptMessageWatchLinkData.parse_watch_link(content)
    )

    assert result.watch_link == "https://example.com/video"
    assert result.remaining_text == "Some descriptive text here"


def test_parse_watch_link_extra_spaces_around_url() -> None:
    """Test parsing a watch link with extra spaces around the URL."""
    content: str = "- WATCH:   https://example.com/watch   "
    result: TranscriptMessageWatchLinkData = (
        TranscriptMessageWatchLinkData.parse_watch_link(content)
    )

    assert result.watch_link == "https://example.com/watch"
    assert result.remaining_text is None


def test_parse_watch_link_extra_spaces_with_remaining_text() -> None:
    """Test parsing a watch link with extra spaces and remaining text."""
    content: str = "- WATCH:   https://example.com/watch   Some text here   "
    result: TranscriptMessageWatchLinkData = (
        TranscriptMessageWatchLinkData.parse_watch_link(content)
    )

    assert result.watch_link == "https://example.com/watch"
    assert result.remaining_text == "Some text here"


def test_parse_watch_link_complex_url() -> None:
    """Test parsing a watch link with a complex URL containing query parameters."""
    content: str = "- WATCH: https://example.com/watch?v=abc123&t=5m30s"
    result: TranscriptMessageWatchLinkData = (
        TranscriptMessageWatchLinkData.parse_watch_link(content)
    )

    assert result.watch_link == "https://example.com/watch?v=abc123&t=5m30s"
    assert result.remaining_text is None


def test_parse_watch_link_url_with_path() -> None:
    """Test parsing a watch link with URL containing paths."""
    content: str = "- WATCH: https://example.com/path/to/video/123"
    result: TranscriptMessageWatchLinkData = (
        TranscriptMessageWatchLinkData.parse_watch_link(content)
    )

    assert result.watch_link == "https://example.com/path/to/video/123"
    assert result.remaining_text is None


def test_parse_watch_link_remaining_text_with_whitespace() -> None:
    """Test that remaining text is properly stripped of leading/trailing whitespace."""
    content: str = "- WATCH: https://example.com/watch    text with spaces   "
    result: TranscriptMessageWatchLinkData = (
        TranscriptMessageWatchLinkData.parse_watch_link(content)
    )

    assert result.watch_link == "https://example.com/watch"
    assert result.remaining_text == "text with spaces"


def test_parse_watch_link_invalid_missing_watch_prefix() -> None:
    """Test that invalid content without 'WATCH:' prefix raises ValueError."""
    import pytest

    content: str = "https://example.com/watch"

    with pytest.raises(ValueError, match=f"Invalid watch link: {re.escape(content)}"):
        TranscriptMessageWatchLinkData.parse_watch_link(content)


def test_parse_watch_link_invalid_missing_url() -> None:
    """Test that content with 'WATCH:' but no URL raises ValueError."""
    import pytest

    content: str = "- WATCH:"

    with pytest.raises(ValueError, match=f"Invalid watch link: {re.escape(content)}"):
        TranscriptMessageWatchLinkData.parse_watch_link(content)


def test_parse_watch_link_invalid_malformed_prefix() -> None:
    """Test that content with malformed prefix raises ValueError."""
    import pytest

    content: str = "WATCH: https://example.com/watch"

    with pytest.raises(ValueError, match=f"Invalid watch link: {re.escape(content)}"):
        TranscriptMessageWatchLinkData.parse_watch_link(content)


def test_parse_watch_link_invalid_non_http_url() -> None:
    """Test that content with non-HTTP(S) URL raises ValueError."""
    import pytest

    content: str = "- WATCH: ftp://example.com/watch"

    with pytest.raises(ValueError, match=f"Invalid watch link: {re.escape(content)}"):
        TranscriptMessageWatchLinkData.parse_watch_link(content)


def test_parse_watch_link_invalid_empty_string() -> None:
    """Test that empty string raises ValueError."""
    import pytest

    content: str = ""

    with pytest.raises(ValueError, match=f"Invalid watch link: {re.escape(content)}"):
        TranscriptMessageWatchLinkData.parse_watch_link(content)


def test_parse_watch_link_invalid_only_whitespace() -> None:
    """Test that string with only whitespace raises ValueError."""
    import pytest

    content: str = "   \n\t  "

    with pytest.raises(ValueError, match=re.escape("Invalid watch link: ") + ".*"):
        TranscriptMessageWatchLinkData.parse_watch_link(content)


def test_parse_watch_link_invalid_wrong_case() -> None:
    """Test that wrong case for 'WATCH' raises ValueError."""
    import pytest

    content: str = "- watch: https://example.com/watch"

    with pytest.raises(ValueError, match=f"Invalid watch link: {re.escape(content)}"):
        TranscriptMessageWatchLinkData.parse_watch_link(content)


def test_named_tuple_properties() -> None:
    """Test that TranscriptMessageWatchLinkData behaves as a NamedTuple."""
    data: TranscriptMessageWatchLinkData = TranscriptMessageWatchLinkData(
        watch_link="https://example.com/watch",
        remaining_text="some text",
    )

    # Test tuple unpacking
    watch_link: str | None = data.watch_link
    remaining_text: str | None = data.remaining_text
    assert watch_link == "https://example.com/watch"
    assert remaining_text == "some text"

    # Test indexed access
    assert data[0] == "https://example.com/watch"
    assert data[1] == "some text"

    # Test named access
    assert data.watch_link == "https://example.com/watch"
    assert data.remaining_text == "some text"


def test_named_tuple_immutability() -> None:
    """Test that TranscriptMessageWatchLinkData is immutable."""
    data: TranscriptMessageWatchLinkData = TranscriptMessageWatchLinkData(
        watch_link="https://example.com/watch",
        remaining_text="some text",
    )

    try:
        data.watch_link = "https://other.com/watch"
        msg: str = "Expected AttributeError for immutable NamedTuple"
        raise AssertionError(msg)

    except AttributeError:
        pass  # Expected behavior


def test_named_tuple_with_none_values() -> None:
    """Test creating TranscriptMessageWatchLinkData with None values."""
    data: TranscriptMessageWatchLinkData = TranscriptMessageWatchLinkData(
        watch_link=None,
        remaining_text=None,
    )

    assert data.watch_link is None
    assert data.remaining_text is None
    assert data[0] is None
    assert data[1] is None


# trunk-ignore-end(ruff/S101,ruff/PLC0415)
