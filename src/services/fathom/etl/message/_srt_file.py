from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

FATHOM_START_YEAR: int = 2024  # Year when Fathom became available
FATHOM_TRANSITION_YEAR: int = 2025
FATHOM_FUTURE_YEAR: int = 2026


class SrtFile(NamedTuple):
    content: list[str]
    path: Path
    url: str
    duration_minutes: float
    date: datetime
    title: str
    full_text: str

    @classmethod
    def from_file_content(  # trunk-ignore(ruff/PLR0912)
        cls: type[SrtFile],
        lines: list[str],
        path: Path,
        full_text: str,
    ) -> SrtFile:
        start_idx: int = 0
        url: str | None = None
        title: str | None = None
        date: datetime | None = None
        duration_minutes: float = 0.0

        # Extract title and date from the first line
        if lines and " - " in lines[0]:
            date_str: str
            title, date_str = lines[0].rsplit(" - ", 1)
            now = datetime.now(tz=timezone.utc)
            current_year: int = now.year

            # Parse the month and day from the date string
            date_without_year = datetime.strptime(
                date_str,
                "%B %d",
            ).replace(
                year=current_year,
                tzinfo=timezone.utc,
            )

            # Determine which year to use
            if current_year == FATHOM_TRANSITION_YEAR:
                # If we're in 2025 and the date is after today, it must be from 2024
                if date_without_year > now:
                    year_to_use = FATHOM_START_YEAR

                else:
                    year_to_use = FATHOM_TRANSITION_YEAR

            elif current_year >= FATHOM_FUTURE_YEAR:
                # From 2026 onwards, always use the current year
                year_to_use = current_year

            else:
                # For 2024 or earlier, use current year
                year_to_use = current_year

            date_str_with_year: str = f"{date_str} {year_to_use}"
            date = datetime.strptime(
                date_str_with_year,
                "%B %d %Y",
            ).replace(
                tzinfo=timezone.utc,
            )

        for line in lines[:5]:
            if "VIEW RECORDING" in line:
                # Extract both URL and duration
                # Example: "VIEW RECORDING - 19 mins (No highlights): https://fathom.video/calls/209771231"
                parts: list[str] = line.split(":")
                min_recording_metadata_section: int = (
                    2  # URL and duration info separated by colon
                )
                if len(parts) >= min_recording_metadata_section:
                    url = parts[-1].strip()
                    if url.startswith("//"):
                        url = f"https:{url}"

                    duration_text: str = parts[0].split("-")[1].strip()
                    try:
                        duration_minutes = float(duration_text.split()[0])

                    except (ValueError, IndexError):
                        duration_minutes = 0.0

                break

        for idx, line in enumerate(lines):
            if line.strip() == "---":
                start_idx = idx + 1
                break

        error_msg: str | None = None
        if url is None:
            error_msg = "URL is None"
            raise ValueError(error_msg)

        if date is None:
            error_msg = "Date is None"
            raise ValueError(error_msg)

        if title is None:
            error_msg = "Title is None"
            raise ValueError(error_msg)

        return cls(
            content=lines[start_idx:],
            path=path,
            url=url,
            duration_minutes=duration_minutes,
            date=date,
            title=title,
            full_text=full_text,
        )


# trunk-ignore-begin(ruff/PLR2004,ruff/S101)
def test_srt_file_from_file_content_valid() -> None:
    """Test successful parsing of valid SRT file content."""
    lines: list[str] = [
        "Team Meeting - January 15",
        "",
        "VIEW RECORDING - 25 mins (No highlights): https://fathom.video/calls/209771231",
        "",
        "---",
        "1",
        "00:00:01,000 --> 00:00:03,000",
        "Hello, welcome to the meeting.",
        "",
        "2",
        "00:00:04,000 --> 00:00:06,000",
        "Let's get started.",
    ]

    path: Path = Path("/test/file.srt")
    full_text: str = "\n".join(lines)
    result: SrtFile = SrtFile.from_file_content(lines, path, full_text)

    assert result.title == "Team Meeting"
    assert result.date == datetime(2025, 1, 15, tzinfo=timezone.utc)
    assert result.url == "https://fathom.video/calls/209771231"
    assert result.duration_minutes == 25.0
    assert result.path == path
    assert result.full_text == full_text
    assert result.content == [
        "1",
        "00:00:01,000 --> 00:00:03,000",
        "Hello, welcome to the meeting.",
        "",
        "2",
        "00:00:04,000 --> 00:00:06,000",
        "Let's get started.",
    ]


def test_srt_file_from_file_content_url_with_double_slash() -> None:
    """Test URL parsing when it starts with //."""
    lines: list[str] = [
        "Test Meeting - February 20",
        "VIEW RECORDING - 30 mins (No highlights): //fathom.video/calls/123456",
        "---",
        "content here",
    ]

    path: Path = Path("/test/file.srt")
    full_text: str = "\n".join(lines)

    result: SrtFile = SrtFile.from_file_content(lines, path, full_text)

    assert result.url == "https://fathom.video/calls/123456"
    assert result.duration_minutes == 30.0


def test_srt_file_from_file_content_duration_parsing_edge_cases() -> None:
    """Test duration parsing with various formats."""
    lines: list[str] = [
        "Meeting - March 10",
        "VIEW RECORDING - 45.5 mins (No highlights): https://fathom.video/calls/123",
        "---",
        "content",
    ]

    path: Path = Path("/test/file.srt")
    full_text: str = "\n".join(lines)

    result: SrtFile = SrtFile.from_file_content(lines, path, full_text)

    assert result.duration_minutes == 45.5


def test_srt_file_from_file_content_duration_parsing_failure() -> None:
    """Test duration parsing when format is unexpected."""
    lines: list[str] = [
        "Meeting - April 5",
        "VIEW RECORDING - invalid mins (No highlights): https://fathom.video/calls/123",
        "---",
        "content",
    ]

    path: Path = Path("/test/file.srt")
    full_text: str = "\n".join(lines)

    result: SrtFile = SrtFile.from_file_content(lines, path, full_text)

    assert result.duration_minutes == 0.0


def test_srt_file_from_file_content_year_logic_current_year() -> None:
    """Test year assignment logic for dates before today in 2025."""
    lines: list[str] = [
        "Meeting - April 1",  # April 1 is before August 15
        "VIEW RECORDING - 20 mins: https://fathom.video/calls/123",
        "---",
        "content",
    ]

    path: Path = Path("/test/file.srt")
    full_text: str = "\n".join(lines)

    result: SrtFile = SrtFile.from_file_content(lines, path, full_text)

    # April 1 is before August 15 in 2025, so it should use 2025
    assert result.date.year == 2025
    assert result.date.month == 4
    assert result.date.day == 1


def test_srt_file_from_file_content_year_logic_early_months() -> None:
    """Test year assignment logic for months < 4 (should use current year)."""
    lines: list[str] = [
        "Meeting - March 1",  # March is month 3, < 4
        "VIEW RECORDING - 20 mins: https://fathom.video/calls/123",
        "---",
        "content",
    ]

    path: Path = Path("/test/file.srt")
    full_text: str = "\n".join(lines)

    result: SrtFile = SrtFile.from_file_content(lines, path, full_text)

    current_year: int = datetime.now(tz=timezone.utc).year
    assert result.date.year == current_year
    assert result.date.month == 3
    assert result.date.day == 1


def test_srt_file_from_file_content_no_separator_line() -> None:
    """Test parsing when there's no '---' separator."""
    lines: list[str] = [
        "Meeting - May 15",
        "VIEW RECORDING - 15 mins: https://fathom.video/calls/123",
        "1",
        "00:00:01,000 --> 00:00:03,000",
        "Hello world",
    ]

    path: Path = Path("/test/file.srt")
    full_text: str = "\n".join(lines)

    result: SrtFile = SrtFile.from_file_content(lines, path, full_text)

    # Should include all lines since no separator found (start_idx remains 0)
    assert result.content == lines


def test_srt_file_from_file_content_missing_url() -> None:
    """Test error when URL is not found."""
    import pytest

    lines: list[str] = [
        "Meeting - June 10",
        "Some other content",
        "---",
        "content",
    ]

    path: Path = Path("/test/file.srt")
    full_text: str = "\n".join(lines)

    with pytest.raises(ValueError, match="URL is None"):
        SrtFile.from_file_content(lines, path, full_text)


def test_srt_file_from_file_content_missing_title_and_date() -> None:
    """Test error when title and date are not found."""
    import pytest

    lines: list[str] = [
        "Invalid first line",  # No " - " separator
        "VIEW RECORDING - 20 mins: https://fathom.video/calls/123",
        "---",
        "content",
    ]

    path: Path = Path("/test/file.srt")
    full_text: str = "\n".join(lines)

    with pytest.raises(ValueError, match="Date is None"):
        SrtFile.from_file_content(lines, path, full_text)


def test_srt_file_from_file_content_empty_lines() -> None:
    """Test parsing with empty lines list."""
    import pytest

    lines: list[str] = []
    path: Path = Path("/test/file.srt")
    full_text: str = ""

    with pytest.raises(ValueError, match="URL is None"):
        SrtFile.from_file_content(lines, path, full_text)


def test_srt_file_from_file_content_view_recording_in_different_position() -> None:
    """Test finding VIEW RECORDING line in different positions within first 5 lines."""
    lines: list[str] = [
        "Meeting - July 20",
        "Some intro text",
        "More intro",
        "Even more",
        "VIEW RECORDING - 35 mins: https://fathom.video/calls/999",
        "After recording line",
        "---",
        "content",
    ]

    path: Path = Path("/test/file.srt")
    full_text: str = "\n".join(lines)

    result: SrtFile = SrtFile.from_file_content(lines, path, full_text)

    assert result.url == "https://fathom.video/calls/999"
    assert result.duration_minutes == 35.0


def test_srt_file_from_file_content_view_recording_beyond_first_five_lines() -> None:
    """Test that VIEW RECORDING is not found if beyond first 5 lines."""
    import pytest

    lines: list[str] = [
        "Meeting - August 25",
        "Line 2",
        "Line 3",
        "Line 4",
        "Line 5",
        "Line 6",
        "VIEW RECORDING - 40 mins: https://fathom.video/calls/999",  # Line 7, should be ignored
        "---",
        "content",
    ]

    path: Path = Path("/test/file.srt")
    full_text: str = "\n".join(lines)

    with pytest.raises(ValueError, match="URL is None"):
        SrtFile.from_file_content(lines, path, full_text)


def test_srt_file_from_file_content_complex_url_parsing() -> None:
    """Test URL parsing with multiple colons in the line."""
    lines: list[str] = [
        "Complex Meeting - September 30",
        "VIEW RECORDING - 22 mins (Highlights: 3:45, 7:30): https://fathom.video/calls/complex123",
        "---",
        "content",
    ]

    path: Path = Path("/test/file.srt")
    full_text: str = "\n".join(lines)

    result: SrtFile = SrtFile.from_file_content(lines, path, full_text)

    assert result.url == "https://fathom.video/calls/complex123"
    assert result.duration_minutes == 22.0


def test_srt_file_from_file_content_insufficient_url_parts() -> None:
    """Test URL parsing when there aren't enough parts after splitting by colon."""
    import pytest

    lines: list[str] = [
        "Meeting - October 5",
        "VIEW RECORDING",  # No colon, insufficient parts
        "---",
        "content",
    ]

    path: Path = Path("/test/file.srt")
    full_text: str = "\n".join(lines)

    with pytest.raises(ValueError, match="URL is None"):
        SrtFile.from_file_content(lines, path, full_text)


def test_srt_file_namedtuple_immutability() -> None:
    """Test that SrtFile is immutable as a NamedTuple."""
    import pytest

    lines: list[str] = [
        "Test Meeting - November 12",
        "VIEW RECORDING - 10 mins: https://fathom.video/calls/123",
        "---",
        "test content",
    ]

    path: Path = Path("/test/file.srt")
    full_text: str = "\n".join(lines)

    result: SrtFile = SrtFile.from_file_content(lines, path, full_text)

    # Should not be able to modify fields
    with pytest.raises(AttributeError):
        result.title = "New Title"  # type: ignore[misc]


def test_srt_file_namedtuple_field_access() -> None:
    """Test accessing all fields of the NamedTuple."""
    lines: list[str] = [
        "Final Test - December 31",
        "VIEW RECORDING - 60 mins: https://fathom.video/calls/final",
        "---",
        "final content line",
    ]

    path: Path = Path("/test/final.srt")
    full_text: str = "\n".join(lines)

    result: SrtFile = SrtFile.from_file_content(lines, path, full_text)

    # Test all field access
    assert isinstance(result.content, list)
    assert isinstance(result.path, Path)
    assert isinstance(result.url, str)
    assert isinstance(result.duration_minutes, float)
    assert isinstance(result.date, datetime)
    assert isinstance(result.title, str)
    assert isinstance(result.full_text, str)

    # Test tuple indexing
    assert result[0] == result.content
    assert result[1] == result.path
    assert result[2] == result.url
    assert result[3] == result.duration_minutes
    assert result[4] == result.date
    assert result[5] == result.title
    assert result[6] == result.full_text


# trunk-ignore-end(ruff/PLR2004,ruff/S101)
