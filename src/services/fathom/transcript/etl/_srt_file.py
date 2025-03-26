from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from pathlib import Path


class SrtFile(NamedTuple):
    content: list[str]
    path: Path
    url: str
    duration_minutes: float
    date: datetime
    title: str
    full_text: str

    @classmethod
    def from_file_content(
        cls,
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
            current_year: int = datetime.now(
                tz=timezone.utc,
            ).year
            month: int = (
                datetime.strptime(
                    date_str.split()[0],
                    "%B",
                )
                .replace(
                    tzinfo=timezone.utc,
                )
                .month
            )
            months_we_did_not_yet_have_fathom: int = 4
            year_to_use: int = 2024 if month >= months_we_did_not_yet_have_fathom else current_year
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
                    # Ensure the URL starts with "https:"
                    if url.startswith("//"):
                        url = f"https:{url}"
                    # Extract duration from the middle part
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
