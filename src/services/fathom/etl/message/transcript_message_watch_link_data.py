"""Watch link data parsing for transcript messages."""

from __future__ import annotations

import re
from typing import NamedTuple


class TranscriptMessageWatchLinkData(NamedTuple):
    watch_link: str | None
    remaining_text: str | None

    @classmethod
    def parse_watch_link(
        cls,
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
