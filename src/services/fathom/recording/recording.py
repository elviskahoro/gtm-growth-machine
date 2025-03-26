from __future__ import annotations

import re
from re import Pattern
from urllib.parse import ParseResult, urlparse

from pydantic import BaseModel


class Recording(BaseModel):
    url: str
    duration_in_minutes: float

    def get_recording_id_from_url(
        self: Recording,
    ) -> str:
        url: str = self.url
        parsed_url: ParseResult = urlparse(
            url=url,
        )
        call_pattern: Pattern[str] = re.compile(r"/calls/(\d+)")
        share_pattern: Pattern[str] = re.compile(r"/share/([A-Za-z0-9_-]+)")
        # trunk-ignore-begin(pyright/reportOptionalMemberAccess)
        match parsed_url.path:
            case str() as path_to_match if call_pattern.match(path_to_match):
                return call_pattern.match(path_to_match).group(1)

            case str() as path_to_match if share_pattern.match(path_to_match):
                return share_pattern.match(path_to_match).group(1)

            case _:
                error_msg: str = f"Could not parse fathom url: {url}"
                raise AssertionError(error_msg)

        # trunk-ignore-end(pyright/reportOptionalMemberAccess)
