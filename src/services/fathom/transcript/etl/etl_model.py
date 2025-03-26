from __future__ import annotations

import re
from datetime import datetime  # trunk-ignore(ruff/TC003)
from typing import TYPE_CHECKING, NamedTuple

from pydantic import BaseModel

if TYPE_CHECKING:
    from collections.abc import Iterator

IS_INTERNAL_ORGANIZATION: list[str] = [
    "SF, 2",
    "chalk.ai",
    "NY, 5",
]


class EtlTranscriptMessageWatchLinkData(NamedTuple):
    watch_link: str | None
    remaining_text: str | None

    @classmethod
    def parse_watch_link(
        cls,
        content: str,
    ) -> EtlTranscriptMessageWatchLinkData:
        pattern = r"- WATCH:\s*(https?://[^\s]+)(?:\s+(.*))?"
        match = re.match(pattern, content)
        if match:
            return cls(
                watch_link=match.group(1),
                remaining_text=match.group(2).strip() if match.group(2) else None,
            )

        error_msg: str = f"Invalid watch link: {content}"
        raise ValueError(error_msg)


class EtlTranscriptMessage(BaseModel):
    recording_id: str
    message_id: int
    url: str
    title: str
    date: datetime

    timestamp: str
    speaker: str
    organization: str | None
    message: str
    action_item: str | None
    watch_link: str | None

    @classmethod
    def parse_timestamp_line(
        cls: type[EtlTranscriptMessage],
        line: str,
        recording_id: str,
        message_id: int,
        url: str,
        title: str,
        date: datetime,
    ) -> EtlTranscriptMessage | None:
        timestamp_match = re.match(
            pattern=r"(\d{1,2}:\d{2}(?::\d{2})?)\s+-\s+(.+?)(?:\s*\((.*?)\))?$",
            string=line,
        )
        if not timestamp_match:
            return None

        return cls(
            timestamp=timestamp_match.group(1),
            speaker=timestamp_match.group(2).strip(),
            organization=(
                timestamp_match.group(3).strip() if timestamp_match.group(3) else None
            ),
            message="",
            action_item=None,
            watch_link=None,
            recording_id=recording_id,
            message_id=message_id,
            url=url,
            title=title,
            date=date,
        )

    def process_content_line(
        self,
        line: str,
    ) -> None:
        """Process a content line and update the TranscriptMessage instance."""
        match line:
            case str() as content if content.startswith("ACTION ITEM:"):
                self.action_item = content[len("ACTION ITEM:") :].strip()

            case str() as content if content.startswith("- WATCH:"):
                watch_data: EtlTranscriptMessageWatchLinkData = (
                    EtlTranscriptMessageWatchLinkData.parse_watch_link(
                        content=content,
                    )
                )
                self.watch_link = watch_data.watch_link
                if watch_data.remaining_text:
                    self.message = (
                        f"{self.message} {watch_data.remaining_text}"
                        if self.message
                        else watch_data.remaining_text
                    )

            case str() as content if content:
                self.message = f"{self.message} {content}" if self.message else content

            case str():
                # skipping empty lines
                pass

            case _:
                error_msg: str = f"Invalid line: {line}"
                raise ValueError(error_msg)

    @staticmethod
    def parse_transcript_lines(
        lines: list[str],
        recording_id: str,
        url: str,
        title: str,
        date: datetime,
    ) -> Iterator[EtlTranscriptMessage]:
        line_index: int = 0
        message_index: int = 1
        current_transcript_message: EtlTranscriptMessage | None = None
        while line_index < len(lines):
            line: str = lines[line_index].strip()
            if not line:
                line_index += 1
                continue

            new_transcript_message: EtlTranscriptMessage | None = (
                EtlTranscriptMessage.parse_timestamp_line(
                    line=line,
                    recording_id=recording_id,
                    message_id=message_index,
                    url=url,
                    title=title,
                    date=date,
                )
            )
            if new_transcript_message is not None:
                if current_transcript_message:
                    yield current_transcript_message

                current_transcript_message = new_transcript_message
                line_index += 1
                continue

            if current_transcript_message is None:
                error_msg: str = f"No transcript message found for line: {line}"
                raise ValueError(error_msg)

            current_transcript_message.process_content_line(
                line=line,
            )
            line_index += 1

        if current_transcript_message:
            yield current_transcript_message
