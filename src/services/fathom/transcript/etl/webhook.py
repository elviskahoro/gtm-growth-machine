# trunk-ignore-all(ruff/TC001,ruff/TC003,ruff/A002)
from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING, NamedTuple

from pydantic import BaseModel

if TYPE_CHECKING:
    from collections.abc import Iterator

from src.services.dlt.filesystem_gcp import (
    gcp_clean_timestamp_from_datetime,
    gcp_clean_string,
)
from src.services.fathom.meeting.meeting import Meeting
from src.services.fathom.recording.recording import Recording
from src.services.fathom.transcript.transcript import Transcript
from src.services.fathom.user.user import FathomUser

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
    id: int
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
        id: int,
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
            id=id,
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
        id: int,
        url: str,
        title: str,
        date: datetime,
    ) -> Iterator[EtlTranscriptMessage]:
        i: int = 0
        current_transcript_message: EtlTranscriptMessage | None = None
        while i < len(lines):
            line: str = lines[i].strip()
            if not line:
                i += 1
                continue

            new_transcript_message: EtlTranscriptMessage | None = (
                EtlTranscriptMessage.parse_timestamp_line(
                    line=line,
                    id=id,
                    url=url,
                    title=title,
                    date=date,
                )
            )
            if new_transcript_message is not None:
                if current_transcript_message:
                    yield current_transcript_message

                current_transcript_message = new_transcript_message
                i += 1
                continue

            if current_transcript_message is None:
                error_msg: str = f"No transcript message found for line: {line}"
                raise ValueError(error_msg)

            current_transcript_message.process_content_line(
                line=line,
            )
            i += 1

        if current_transcript_message:
            yield current_transcript_message


class Webhook(BaseModel):
    id: int
    recording: Recording
    meeting: Meeting
    fathom_user: FathomUser
    transcript: Transcript

    def etl_is_valid_webhook(
        self: Webhook,
    ) -> bool:
        return True

    def etl_get_invalid_webhook_error_msg(
        self: Webhook,
    ) -> str:
        return "Invalid webhook"

    def etl_get_file_name(
        self: Webhook,
    ) -> str:
        timestamp: str = gcp_clean_timestamp_from_datetime(
            dt=self.meeting.scheduled_start_time,
        )
        recording_id: str
        if recording_id_raw := self.recording.get_recording_id_from_url():
            recording_id = f"{recording_id_raw:08d}"
        else:
            recording_id = f"{0:08d}"

        title: str = gcp_clean_string(self.meeting.title)
        return f"{timestamp}-{recording_id}-{title}.jsonl"

    def etl_get_json(
        self: Webhook,
    ) -> str:
        lines: list[str] = self.transcript.plaintext.split("\n")
        transcript_messages: Iterator[EtlTranscriptMessage] = (
            EtlTranscriptMessage.parse_transcript_lines(
                lines=lines,
                id=self.id,
                url=self.recording.url,
                title=self.meeting.title,
                date=self.meeting.scheduled_start_time,
            )
        )
        # Convert each message to a JSON string and join them with newlines
        return "\n".join(
            message.model_dump_json(
                indent=None,
            )
            for message in transcript_messages
        )
