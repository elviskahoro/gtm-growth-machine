from __future__ import annotations

import re
from datetime import datetime  # trunk-ignore(ruff/TC003)
from re import Match
from typing import TYPE_CHECKING

import pyarrow as pa
from pydantic import BaseModel, ValidationError, validate_email

from .speaker import Speaker
from .watch_link_data import TranscriptMessageWatchLinkData

if TYPE_CHECKING:
    from collections.abc import Iterator


class TranscriptMessage(BaseModel):
    id: str
    recording_id: str
    message_id: int
    url: str
    title: str
    date: datetime

    timestamp: int
    speaker: str
    organization: str | None
    message: str
    action_item: str | None
    watch_link: str | None

    @staticmethod
    def gemini_get_column_to_embed() -> str:
        return "message"

    @staticmethod
    def lance_get_project_name() -> str:
        return "fathom-8ywo6z"

    @staticmethod
    def lance_get_table_name() -> str:
        return "fathom-messages"

    @staticmethod
    def lance_get_vector_index_type() -> str:
        return "IVF_HNSW_SQ"

    @staticmethod
    def lance_get_vector_index_cache_size() -> int:
        return 512

    @staticmethod
    def lance_get_vector_index_metric() -> str:
        return "cosine"

    @staticmethod
    def lance_get_vector_column_name() -> str:
        return "embedding"

    @staticmethod
    def lance_get_vector_dimension() -> int:
        return 768

    @staticmethod
    def lance_get_primary_key_index_type() -> str:
        return "BTREE"

    @staticmethod
    def lance_get_primary_key() -> str:
        primary_key: str = "id"
        fields: set[str] = set(TranscriptMessage.model_fields.keys())
        if primary_key in fields:
            return primary_key

        error_msg: str = (
            f"Primary key {primary_key} not found in model fields. Available fields: {fields}"
        )
        raise ValueError(
            error_msg,
        )

    @staticmethod
    def lance_get_schema() -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.string()),
                pa.field("recording_id", pa.string()),
                pa.field("message_id", pa.int32()),
                pa.field("url", pa.string()),
                pa.field("title", pa.string()),
                pa.field(
                    "date",
                    pa.timestamp("us"),
                ),  # microsecond timestamp for datetime
                pa.field("timestamp", pa.int32()),
                pa.field("speaker", pa.string()),
                pa.field(
                    "organization",
                    pa.string(),
                    nullable=True,
                ),
                pa.field("message", pa.string()),
                pa.field(
                    "action_item",
                    pa.string(),
                    nullable=True,
                ),
                pa.field(
                    "watch_link",
                    pa.string(),
                    nullable=True,
                ),
                pa.field(
                    "embedding",
                    pa.list_(
                        pa.float32(),
                        TranscriptMessage.lance_get_vector_dimension(),
                    ),
                    nullable=True,
                ),
            ],
        )

    @staticmethod
    def convert_timestamp_to_seconds(
        timestamp: str,
    ) -> int:
        time_parts: list[str] = timestamp.split(":")
        match len(time_parts):
            case 2:
                hours, minutes, seconds = "0", *time_parts
            case 3:
                hours, minutes, seconds = time_parts
            case _:
                error_msg: str = f"Invalid timestamp format: {timestamp}"
                raise ValueError(error_msg)

        return int(hours) * 3600 + int(minutes) * 60 + int(float(seconds))

    @classmethod
    def parse_timestamp_line(
        cls: type[TranscriptMessage],
        line: str,
        recording_id: str,
        message_id: int,
        url: str,
        title: str,
        date: datetime,
        speaker_map: dict[str, str],
    ) -> TranscriptMessage | None:
        transcript_entry_match: Match[str] | None = re.match(
            pattern=r"(\d{1,2}:\d{2}(?::\d{2})?)\s+-\s+(.+?)(?:\s+\(([^)]+)\))?$",
            string=line,
        )
        if not transcript_entry_match:
            return None

        timestamp: str = transcript_entry_match.group(1)
        speaker_raw: str = transcript_entry_match.group(2).strip()
        organization_raw: str | None = transcript_entry_match.group(3)
        speaker: str = Speaker.get_email_by_name_with_lookup(
            lookup_map=speaker_map,
            search_name=speaker_raw,
        )
        organization: str | None = None
        try:
            # Validate email using Pydantic v2 validate_email function
            validate_email(speaker)
            organization = speaker.split("@", 1)[1]

        except (ValidationError, ValueError, IndexError):
            if organization_raw:
                organization_raw.strip()

        return cls(
            id=f"{recording_id}-{message_id:05d}",
            timestamp=TranscriptMessage.convert_timestamp_to_seconds(
                timestamp=timestamp,
            ),
            speaker=speaker,
            organization=organization,
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
                watch_data: TranscriptMessageWatchLinkData = (
                    TranscriptMessageWatchLinkData.parse_watch_link(
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
        speaker_map: dict[str, str],
    ) -> Iterator[TranscriptMessage]:
        line_index: int = 0
        message_index: int = 1
        current_transcript_message: TranscriptMessage | None = None
        while line_index < len(lines):
            line: str = lines[line_index].strip()
            if not line:
                line_index += 1
                continue

            new_transcript_message: TranscriptMessage | None = (
                TranscriptMessage.parse_timestamp_line(
                    line=line,
                    recording_id=recording_id,
                    message_id=message_index,
                    url=url,
                    title=title,
                    date=date,
                    speaker_map=speaker_map,
                )
            )
            if new_transcript_message is not None:
                message_index += 1
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
