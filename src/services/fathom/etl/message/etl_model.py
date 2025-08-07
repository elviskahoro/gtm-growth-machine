from __future__ import annotations

import re
from datetime import datetime  # trunk-ignore(ruff/TC003)
from re import Match
from typing import TYPE_CHECKING, NamedTuple

import pyarrow as pa
from pydantic import BaseModel, EmailStr, Field, ValidationError, validate_email

if TYPE_CHECKING:
    from collections.abc import Iterator


class Speaker(BaseModel):
    name: str = Field(
        ...,
        description="Name of the speaker",
    )
    email: EmailStr = Field(
        ...,
        description="Email address of the entity",
    )
    aliases: list[str] = Field(
        default_factory=list,
        description="List of alternative names or aliases",
    )

    @staticmethod
    def build_speaker_lookup_map(
        speakers: list[Speaker],
    ) -> dict[str, str]:
        lookup_map: dict[str, str] = {}
        for speaker in speakers:
            lookup_map[speaker.name.lower()] = speaker.email
            for alias in speaker.aliases:
                lookup_map[alias.lower()] = speaker.email

        return lookup_map

    @staticmethod
    def get_email_by_name_with_lookup(
        lookup_map: dict[str, str],
        search_name: str,
    ) -> str:
        return lookup_map.get(search_name.lower(), search_name)


class Storage(BaseModel):
    speakers_internal: list[Speaker] = Field(
        default_factory=list,
        description="List of speakers with their emails and aliases",
    )


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
    def lance_get_index_type() -> str:
        return "IVF_HNSW_SQ"

    @staticmethod
    def lance_get_index_cache_size() -> int:
        return 512

    @staticmethod
    def lance_get_index_metric() -> str:
        return "cosine"

    @staticmethod
    def lance_get_vector_column_name() -> str:
        return "embedding"

    @staticmethod
    def lance_get_vector_dimension() -> int:
        return 768

    @staticmethod
    def lance_get_primary_key() -> str:
        primary_key: str = "id"
        fields: set[str] = set(EtlTranscriptMessage.model_fields.keys())
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
                        EtlTranscriptMessage.lance_get_vector_dimension(),
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
        cls: type[EtlTranscriptMessage],
        line: str,
        recording_id: str,
        message_id: int,
        url: str,
        title: str,
        date: datetime,
        speaker_map: dict[str, str],
    ) -> EtlTranscriptMessage | None:
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
            timestamp=EtlTranscriptMessage.convert_timestamp_to_seconds(
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
        speaker_map: dict[str, str],
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
