# trunk-ignore-all(ruff/TC001)
from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from collections.abc import Iterator

from src.services.fathom.meeting.meeting import Meeting
from src.services.fathom.recording.recording import Recording
from src.services.fathom.transcript.transcript import Transcript
from src.services.fathom.user.user import FathomUser
from src.services.local.filesystem import (
    file_clean_string,
    file_clean_timestamp_from_datetime,
)

from .etl_model import EtlTranscriptMessage


class Webhook(BaseModel):
    id: int
    recording: Recording
    meeting: Meeting
    fathom_user: FathomUser
    transcript: Transcript

    @staticmethod
    def modal_get_secret_collection_name() -> str:
        return "devx-fathom"

    @staticmethod
    def etl_get_bucket_name() -> str:
        return "chalk-ai-devx-fathom-transcripts-etl"

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
        timestamp: str = file_clean_timestamp_from_datetime(
            dt=self.meeting.scheduled_start_time,
        )
        recording_id: str = self.recording.get_recording_id_from_url()
        title: str = file_clean_string(
            string=self.meeting.title,
        )
        return f"{timestamp}-{recording_id}-{title}.jsonl"

    def etl_get_json(
        self: Webhook,
    ) -> str:
        lines: list[str] = self.transcript.plaintext.split("\n")
        recording_id: str = self.recording.get_recording_id_from_url()
        transcript_messages: Iterator[EtlTranscriptMessage] = (
            EtlTranscriptMessage.parse_transcript_lines(
                recording_id=recording_id,
                lines=lines,
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
