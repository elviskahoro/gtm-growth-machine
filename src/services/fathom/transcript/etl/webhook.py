# trunk-ignore-all(ruff/TC001)
from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from collections.abc import Iterator

from src.services.fathom.meeting.meeting import Meeting
from src.services.fathom.recording.recording import Recording
from src.services.fathom.transcript.etl.etl_model import EtlTranscriptMessage
from src.services.fathom.transcript.transcript import Transcript
from src.services.fathom.user.user import FathomUser
from src.services.local.filesystem import FileCleaner


class Webhook(BaseModel):
    id: int
    recording: Recording
    meeting: Meeting
    fathom_user: FathomUser
    transcript: Transcript

    @staticmethod
    def modal_get_secret_collection_names() -> list[str]:
        return [
            "devx-fathom",
            "devx-octolens",
        ]

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
        timestamp: str = FileCleaner.file_clean_timestamp_from_datetime(
            dt=self.meeting.scheduled_start_time,
        )
        recording_id: str = self.recording.get_recording_id_from_url()
        title: str = FileCleaner.file_clean_string(
            string=self.meeting.title,
        )
        return f"{timestamp}-{recording_id}-{title}.jsonl"

    def etl_get_base_models(
        self: Webhook,
    ) -> Iterator[EtlTranscriptMessage]:
        lines: list[str] = self.transcript.plaintext.split("\n")
        recording_id: str = self.recording.get_recording_id_from_url()
        speakers_raw: str | None = os.environ.get("SPEAKERS_INTERNAL")
        speakers: list[str] | None = None
        if speakers_raw:
            try:
                speakers_data: dict[str, list[str]] | list[str] = json.loads(
                    speakers_raw,
                )
                if isinstance(speakers_data, dict) and "speakers" in speakers_data:
                    speakers = speakers_data["speakers"]

            except json.JSONDecodeError as e:
                print(f"Error parsing SPEAKERS_INTERNAL: {e}")

        yield from EtlTranscriptMessage.parse_transcript_lines(
            recording_id=recording_id,
            lines=lines,
            url=self.recording.url,
            title=self.meeting.title,
            date=self.meeting.scheduled_start_time,
            speakers_internal=speakers,
            organization_internal=os.environ.get("ORGANIZATION_INTERNAL"),
        )

    def etl_get_json(
        self: Webhook,
    ) -> str:
        return "\n".join(
            message.model_dump_json(
                indent=None,
            )
            for message in self.etl_get_base_models()
        )

    @staticmethod
    def lance_get_project_name() -> str:
        return EtlTranscriptMessage.lance_get_project_name()

    @staticmethod
    def lance_get_base_model_type() -> type[EtlTranscriptMessage]:
        return EtlTranscriptMessage
