# trunk-ignore-all(ruff/TC001)
from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from collections.abc import Iterator

    import modal

from src.services.fathom.etl.etl_model import EtlTranscriptMessage, Speaker, Storage
from src.services.fathom.meeting.meeting import Meeting
from src.services.fathom.recording.recording import Recording
from src.services.fathom.transcript.transcript import Transcript
from src.services.fathom.user.user import FathomUser
from src.services.local.filesystem import FileUtility


class Webhook(BaseModel):
    id: int
    recording: Recording
    meeting: Meeting
    fathom_user: FathomUser
    transcript: Transcript

    @staticmethod
    def modal_get_secret_collection_names() -> list[str]:
        return [
            "devx-growth-gcp",
        ]

    @staticmethod
    def etl_get_bucket_name() -> str:
        return "chalk-ai-devx-fathom-transcripts-etl"

    @staticmethod
    def storage_get_app_name() -> modal._Dict:
        return f"{Webhook.etl_get_bucket_name()}-storage"

    @staticmethod
    def storage_get_base_model_type() -> type[Storage]:
        return Storage

    @staticmethod
    def lance_get_project_name() -> str:
        return EtlTranscriptMessage.lance_get_project_name()

    @staticmethod
    def lance_get_base_model_type() -> type[EtlTranscriptMessage]:
        return EtlTranscriptMessage

    def etl_is_valid_webhook(
        self: Webhook,
    ) -> bool:
        return True

    def etl_get_invalid_webhook_error_msg(
        self: Webhook,
    ) -> str:
        return "Invalid webhook"

    def etl_get_json(
        self: Webhook,
        storage: BaseModel | None,
    ) -> str:
        if storage is None:
            error: str = (
                "Storage should not be None. Please provide the metadata for processing Fathom webhooks"
            )
            raise AttributeError(error)

        return "\n".join(
            message.model_dump_json(
                indent=None,
            )
            for message in self.etl_get_base_models(
                storage=storage,
            )
        )

    def etl_get_file_name(
        self: Webhook,
    ) -> str:
        timestamp: str = FileUtility.file_clean_timestamp_from_datetime(
            dt=self.meeting.scheduled_start_time,
        )
        recording_id: str = self.recording.get_recording_id_from_url()
        title: str = FileUtility.file_clean_string(
            string=self.meeting.title,
        )
        return f"{timestamp}-{recording_id}-{title}.jsonl"

    def etl_get_base_models(
        self: Webhook,
        storage: BaseModel,
    ) -> Iterator[EtlTranscriptMessage]:
        speakers: list[Speaker] = storage.speakers_internal
        if speakers is None:
            error: str = "Speakers not found in storage"
            raise ValueError(error)

        speaker_map: dict[str, str] = Speaker.build_speaker_lookup_map(
            speakers=speakers,
        )
        lines: list[str] = self.transcript.plaintext.split("\n")
        recording_id: str = self.recording.get_recording_id_from_url()
        yield from EtlTranscriptMessage.parse_transcript_lines(
            recording_id=recording_id,
            lines=lines,
            url=self.recording.url,
            title=self.meeting.title,
            date=self.meeting.scheduled_start_time,
            speaker_map=speaker_map,
        )
