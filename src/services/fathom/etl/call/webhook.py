# trunk-ignore-all(ruff/TC001)
from __future__ import annotations

import orjson
from flatsplode import flatsplode
from pydantic import BaseModel

from src.services.fathom.etl.message.etl_model import EtlTranscriptMessage
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
        return "chalk-ai-devx-fathom-calls-etl-01"

    @staticmethod
    def storage_get_app_name() -> str:
        return f"{Webhook.etl_get_bucket_name()}-storage"

    @staticmethod
    def storage_get_base_model_type() -> None:
        return None

    @staticmethod
    def lance_get_project_name() -> str:
        raise NotImplementedError

    @staticmethod
    def lance_get_base_model_type() -> type[EtlTranscriptMessage]:
        raise NotImplementedError

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
        storage: None,
    ) -> str:
        del storage
        model_data: dict = self.model_dump(mode="json")
        flattened_data: list[dict] = list(
            flatsplode(
                item=model_data,
                join="_",
            ),
        )
        jsonl_bytes: bytes = b"\n".join(orjson.dumps(item) for item in flattened_data)
        return jsonl_bytes.decode("utf-8")

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
        storage: None,
    ) -> None:
        del storage
        error: str = "Webhook does not support getting base models."
        raise NotImplementedError(error)
