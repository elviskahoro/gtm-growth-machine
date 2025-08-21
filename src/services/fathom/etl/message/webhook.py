from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

from pydantic import BaseModel

from src.services.fathom.etl.message.speaker import Speaker
from src.services.fathom.etl.message.storage import Storage
from src.services.fathom.etl.message.transcript_message import TranscriptMessage
from src.services.fathom.meeting.meeting import Meeting
from src.services.fathom.recording.recording import Recording
from src.services.fathom.transcript.transcript import Transcript
from src.services.fathom.user.user import FathomUser
from src.services.local.filesystem import FileUtility

if TYPE_CHECKING:
    from collections.abc import Iterator


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
        return "chalk-ai-devx-fathom-messages-etl-01"

    @staticmethod
    def storage_get_app_name() -> str:
        return f"{Webhook.etl_get_bucket_name()}-storage"

    @staticmethod
    def storage_get_base_model_type() -> type[Storage]:
        return Storage

    @staticmethod
    def lance_get_project_name() -> str:
        return TranscriptMessage.lance_get_project_name()

    @staticmethod
    def lance_get_table_name() -> str:
        return TranscriptMessage.lance_get_table_name()

    @staticmethod
    def lance_get_primary_key() -> str:
        return TranscriptMessage.lance_get_primary_key()

    @staticmethod
    def lance_get_primary_key_index_type() -> str:
        return TranscriptMessage.lance_get_primary_key_index_type()

    @staticmethod
    def lance_get_base_model_type() -> type[TranscriptMessage]:
        return TranscriptMessage

    @staticmethod
    def etl_expects_storage_file() -> bool:
        return True

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
        storage: Storage | None,
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
        storage: Storage,
    ) -> Iterator[TranscriptMessage]:
        speakers: list[Speaker] = storage.speakers_internal
        if not speakers:
            error: str = "Speakers not found in storage"
            raise ValueError(error)

        speaker_map: dict[str, str] = Speaker.build_speaker_lookup_map(
            speakers=speakers,
        )
        lines: list[str] = self.transcript.plaintext.split("\n")
        recording_id: str = self.recording.get_recording_id_from_url()
        yield from TranscriptMessage.parse_transcript_lines(
            recording_id=recording_id,
            lines=lines,
            url=self.recording.url,
            title=self.meeting.title,
            date=self.meeting.scheduled_start_time,
            speaker_map=speaker_map,
        )


# trunk-ignore-begin(ruff/PLR2004,ruff/S101,ruff/ANN401,ruff/PLC0415)
class TestWebhookUtilities:
    """Test utilities for creating mock Webhook components and instances."""

    @staticmethod
    def create_recording(
        url: str = "https://example.com/recording/123",
        duration: float = 30.0,
    ) -> Recording:
        """Create a Recording instance for testing."""
        return Recording(url=url, duration_in_minutes=duration)

    @staticmethod
    def create_meeting(
        start_time: datetime | None = None,
        title: str = "Test Meeting",
    ) -> Meeting:
        """Create a Meeting instance for testing."""
        if start_time is None:
            start_time = datetime(2023, 12, 15, 14, 30, 0, tzinfo=timezone.utc)
        return Meeting(
            scheduled_start_time=start_time,
            scheduled_end_time=None,
            scheduled_duration_in_minutes=None,
            join_url="https://zoom.us/j/123456789",
            title=title,
            has_external_invitees=None,
            external_domains=None,
            invitees=None,
        )

    @staticmethod
    def create_fathom_user(
        name: str = "Test User",
        email: str = "test@example.com",
        team: str = "Test Team",
    ) -> FathomUser:
        """Create a FathomUser instance for testing."""
        return FathomUser(name=name, email=email, team=team)

    @staticmethod
    def create_transcript(plaintext: str = "Test transcript content") -> Transcript:
        """Create a Transcript instance for testing."""
        return Transcript(plaintext=plaintext)

    @staticmethod
    def create_webhook(
        webhook_id: int = 1,
        **kwargs: Any,
    ) -> Webhook:
        """Create a Webhook instance for testing with default or custom components."""
        return Webhook(
            id=webhook_id,
            recording=kwargs.get("recording", TestWebhookUtilities.create_recording()),
            meeting=kwargs.get("meeting", TestWebhookUtilities.create_meeting()),
            fathom_user=kwargs.get(
                "fathom_user",
                TestWebhookUtilities.create_fathom_user(),
            ),
            transcript=kwargs.get(
                "transcript",
                TestWebhookUtilities.create_transcript(),
            ),
        )


def test_modal_get_secret_collection_names() -> None:
    result: list[str] = Webhook.modal_get_secret_collection_names()
    assert result == ["devx-growth-gcp"]
    assert isinstance(result, list)
    assert len(result) == 1


def test_etl_get_bucket_name() -> None:
    result: str = Webhook.etl_get_bucket_name()
    assert result == "chalk-ai-devx-fathom-messages-etl-01"
    assert isinstance(result, str)


def test_storage_get_app_name() -> None:
    result: str = Webhook.storage_get_app_name()
    expected: str = "chalk-ai-devx-fathom-messages-etl-01-storage"
    assert result == expected
    assert isinstance(result, str)


def test_storage_get_base_model_type() -> None:
    result: type[Storage] = Webhook.storage_get_base_model_type()
    assert result is Storage


def test_lance_get_methods_delegate_to_transcript_message() -> None:
    with patch.object(
        TranscriptMessage,
        "lance_get_project_name",
        return_value="test_project",
    ):
        result: str = Webhook.lance_get_project_name()
        assert result == "test_project"

    with patch.object(
        TranscriptMessage,
        "lance_get_table_name",
        return_value="test_table",
    ):
        result: str = Webhook.lance_get_table_name()
        assert result == "test_table"

    with patch.object(
        TranscriptMessage,
        "lance_get_primary_key",
        return_value="test_key",
    ):
        result: str = Webhook.lance_get_primary_key()
        assert result == "test_key"

    with patch.object(
        TranscriptMessage,
        "lance_get_primary_key_index_type",
        return_value="test_index",
    ):
        result: str = Webhook.lance_get_primary_key_index_type()
        assert result == "test_index"


def test_lance_get_base_model_type() -> None:
    result: type[TranscriptMessage] = Webhook.lance_get_base_model_type()
    assert result is TranscriptMessage


def test_etl_is_valid_webhook() -> None:
    webhook: Webhook = TestWebhookUtilities.create_webhook()
    assert webhook.etl_is_valid_webhook() is True


def test_etl_get_invalid_webhook_error_msg() -> None:
    webhook: Webhook = TestWebhookUtilities.create_webhook()
    result: str = webhook.etl_get_invalid_webhook_error_msg()
    assert result == "Invalid webhook"
    assert isinstance(result, str)


def test_etl_get_json_with_none_storage() -> None:
    import pytest

    webhook: Webhook = TestWebhookUtilities.create_webhook()

    with pytest.raises(AttributeError) as exc_info:
        webhook.etl_get_json(storage=None)

    assert "Storage should not be None" in str(exc_info.value)


def test_etl_get_json_with_valid_storage() -> None:
    # Create mock transcript messages
    mock_message1: MagicMock = MagicMock()
    mock_message1.model_dump_json.return_value = '{"id": 1, "text": "Hello"}'
    mock_message2: MagicMock = MagicMock()
    mock_message2.model_dump_json.return_value = '{"id": 2, "text": "World"}'

    webhook: Webhook = TestWebhookUtilities.create_webhook()
    storage: MagicMock = MagicMock()

    # Patch at the class level since we can't patch instance methods on Pydantic models
    with patch.object(
        Webhook,
        "etl_get_base_models",
        return_value=[mock_message1, mock_message2],
    ):
        result: str = webhook.etl_get_json(storage=storage)

    expected: str = '{"id": 1, "text": "Hello"}\n{"id": 2, "text": "World"}'
    assert result == expected
    mock_message1.model_dump_json.assert_called_once_with(indent=None)
    mock_message2.model_dump_json.assert_called_once_with(indent=None)


def test_etl_get_file_name() -> None:
    mock_datetime: datetime = datetime(2023, 12, 15, 14, 30, 0, tzinfo=timezone.utc)
    meeting: Meeting = TestWebhookUtilities.create_meeting(
        start_time=mock_datetime,
        title="Test Meeting",
    )
    webhook: Webhook = TestWebhookUtilities.create_webhook(meeting=meeting)

    # Patch at the class level for the Recording method
    with patch.object(
        Recording,
        "get_recording_id_from_url",
        return_value="rec123",
    ), patch.object(
        FileUtility,
        "file_clean_timestamp_from_datetime",
        return_value="20231215-143000",
    ), patch.object(
        FileUtility,
        "file_clean_string",
        return_value="Test-Meeting",
    ):
        result: str = webhook.etl_get_file_name()

    expected: str = "20231215-143000-rec123-Test-Meeting.jsonl"
    assert result == expected


def test_etl_get_base_models_success() -> None:
    mock_speakers: list[MagicMock] = [MagicMock(), MagicMock()]
    mock_storage: MagicMock = MagicMock()
    mock_storage.speakers_internal = mock_speakers

    recording: Recording = TestWebhookUtilities.create_recording(
        url="https://example.com/recording",
    )
    meeting: Meeting = TestWebhookUtilities.create_meeting(
        start_time=datetime(2023, 12, 15, tzinfo=timezone.utc),
        title="Test Meeting",
    )
    transcript: Transcript = TestWebhookUtilities.create_transcript(
        plaintext="Line 1\nLine 2\nLine 3",
    )
    webhook: Webhook = TestWebhookUtilities.create_webhook(
        recording=recording,
        meeting=meeting,
        transcript=transcript,
    )

    mock_speaker_map: dict[str, str] = {"speaker1": "Speaker One"}
    mock_messages: list[MagicMock] = [MagicMock(), MagicMock()]

    # Patch at the class level and capture the mock objects
    with patch.object(
        Recording,
        "get_recording_id_from_url",
        return_value="rec123",
    ), patch.object(
        Speaker,
        "build_speaker_lookup_map",
        return_value=mock_speaker_map,
    ) as mock_build_speaker_lookup, patch.object(
        TranscriptMessage,
        "parse_transcript_lines",
        return_value=iter(mock_messages),
    ) as mock_parse_transcript:
        result: list[TranscriptMessage] = list(
            webhook.etl_get_base_models(storage=mock_storage),
        )

    assert result == mock_messages
    mock_build_speaker_lookup.assert_called_once_with(speakers=mock_speakers)
    mock_parse_transcript.assert_called_once_with(
        recording_id="rec123",
        lines=["Line 1", "Line 2", "Line 3"],
        url="https://example.com/recording",
        title="Test Meeting",
        date=datetime(2023, 12, 15, tzinfo=timezone.utc),
        speaker_map=mock_speaker_map,
    )


def test_etl_get_base_models_no_speakers() -> None:
    import pytest

    mock_storage: MagicMock = MagicMock()
    mock_storage.speakers_internal = None

    webhook: Webhook = TestWebhookUtilities.create_webhook()

    with pytest.raises(ValueError, match="Speakers not found in storage") as exc_info:
        list(webhook.etl_get_base_models(storage=mock_storage))

    assert "Speakers not found in storage" in str(exc_info.value)


def test_webhook_model_creation() -> None:
    # Create specific test data
    recording: Recording = TestWebhookUtilities.create_recording(
        url="https://example.com/recording/42",
        duration=45.0,
    )
    meeting: Meeting = TestWebhookUtilities.create_meeting(
        start_time=datetime(2023, 12, 15, 10, 0, 0, tzinfo=timezone.utc),
        title="Important Meeting",
    )
    fathom_user: FathomUser = TestWebhookUtilities.create_fathom_user(
        name="John Doe",
        email="john@example.com",
        team="Engineering",
    )
    transcript: Transcript = TestWebhookUtilities.create_transcript(
        plaintext="This is the meeting transcript",
    )

    webhook: Webhook = TestWebhookUtilities.create_webhook(
        webhook_id=42,
        recording=recording,
        meeting=meeting,
        fathom_user=fathom_user,
        transcript=transcript,
    )

    assert webhook.id == 42
    assert webhook.recording == recording
    assert webhook.meeting == meeting
    assert webhook.fathom_user == fathom_user
    assert webhook.transcript == transcript


def test_etl_get_json_empty_iterator() -> None:
    webhook: Webhook = TestWebhookUtilities.create_webhook()
    storage: MagicMock = MagicMock()

    # Patch at the class level
    with patch.object(Webhook, "etl_get_base_models", return_value=[]):
        result: str = webhook.etl_get_json(storage=storage)

    assert result == ""


# trunk-ignore-end(ruff/PLR2004,ruff/S101,ruff/ANN401,ruff/PLC0415)
