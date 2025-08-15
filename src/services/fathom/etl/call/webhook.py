from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import orjson
from flatsplode import flatsplode
from pydantic import BaseModel

from src.services.fathom.meeting.meeting import Meeting
from src.services.fathom.recording.recording import Recording
from src.services.fathom.transcript.transcript import Transcript
from src.services.fathom.user.user import FathomUser
from src.services.local.filesystem import FileUtility

if TYPE_CHECKING:
    from unittest.mock import Mock

    from src.services.fathom.etl.message.transcript_message import TranscriptMessage


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
    def lance_get_base_model_type() -> type[TranscriptMessage]:
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
        model_data: dict = self.model_dump(
            mode="json",
        )
        recording_id: str = self.recording.get_recording_id_from_url()
        flattened_data: list[dict] = list(
            flatsplode(
                item=model_data,
                join="_",
            ),
        )
        for count, item in enumerate(
            flattened_data,
            start=1,
        ):
            if "recording_id" not in item:
                item["recording_id"] = recording_id

            item["id"] = f"{recording_id}-{count:05}"

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


# trunk-ignore-begin(ruff/PLR2004,ruff/S101,pyright/reportArgumentType,ruff/PLC0415)
class TestWebhookFixtures:
    """Test fixtures and utilities for Webhook testing."""

    @staticmethod
    def create_mock_recording() -> Mock:
        """Create a mock Recording object."""
        from unittest.mock import Mock

        recording: Mock = Mock(spec=Recording)
        recording.get_recording_id_from_url.return_value = "rec_123456"
        return recording

    @staticmethod
    def create_mock_meeting() -> Mock:
        """Create a mock Meeting object."""
        from unittest.mock import Mock

        meeting: Mock = Mock(spec=Meeting)
        meeting.title = "Test Meeting Title"
        meeting.scheduled_start_time = datetime(
            2024,
            3,
            15,
            10,
            30,
            0,
            tzinfo=timezone.utc,
        )
        return meeting

    @staticmethod
    def create_mock_fathom_user() -> Mock:
        """Create a mock FathomUser object."""
        from unittest.mock import Mock

        return Mock(spec=FathomUser)

    @staticmethod
    def create_mock_transcript() -> Mock:
        """Create a mock Transcript object."""
        from unittest.mock import Mock

        return Mock(spec=Transcript)

    @classmethod
    def create_sample_webhook(cls) -> Webhook:
        """Create a sample Webhook instance for testing."""
        return Webhook(
            id=12345,
            recording=cls.create_mock_recording(),
            meeting=cls.create_mock_meeting(),
            fathom_user=cls.create_mock_fathom_user(),
            transcript=cls.create_mock_transcript(),
        )


def test_webhook_static_methods() -> None:
    """Test all static methods return expected values for Webhook class."""
    # Test modal_get_secret_collection_names
    secret_names: list[str] = Webhook.modal_get_secret_collection_names()
    assert secret_names == ["devx-growth-gcp"]
    assert isinstance(secret_names, list)

    # Test etl_get_bucket_name
    bucket_name: str = Webhook.etl_get_bucket_name()
    assert bucket_name == "chalk-ai-devx-fathom-calls-etl-01"
    assert isinstance(bucket_name, str)

    # Test storage_get_app_name
    app_name: str = Webhook.storage_get_app_name()
    expected_app_name: str = f"{bucket_name}-storage"
    assert app_name == expected_app_name
    assert isinstance(app_name, str)

    # Test storage_get_base_model_type
    base_model_type: None = Webhook.storage_get_base_model_type()
    assert base_model_type is None


def test_webhook_not_implemented_methods() -> None:
    """Test methods that raise NotImplementedError."""
    import pytest

    # Webhook raises NotImplementedError
    with pytest.raises(NotImplementedError):
        Webhook.lance_get_project_name()

    with pytest.raises(NotImplementedError):
        Webhook.lance_get_base_model_type()


def test_webhook_model_creation() -> None:
    """Test Webhook model creation with all required fields."""
    mock_recording: Mock = TestWebhookFixtures.create_mock_recording()
    mock_meeting: Mock = TestWebhookFixtures.create_mock_meeting()
    mock_fathom_user: Mock = TestWebhookFixtures.create_mock_fathom_user()
    mock_transcript: Mock = TestWebhookFixtures.create_mock_transcript()

    webhook: Webhook = Webhook(
        id=99999,
        recording=mock_recording,
        meeting=mock_meeting,
        fathom_user=mock_fathom_user,
        transcript=mock_transcript,
    )

    assert webhook.id == 99999
    assert webhook.recording is mock_recording
    assert webhook.meeting is mock_meeting
    assert webhook.fathom_user is mock_fathom_user
    assert webhook.transcript is mock_transcript


def test_etl_is_valid_webhook() -> None:
    """Test etl_is_valid_webhook always returns True."""
    sample_webhook: Webhook = TestWebhookFixtures.create_sample_webhook()
    assert sample_webhook.etl_is_valid_webhook() is True


def test_etl_get_invalid_webhook_error_msg() -> None:
    """Test etl_get_invalid_webhook_error_msg returns expected message."""
    sample_webhook: Webhook = TestWebhookFixtures.create_sample_webhook()
    error_msg: str = sample_webhook.etl_get_invalid_webhook_error_msg()
    assert error_msg == "Invalid webhook"
    assert isinstance(error_msg, str)


def test_etl_get_base_models_behavior() -> None:
    """Test etl_get_base_models behavior."""
    import pytest

    sample_webhook: Webhook = TestWebhookFixtures.create_sample_webhook()

    # Webhook raises NotImplementedError
    with pytest.raises(NotImplementedError) as exc_info:
        sample_webhook.etl_get_base_models(storage=None)

    assert str(exc_info.value) == "Webhook does not support getting base models."


def test_etl_get_file_name() -> None:
    """Test etl_get_file_name generates expected filename format."""
    from unittest.mock import patch

    sample_webhook: Webhook = TestWebhookFixtures.create_sample_webhook()

    # Setup mocks
    with patch(
        "src.services.local.filesystem.FileUtility.file_clean_timestamp_from_datetime",
    ) as mock_clean_timestamp, patch(
        "src.services.local.filesystem.FileUtility.file_clean_string",
    ) as mock_clean_string:

        mock_clean_timestamp.return_value = "20240315_103000"
        mock_clean_string.return_value = "clean_test_meeting_title"

        # Call the method
        filename: str = sample_webhook.etl_get_file_name()

        # Verify the result
        expected_filename: str = (
            "20240315_103000-rec_123456-clean_test_meeting_title.jsonl"
        )
        assert filename == expected_filename

        # Verify utility methods were called correctly
        mock_clean_timestamp.assert_called_once_with(
            dt=sample_webhook.meeting.scheduled_start_time,
        )
        mock_clean_string.assert_called_once_with(
            string=sample_webhook.meeting.title,
        )
        sample_webhook.recording.get_recording_id_from_url.assert_called_once()


def test_etl_get_file_name_empty_title() -> None:
    """Test etl_get_file_name with empty meeting title."""
    from unittest.mock import patch

    sample_webhook: Webhook = TestWebhookFixtures.create_sample_webhook()

    # Setup mocks for empty title
    with patch(
        "src.services.local.filesystem.FileUtility.file_clean_timestamp_from_datetime",
    ) as mock_clean_timestamp, patch(
        "src.services.local.filesystem.FileUtility.file_clean_string",
    ) as mock_clean_string:

        mock_clean_timestamp.return_value = "20240315_103000"
        mock_clean_string.return_value = ""

        filename: str = sample_webhook.etl_get_file_name()
        expected_filename: str = "20240315_103000-rec_123456-.jsonl"
        assert filename == expected_filename


def test_etl_get_json_basic_structure() -> None:
    """Test etl_get_json returns valid JSONL structure."""
    from unittest.mock import patch

    sample_webhook: Webhook = TestWebhookFixtures.create_sample_webhook()

    # For Webhook, patch at the class level
    with patch.object(
        Webhook,
        "model_dump",
        return_value={
            "id": 12345,
            "nested": {
                "field1": "value1",
                "field2": "value2",
            },
        },
    ):
        # Call the method
        result: str = sample_webhook.etl_get_json(storage=None)

        # Verify it's a valid string
        assert isinstance(result, str)

        # Verify it contains JSONL format (newline-separated JSON objects)
        lines: list[str] = result.strip().split("\n")
        assert len(lines) > 0

        # Each line should be valid JSON
        for line in lines:
            parsed: dict[str, Any] = orjson.loads(line)
            assert isinstance(parsed, dict)
            # Each item should have recording_id and id fields
            assert "recording_id" in parsed
            assert "id" in parsed
            assert parsed["recording_id"] == "rec_123456"


def test_etl_get_json_with_flatsplode_data() -> None:
    """Test etl_get_json with nested data that gets flattened."""
    from unittest.mock import patch

    sample_webhook: Webhook = TestWebhookFixtures.create_sample_webhook()

    with patch.object(
        Webhook,
        "model_dump",
        return_value={
            "id": 12345,
            "user": {
                "name": "John Doe",
                "email": "john@example.com",
                "metadata": {
                    "role": "admin",
                    "department": "engineering",
                },
            },
            "meeting": {
                "title": "Test Meeting",
                "duration": 3600,
            },
        },
    ):
        result: str = sample_webhook.etl_get_json(storage=None)
        lines: list[str] = result.strip().split("\n")

        # Parse first line to check flattening
        first_item: dict[str, Any] = orjson.loads(lines[0])

        # Check that nested fields are flattened with underscore separator
        assert "user_name" in first_item or "user" in first_item
        assert "recording_id" in first_item
        assert "id" in first_item
        assert first_item["id"] == "rec_123456-00001"


def test_etl_get_json_id_generation() -> None:
    """Test that etl_get_json generates sequential IDs correctly."""
    from unittest.mock import patch

    sample_webhook: Webhook = TestWebhookFixtures.create_sample_webhook()

    with patch.object(
        Webhook,
        "model_dump",
        return_value={
            "id": 12345,
            "items": [
                {"name": "item1", "value": 100},
                {"name": "item2", "value": 200},
                {"name": "item3", "value": 300},
            ],
        },
    ):
        result: str = sample_webhook.etl_get_json(storage=None)
        lines: list[str] = result.strip().split("\n")

        # Check that each line has a sequential ID
        for i, line in enumerate(lines, start=1):
            item: dict[str, Any] = orjson.loads(line)
            expected_id: str = f"rec_123456-{i:05d}"
            assert item["id"] == expected_id


def test_etl_get_json_recording_id_already_present() -> None:
    """Test that etl_get_json doesn't override existing recording_id."""
    from unittest.mock import patch

    sample_webhook: Webhook = TestWebhookFixtures.create_sample_webhook()

    with patch.object(
        Webhook,
        "model_dump",
        return_value={
            "id": 12345,
            "recording_id": "existing_recording_id",
            "data": "test",
        },
    ):
        result: str = sample_webhook.etl_get_json(storage=None)
        lines: list[str] = result.strip().split("\n")

        # The existing recording_id should be preserved
        first_item: dict[str, Any] = orjson.loads(lines[0])
        assert first_item["recording_id"] == "existing_recording_id"


def test_etl_get_json_empty_data() -> None:
    """Test etl_get_json with minimal data."""
    from unittest.mock import patch

    sample_webhook: Webhook = TestWebhookFixtures.create_sample_webhook()

    with patch.object(Webhook, "model_dump", return_value={"id": 12345}):
        result: str = sample_webhook.etl_get_json(storage=None)
        assert isinstance(result, str)

        lines: list[str] = result.strip().split("\n")
        assert len(lines) == 1

        item: dict[str, Any] = orjson.loads(lines[0])
        assert item["id"] == "rec_123456-00001"
        assert item["recording_id"] == "rec_123456"


def test_etl_get_json_storage_parameter_ignored() -> None:
    """Test that the storage parameter is properly ignored."""
    from unittest.mock import patch

    sample_webhook: Webhook = TestWebhookFixtures.create_sample_webhook()

    with patch.object(Webhook, "model_dump", return_value={"id": 12345}):
        # Should work with None
        result1: str = sample_webhook.etl_get_json(storage=None)
        assert isinstance(result1, str)

        # Should work with any value (gets deleted)
        result2: str = sample_webhook.etl_get_json(storage="ignored_value")
        assert isinstance(result2, str)

        # Results should be identical
        assert result1 == result2


def test_webhook_pydantic_validation_missing_fields() -> None:
    """Test that Webhook requires all fields for creation."""
    import pytest
    from pydantic import ValidationError

    # Test missing required fields
    with pytest.raises(ValidationError) as exc_info:
        Webhook(
            id=123,
            recording=None,
            meeting=None,
            fathom_user=None,
            transcript=None,
        )

    error_str: str = str(exc_info.value)
    required_fields: list[str] = ["recording", "meeting", "fathom_user", "transcript"]

    # At least one required field should be mentioned in the error
    assert any(field in error_str for field in required_fields)


def test_webhook_with_edge_case_values() -> None:
    """Test that Webhook handles valid but edge case values."""
    mock_recording: Mock = TestWebhookFixtures.create_mock_recording()
    mock_meeting: Mock = TestWebhookFixtures.create_mock_meeting()
    mock_fathom_user: Mock = TestWebhookFixtures.create_mock_fathom_user()
    mock_transcript: Mock = TestWebhookFixtures.create_mock_transcript()

    # Test with zero ID
    webhook: Webhook = Webhook(
        id=0,
        recording=mock_recording,
        meeting=mock_meeting,
        fathom_user=mock_fathom_user,
        transcript=mock_transcript,
    )
    assert webhook.id == 0

    # Test with negative ID
    webhook_negative: Webhook = Webhook(
        id=-1,
        recording=mock_recording,
        meeting=mock_meeting,
        fathom_user=mock_fathom_user,
        transcript=mock_transcript,
    )
    assert webhook_negative.id == -1


def test_recording_method_calls() -> None:
    """Test that recording methods are called correctly."""
    from unittest.mock import patch

    sample_webhook: Webhook = TestWebhookFixtures.create_sample_webhook()

    # Test that get_recording_id_from_url is called in etl_get_file_name
    with patch(
        "src.services.local.filesystem.FileUtility.file_clean_timestamp_from_datetime",
        return_value="timestamp",
    ), patch(
        "src.services.local.filesystem.FileUtility.file_clean_string",
        return_value="title",
    ):

        sample_webhook.etl_get_file_name()
        sample_webhook.recording.get_recording_id_from_url.assert_called()


def test_meeting_attributes_access() -> None:
    """Test that meeting attributes are accessed correctly."""
    from unittest.mock import patch

    sample_webhook: Webhook = TestWebhookFixtures.create_sample_webhook()

    # Test that meeting attributes are used in etl_get_file_name
    with patch(
        "src.services.local.filesystem.FileUtility.file_clean_timestamp_from_datetime",
        return_value="timestamp",
    ) as mock_timestamp, patch(
        "src.services.local.filesystem.FileUtility.file_clean_string",
        return_value="title",
    ) as mock_string:

        sample_webhook.etl_get_file_name()

        # Verify the meeting attributes were accessed
        mock_timestamp.assert_called_once_with(
            dt=sample_webhook.meeting.scheduled_start_time,
        )
        mock_string.assert_called_once_with(string=sample_webhook.meeting.title)


def test_model_dump_called_in_etl_get_json() -> None:
    """Test that model_dump is called with correct parameters in etl_get_json."""
    from unittest.mock import patch

    sample_webhook: Webhook = TestWebhookFixtures.create_sample_webhook()

    with patch.object(Webhook, "model_dump") as mock_model_dump:
        mock_model_dump.return_value = {"id": 12345}

        sample_webhook.etl_get_json(storage=None)

        # Verify model_dump was called with json mode
        mock_model_dump.assert_called_once_with(mode="json")


# trunk-ignore-end(ruff/PLR2004,ruff/S101,pyright/reportArgumentType,ruff/PLC0415)
