# trunk-ignore-all(ruff/TC001)
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock, patch

import orjson
import pytest
from flatsplode import flatsplode
from pydantic import BaseModel

from src.services.fathom.etl.message.transcript_message import TranscriptMessage
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
@pytest.fixture
def mock_recording() -> Mock:
    """Create a mock Recording object."""
    recording = Mock(spec=Recording)
    recording.get_recording_id_from_url.return_value = "rec_123456"
    return recording


@pytest.fixture
def mock_meeting() -> Mock:
    """Create a mock Meeting object."""
    meeting = Mock(spec=Meeting)
    meeting.title = "Test Meeting Title"
    meeting.scheduled_start_time = datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc)
    return meeting


@pytest.fixture
def mock_fathom_user() -> Mock:
    """Create a mock FathomUser object."""
    return Mock(spec=FathomUser)


@pytest.fixture
def mock_transcript() -> Mock:
    """Create a mock Transcript object."""
    return Mock(spec=Transcript)


@pytest.fixture
def sample_webhook(
    mock_recording: Mock,
    mock_meeting: Mock,
    mock_fathom_user: Mock,
    mock_transcript: Mock,
) -> Webhook:
    """Create a sample Webhook instance for testing."""
    return Webhook(
        id=12345,
        recording=mock_recording,
        meeting=mock_meeting,
        fathom_user=mock_fathom_user,
        transcript=mock_transcript,
    )


def test_webhook_static_methods() -> None:
    """Test all static methods return expected values."""
    # Test modal_get_secret_collection_names
    secret_names = Webhook.modal_get_secret_collection_names()
    assert secret_names == ["devx-growth-gcp"]
    assert isinstance(secret_names, list)

    # Test etl_get_bucket_name
    bucket_name = Webhook.etl_get_bucket_name()
    assert bucket_name == "chalk-ai-devx-fathom-calls-etl-01"
    assert isinstance(bucket_name, str)

    # Test storage_get_app_name
    app_name = Webhook.storage_get_app_name()
    assert app_name == "chalk-ai-devx-fathom-calls-etl-01-storage"
    assert isinstance(app_name, str)

    # Test storage_get_base_model_type
    base_model_type = Webhook.storage_get_base_model_type()
    assert base_model_type is None


def test_webhook_not_implemented_methods() -> None:
    """Test methods that raise NotImplementedError."""
    with pytest.raises(NotImplementedError):
        Webhook.lance_get_project_name()

    with pytest.raises(NotImplementedError):
        Webhook.lance_get_base_model_type()


def test_webhook_model_creation(
    mock_recording: Mock,
    mock_meeting: Mock,
    mock_fathom_user: Mock,
    mock_transcript: Mock,
) -> None:
    """Test Webhook model creation with all required fields."""
    webhook = Webhook(
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


def test_etl_is_valid_webhook(
    sample_webhook: Webhook,
) -> None:
    """Test etl_is_valid_webhook always returns True."""
    assert sample_webhook.etl_is_valid_webhook() is True


def test_etl_get_invalid_webhook_error_msg(
    sample_webhook: Webhook,
) -> None:
    """Test etl_get_invalid_webhook_error_msg returns expected message."""
    error_msg = sample_webhook.etl_get_invalid_webhook_error_msg()
    assert error_msg == "Invalid webhook"
    assert isinstance(error_msg, str)


def test_etl_get_base_models_raises_not_implemented(
    sample_webhook: Webhook,
) -> None:
    """Test etl_get_base_models raises NotImplementedError with expected message."""
    with pytest.raises(NotImplementedError) as exc_info:
        sample_webhook.etl_get_base_models(storage=None)

    assert str(exc_info.value) == "Webhook does not support getting base models."


@patch("src.services.local.filesystem.FileUtility.file_clean_timestamp_from_datetime")
@patch("src.services.local.filesystem.FileUtility.file_clean_string")
def test_etl_get_file_name(
    mock_clean_string: Mock,
    mock_clean_timestamp: Mock,
    sample_webhook: Webhook,
) -> None:
    """Test etl_get_file_name generates expected filename format."""
    # Setup mocks
    mock_clean_timestamp.return_value = "20240315_103000"
    mock_clean_string.return_value = "clean_test_meeting_title"

    # Call the method
    filename = sample_webhook.etl_get_file_name()

    # Verify the result
    expected_filename = "20240315_103000-rec_123456-clean_test_meeting_title.jsonl"
    assert filename == expected_filename

    # Verify utility methods were called correctly
    mock_clean_timestamp.assert_called_once_with(
        dt=sample_webhook.meeting.scheduled_start_time,
    )
    mock_clean_string.assert_called_once_with(
        string=sample_webhook.meeting.title,
    )
    sample_webhook.recording.get_recording_id_from_url.assert_called_once()


@patch("src.services.local.filesystem.FileUtility.file_clean_timestamp_from_datetime")
@patch("src.services.local.filesystem.FileUtility.file_clean_string")
def test_etl_get_file_name_empty_title(
    mock_clean_string: Mock,
    mock_clean_timestamp: Mock,
    sample_webhook: Webhook,
) -> None:
    """Test etl_get_file_name with empty meeting title."""
    # Setup mocks for empty title
    mock_clean_timestamp.return_value = "20240315_103000"
    mock_clean_string.return_value = ""

    filename = sample_webhook.etl_get_file_name()
    expected_filename = "20240315_103000-rec_123456-.jsonl"
    assert filename == expected_filename


def test_etl_get_json_basic_structure(
    sample_webhook: Webhook,
) -> None:
    """Test etl_get_json returns valid JSONL structure."""
    # Mock the model_dump method to return predictable data
    with patch.object(sample_webhook, "model_dump") as mock_model_dump:
        mock_model_dump.return_value = {
            "id": 12345,
            "nested": {
                "field1": "value1",
                "field2": "value2",
            },
        }

        # Call the method
        result = sample_webhook.etl_get_json(storage=None)

        # Verify it's a valid string
        assert isinstance(result, str)

        # Verify it contains JSONL format (newline-separated JSON objects)
        lines = result.strip().split("\n")
        assert len(lines) > 0

        # Each line should be valid JSON
        for line in lines:
            parsed = orjson.loads(line)
            assert isinstance(parsed, dict)
            # Each item should have recording_id and id fields
            assert "recording_id" in parsed
            assert "id" in parsed
            assert parsed["recording_id"] == "rec_123456"


def test_etl_get_json_with_flatsplode_data(
    sample_webhook: Webhook,
) -> None:
    """Test etl_get_json with nested data that gets flattened."""
    with patch.object(sample_webhook, "model_dump") as mock_model_dump:
        mock_model_dump.return_value = {
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
        }

        result = sample_webhook.etl_get_json(storage=None)
        lines = result.strip().split("\n")

        # Parse first line to check flattening
        first_item = orjson.loads(lines[0])

        # Check that nested fields are flattened with underscore separator
        assert "user_name" in first_item or "user" in first_item
        assert "recording_id" in first_item
        assert "id" in first_item
        assert first_item["id"] == "rec_123456-00001"


def test_etl_get_json_id_generation(
    sample_webhook: Webhook,
) -> None:
    """Test that etl_get_json generates sequential IDs correctly."""
    with patch.object(sample_webhook, "model_dump") as mock_model_dump:
        # Create data that will result in multiple flattened items
        mock_model_dump.return_value = {
            "id": 12345,
            "items": [
                {"name": "item1", "value": 100},
                {"name": "item2", "value": 200},
                {"name": "item3", "value": 300},
            ],
        }

        result = sample_webhook.etl_get_json(storage=None)
        lines = result.strip().split("\n")

        # Check that each line has a sequential ID
        for i, line in enumerate(lines, start=1):
            item = orjson.loads(line)
            expected_id = f"rec_123456-{i:05d}"
            assert item["id"] == expected_id


def test_etl_get_json_recording_id_already_present(
    sample_webhook: Webhook,
) -> None:
    """Test that etl_get_json doesn't override existing recording_id."""
    with patch.object(sample_webhook, "model_dump") as mock_model_dump:
        mock_model_dump.return_value = {
            "id": 12345,
            "recording_id": "existing_recording_id",
            "data": "test",
        }

        result = sample_webhook.etl_get_json(storage=None)
        lines = result.strip().split("\n")

        # The existing recording_id should be preserved
        first_item = orjson.loads(lines[0])
        assert first_item["recording_id"] == "existing_recording_id"


def test_etl_get_json_empty_data(
    sample_webhook: Webhook,
) -> None:
    """Test etl_get_json with minimal data."""
    with patch.object(sample_webhook, "model_dump") as mock_model_dump:
        mock_model_dump.return_value = {"id": 12345}

        result = sample_webhook.etl_get_json(storage=None)
        assert isinstance(result, str)

        lines = result.strip().split("\n")
        assert len(lines) == 1

        item = orjson.loads(lines[0])
        assert item["id"] == "rec_123456-00001"
        assert item["recording_id"] == "rec_123456"


def test_etl_get_json_storage_parameter_ignored(
    sample_webhook: Webhook,
) -> None:
    """Test that the storage parameter is properly ignored."""
    with patch.object(sample_webhook, "model_dump") as mock_model_dump:
        mock_model_dump.return_value = {"id": 12345}

        # Should work with None
        result1 = sample_webhook.etl_get_json(storage=None)
        assert isinstance(result1, str)

        # Should work with any value (gets deleted)
        result2 = sample_webhook.etl_get_json(storage="ignored_value")
        assert isinstance(result2, str)

        # Results should be identical
        assert result1 == result2


def test_webhook_pydantic_validation_missing_fields() -> None:
    """Test that Webhook requires all fields for creation."""
    from pydantic import ValidationError

    # Test missing required fields
    with pytest.raises(ValidationError) as exc_info:
        Webhook(id=123, recording=None, meeting=None, fathom_user=None, transcript=None)

    error_str = str(exc_info.value)
    required_fields = ["recording", "meeting", "fathom_user", "transcript"]

    # At least one required field should be mentioned in the error
    assert any(field in error_str for field in required_fields)


def test_webhook_with_none_values(
    mock_recording: Mock,
    mock_meeting: Mock,
    mock_fathom_user: Mock,
    mock_transcript: Mock,
) -> None:
    """Test that Webhook handles valid but edge case values."""
    # Test with zero ID
    webhook = Webhook(
        id=0,
        recording=mock_recording,
        meeting=mock_meeting,
        fathom_user=mock_fathom_user,
        transcript=mock_transcript,
    )
    assert webhook.id == 0

    # Test with negative ID
    webhook_negative = Webhook(
        id=-1,
        recording=mock_recording,
        meeting=mock_meeting,
        fathom_user=mock_fathom_user,
        transcript=mock_transcript,
    )
    assert webhook_negative.id == -1


def test_recording_method_calls(
    sample_webhook: Webhook,
) -> None:
    """Test that recording methods are called correctly."""
    # Test that get_recording_id_from_url is called in etl_get_file_name
    with patch("src.services.local.filesystem.FileUtility.file_clean_timestamp_from_datetime", return_value="timestamp"), \
         patch("src.services.local.filesystem.FileUtility.file_clean_string", return_value="title"):

        sample_webhook.etl_get_file_name()
        sample_webhook.recording.get_recording_id_from_url.assert_called()


def test_meeting_attributes_access(
    sample_webhook: Webhook,
) -> None:
    """Test that meeting attributes are accessed correctly."""
    # Test that meeting attributes are used in etl_get_file_name
    with patch("src.services.local.filesystem.FileUtility.file_clean_timestamp_from_datetime", return_value="timestamp") as mock_timestamp, \
         patch("src.services.local.filesystem.FileUtility.file_clean_string", return_value="title") as mock_string:

        sample_webhook.etl_get_file_name()

        # Verify the meeting attributes were accessed
        mock_timestamp.assert_called_once_with(dt=sample_webhook.meeting.scheduled_start_time)
        mock_string.assert_called_once_with(string=sample_webhook.meeting.title)


def test_model_dump_called_in_etl_get_json(
    sample_webhook: Webhook,
) -> None:
    """Test that model_dump is called with correct parameters in etl_get_json."""
    with patch.object(sample_webhook, "model_dump") as mock_model_dump:
        mock_model_dump.return_value = {"id": 12345}

        sample_webhook.etl_get_json(storage=None)

        # Verify model_dump was called with json mode
        mock_model_dump.assert_called_once_with(mode="json")


# trunk-ignore-end(ruff/PLR2004,ruff/S101,pyright/reportArgumentType,ruff/PLC0415)
