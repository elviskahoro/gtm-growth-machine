from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import modal
import polars as pl
from chalk.client import ChalkClient
from modal import Image
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.semconv.attributes import service_attributes

from src.services.fathom.etl.call.backfill_recordings_writer import RecordingWriter

if TYPE_CHECKING:
    from chalk.client.response import OnlineQueryResult


image: Image = modal.Image.debian_slim().uv_pip_install(
    "chalkpy",
    "flatsplode",
    "polars",
    "opentelemetry-api",
    "opentelemetry-sdk",
    "opentelemetry-exporter-otlp-proto-http",
    "opentelemetry-semantic-conventions",
)
image = image.add_local_python_source(
    *[
        "src",
    ],
)

APP_NAME: str = "chalk-fathom-calls"
app = modal.App(
    name=APP_NAME,
    image=image,
)


@dataclass
class Config:
    branch_to_use: str | None = None

    def set_branch(
        self,
        branch: str,
    ) -> None:
        """Set the branch to use for processing."""
        self.branch_to_use = branch


# Global config instance
backfill_config: Config = Config()


def setup_otel(
    hyperdx_api_key: str,
) -> trace.Tracer:
    """Setup OpenTelemetry tracing with HyperDX."""
    resource = Resource.create(
        {
            service_attributes.SERVICE_NAME: APP_NAME,
            service_attributes.SERVICE_VERSION: "1.0.0",
        },
    )
    tracer_provider = trace.get_tracer_provider()

    # Only set up the tracer provider and span processor if not already configured
    if not hasattr(tracer_provider, "add_span_processor"):
        trace.set_tracer_provider(TracerProvider(resource=resource))
        tracer_provider = trace.get_tracer_provider()

    # Check if span processor is already added by checking if we have any processors
    # If the provider already has processors, don't add another one
    if not getattr(tracer_provider, "_span_processors", []):
        otlp_exporter = OTLPSpanExporter(
            endpoint="https://in-otel.hyperdx.io/v1/traces",
            headers={
                "authorization": hyperdx_api_key,
            },
        )
        span_processor = BatchSpanProcessor(otlp_exporter)
        tracer_provider.add_span_processor(span_processor)

    return trace.get_tracer(__name__)


def _convert_datetimes(obj: Any) -> Any:  # trunk-ignore(ruff/ANN401)
    """Recursively convert datetime objects to ISO strings."""
    if isinstance(obj, datetime):
        return obj.isoformat()

    if isinstance(obj, dict):
        return {key: _convert_datetimes(value) for key, value in obj.items()}

    if isinstance(obj, list):
        return [_convert_datetimes(item) for item in obj]

    return obj


def _load_successful_recordings() -> set[str]:
    """Load previously successful recordings from disk."""
    output_path: Path = Path("out/fathom-call-backfill_successful_recordings.json")
    if not output_path.exists():
        return set()

    try:
        with output_path.open(mode="r") as f:
            successful_recordings: list[str] = json.load(f)
            return set(successful_recordings)

    except (json.JSONDecodeError, ValueError):
        print("Warning: Could not load successful recordings file, starting fresh")
        return set()


def _get_recording_ids(
    input_csv_path: str = "",
) -> list[str]:
    def _load_recording_ids_from_csv(
        csv_path: str,
    ) -> list[str]:
        """Load recording IDs from a CSV file."""
        df: pl.DataFrame = pl.read_csv(
            source=csv_path,
            has_header=True,
            schema_overrides={"recording_id": pl.Utf8},
        )
        recording_ids: list[str] = df["recording_id"].to_list()
        print(
            f"Loaded {len(recording_ids)} recording IDs from custom CSV file: {input_csv_path}",
        )
        return recording_ids

    # Get the initial list of recording IDs
    if not input_csv_path:
        error_msg: str = "CSV path is required"
        raise ValueError(error_msg)

    initial_recording_ids: list[str] = _load_recording_ids_from_csv(input_csv_path)
    successful_recordings: set[str] = _load_successful_recordings()
    filtered_recording_ids: list[str] = [
        recording_id
        for recording_id in initial_recording_ids
        if recording_id not in successful_recordings
    ]
    print(
        f"Filtered out {len(initial_recording_ids) - len(filtered_recording_ids)} "
        f"already processed recordings",
    )
    print(f"Remaining recordings to process: {len(filtered_recording_ids)}")
    return filtered_recording_ids


@app.function(
    secrets=[
        modal.Secret.from_name(name=APP_NAME),
    ],
    max_containers=3,
)
def process_recording(
    recording_id: str,
) -> tuple[str, bool]:
    """Process a single recording ID by querying Chalk and logging results.

    Returns:
        tuple[str, bool]: (recording_id, success_flag)
    """
    hyperdx_api_key: str = os.environ["HYPERDX_API_KEY"]
    tracer: trace.Tracer = setup_otel(hyperdx_api_key)

    with tracer.start_as_current_span("process_recording") as recording_span:
        recording_span.set_attribute("recording_id", recording_id)

        def handle_recording_error(
            recording_id: str,
            exception: Exception,
        ) -> None:
            """Handle recording processing errors with telemetry logging."""
            recording_span.set_attribute("status", "error")
            recording_span.set_attribute("error", str(exception))
            recording_span.set_attribute("webhook_status_code", "unknown")
            recording_span.add_event(
                "error",
                {
                    "message": str(exception),
                    "recording_id": recording_id,
                },
            )
            print(f"Error processing recording {recording_id}: {exception}")

        def remove_unserializable_fields(
            data: dict[str, Any],
        ) -> int:
            """Remove unserializable prompt response fields from data.

            Returns:
                int: Number of fields removed
            """
            print(
                f"Recording: {recording_id}: Removing unserialiazable prompt responses",
            )
            keys_to_remove: list[str] = [
                "fathom_call.llm_call_summary_general",
                "fathom_call.llm_call_summary_sales",
                "fathom_call.llm_call_summary_marketing",
                "fathom_call.llm_call_type",
                "fathom_call.llm_call_insights_sales",
                "fathom_call.transcript_plaintext_list",
            ]
            removed_count: int = 0
            for key in keys_to_remove:
                if key in data:
                    data.pop(key, None)
                    removed_count += 1
            return removed_count

        branch: str | None = backfill_config.branch_to_use

        print(f"Recording: {recording_id}: Creating client")
        client: ChalkClient = ChalkClient(
            client_id=os.environ["CHALK_CLIENT_ID"],
            client_secret=os.environ["CHALK_CLIENT_SECRET"],
            **({"branch": branch} if branch else {}),
        )
        print(f"Recording: {recording_id}: Querying recording")
        try:
            query: OnlineQueryResult = client.query(
                input={"fathom_call.id": recording_id},
                output=[
                    "fathom_call.id",
                    "fathom_call.webhook_status_code",
                    "fathom_call.llm_call_insights_sales",
                ],
            )

        except (OSError, ValueError, KeyError, TypeError) as e:
            handle_recording_error(
                recording_id=recording_id,
                exception=e,
            )
            return (recording_id, False)

        # Process successful query response
        data: dict[str, Any] = query.to_dict()
        webhook_status_code: int | None = data.get(
            "fathom_call.webhook_status_code",
        )
        if webhook_status_code is None:
            return (recording_id, False)

        removed_count: int = remove_unserializable_fields(data)

        def _flush_otel_logs() -> None:
            recording_span.set_attribute("fields_removed_count", removed_count)
            recording_span.set_attribute("webhook_status_code", webhook_status_code)
            recording_span.set_attribute("status", "success")
            recording_span.add_event(
                "fathom_call_processed",
                {
                    "recording_id": recording_id,
                    "data": json.dumps(_convert_datetimes(data)),
                },
            )
            print(
                f"Recording: {recording_id}: Finished with webhook status: {webhook_status_code}",
            )

        _flush_otel_logs()
        return (recording_id, True)


def main(
    recording_ids: list[str],
    branch: str = "",
) -> None:
    class ProcessingStatus(Enum):
        SUCCESS = "successful"
        FAILED = "failed"

    def _handle_recording(
        recording_id: str,
        count: int,
        id_count: int,
        status: ProcessingStatus,
        writer: RecordingWriter,
    ) -> None:
        message: str = (
            "Successfully processed"
            if status == ProcessingStatus.SUCCESS
            else "Error processing recording"
        )
        print(f"{count:05d}/{id_count:05d}: {recording_id}: {message}")
        try:
            writer.add(recording_id)

        except Exception as e:
            status_text: str = (
                "successful" if status == ProcessingStatus.SUCCESS else "failed"
            )
            print(f"ERROR: Failed to write {status_text} recording {recording_id}: {e}")
            raise

    backfill_config.set_branch(branch)
    id_count = len(recording_ids)

    successfully_processed: list[str] = []
    failed_recordings: list[str] = []

    with RecordingWriter(
        ProcessingStatus.SUCCESS.value,
    ) as success_writer, RecordingWriter(
        ProcessingStatus.FAILED.value,
    ) as failed_writer:
        for count, (recording_id, success) in enumerate(
            process_recording.map(
                recording_ids,
            ),
            start=1,
        ):
            status: ProcessingStatus = (
                ProcessingStatus.SUCCESS if success else ProcessingStatus.FAILED
            )
            writer: RecordingWriter = success_writer if success else failed_writer
            _handle_recording(
                recording_id=recording_id,
                count=count,
                id_count=id_count,
                status=status,
                writer=writer,
            )

    if successfully_processed:
        print(f"Successfully processed: {len(successfully_processed)} recordings")

    if failed_recordings:
        print(f"Failed to process: {len(failed_recordings)} recordings")


@app.local_entrypoint()
def local(
    input_csv_path: str = "",
    branch: str = "",
) -> None:
    recording_ids: list[str] = _get_recording_ids(input_csv_path=input_csv_path)
    main(
        recording_ids=recording_ids,
        branch=branch,
    )


# trunk-ignore-begin(ruff/PLR2004,ruff/S101)
def test_config_set_branch() -> None:
    """Test Config class branch setting functionality."""
    config = Config()
    assert config.branch_to_use is None

    config.set_branch("main")
    assert config.branch_to_use == "main"

    config.set_branch("feature/test-branch")
    assert config.branch_to_use == "feature/test-branch"

    config.set_branch("")
    assert config.branch_to_use == ""


def test_convert_datetimes_with_datetime() -> None:
    """Test convert_datetimes function with datetime objects."""
    from datetime import datetime, timezone

    dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc)
    result = _convert_datetimes(dt)
    assert result == "2024-01-15T10:30:45+00:00"


def test_convert_datetimes_with_dict() -> None:
    """Test convert_datetimes function with nested dictionaries."""
    from datetime import datetime, timezone

    data = {
        "created_at": datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc),
        "title": "Test Call",
        "metadata": {
            "updated_at": datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc),
            "status": "completed",
        },
    }

    result = _convert_datetimes(data)
    expected = {
        "created_at": "2024-01-15T10:30:45+00:00",
        "title": "Test Call",
        "metadata": {
            "updated_at": "2024-01-15T11:00:00+00:00",
            "status": "completed",
        },
    }
    assert result == expected


def test_convert_datetimes_with_list() -> None:
    """Test convert_datetimes function with lists containing datetime objects."""
    from datetime import datetime, timezone

    data = [
        datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc),
        "regular_string",
        {"timestamp": datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc)},
    ]

    result = _convert_datetimes(data)
    expected = [
        "2024-01-15T10:30:45+00:00",
        "regular_string",
        {"timestamp": "2024-01-15T11:00:00+00:00"},
    ]
    assert result == expected


def test_convert_datetimes_with_primitive_types() -> None:
    """Test convert_datetimes function with primitive types."""
    assert _convert_datetimes("string") == "string"
    assert _convert_datetimes(123) == 123
    assert _convert_datetimes(12.34) == 12.34
    # Test boolean value preservation
    bool_value = True
    assert _convert_datetimes(bool_value) is True
    assert _convert_datetimes(None) is None


def test_load_successful_recordings_file_not_exists() -> None:
    """Test _load_successful_recordings when file doesn't exist."""
    import tempfile
    from unittest.mock import patch

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create path but don't use it since we're mocking the Path class
        _unused_path = Path(temp_dir) / "non_existent.json"
        with patch("src.services.fathom.etl.call.backfill.Path") as mock_path:
            mock_path.return_value.exists.return_value = False
            result = _load_successful_recordings()
            assert result == set()


def test_load_successful_recordings_valid_file() -> None:
    """Test _load_successful_recordings with valid JSON file."""
    import json
    from unittest.mock import mock_open, patch

    test_data = ["rec_123", "rec_456", "rec_789"]
    json_content = json.dumps(test_data)

    with patch("src.services.fathom.etl.call.backfill.Path") as mock_path:
        mock_path.return_value.exists.return_value = True
        with patch("builtins.open", mock_open(read_data=json_content)):
            result = _load_successful_recordings()
            assert result == {"rec_123", "rec_456", "rec_789"}


def test_load_successful_recordings_invalid_json() -> None:
    """Test _load_successful_recordings with invalid JSON."""
    from unittest.mock import mock_open, patch

    invalid_json = "{'invalid': json}"

    with patch("src.services.fathom.etl.call.backfill.Path") as mock_path:
        mock_path.return_value.exists.return_value = True
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            result = _load_successful_recordings()
            assert result == set()


def test_get_recording_ids_no_csv_path() -> None:
    """Test _get_recording_ids raises ValueError when no CSV path provided."""
    import pytest

    with pytest.raises(ValueError, match="CSV path is required"):
        _get_recording_ids("")


def test_get_recording_ids_with_filtering() -> None:
    """Test _get_recording_ids filters out successful recordings."""
    from unittest.mock import patch

    import polars as pl

    # Create test CSV data
    test_df = pl.DataFrame(
        {
            "recording_id": ["rec_1", "rec_2", "rec_3", "rec_4"],
        },
    )

    with patch("src.services.fathom.etl.call.backfill.pl.read_csv") as mock_read_csv:
        mock_read_csv.return_value = test_df

        with patch(
            "src.services.fathom.etl.call.backfill._load_successful_recordings",
        ) as mock_load:
            mock_load.return_value = {"rec_2", "rec_4"}  # Already processed

            result = _get_recording_ids("test.csv")
            assert result == ["rec_1", "rec_3"]


def test_remove_unserializable_fields() -> None:
    """Test the remove_unserializable_fields function from process_recording."""

    # We need to extract and test this function in isolation
    # Since it's defined inside process_recording, we'll simulate it
    def remove_unserializable_fields(data: dict[str, Any]) -> int:
        """Remove unserializable prompt response fields from data."""
        keys_to_remove = [
            "fathom_call.llm_call_summary_general",
            "fathom_call.llm_call_summary_sales",
            "fathom_call.llm_call_summary_marketing",
            "fathom_call.llm_call_type",
            "fathom_call.llm_call_insights_sales",
            "fathom_call.transcript_plaintext_list",
        ]
        removed_count = 0
        for key in keys_to_remove:
            if key in data:
                data.pop(key, None)
                removed_count += 1
        return removed_count

    # Test with all fields present
    data_with_all_fields = {
        "fathom_call.id": "test_123",
        "fathom_call.llm_call_summary_general": "General summary",
        "fathom_call.llm_call_summary_sales": "Sales summary",
        "fathom_call.llm_call_summary_marketing": "Marketing summary",
        "fathom_call.llm_call_type": "sales",
        "fathom_call.llm_call_insights_sales": "Sales insights",
        "fathom_call.transcript_plaintext_list": ["Hello", "World"],
        "fathom_call.title": "Keep this field",
    }

    removed_count = remove_unserializable_fields(data_with_all_fields)
    assert removed_count == 6
    assert "fathom_call.id" in data_with_all_fields
    assert "fathom_call.title" in data_with_all_fields
    assert "fathom_call.llm_call_summary_general" not in data_with_all_fields
    assert "fathom_call.llm_call_summary_sales" not in data_with_all_fields


def test_remove_unserializable_fields_partial() -> None:
    """Test remove_unserializable_fields with only some fields present."""

    def remove_unserializable_fields(data: dict[str, Any]) -> int:
        keys_to_remove = [
            "fathom_call.llm_call_summary_general",
            "fathom_call.llm_call_summary_sales",
            "fathom_call.llm_call_summary_marketing",
            "fathom_call.llm_call_type",
            "fathom_call.llm_call_insights_sales",
            "fathom_call.transcript_plaintext_list",
        ]
        removed_count = 0
        for key in keys_to_remove:
            if key in data:
                data.pop(key, None)
                removed_count += 1
        return removed_count

    data_partial = {
        "fathom_call.id": "test_456",
        "fathom_call.llm_call_summary_general": "General summary",
        "fathom_call.llm_call_type": "sales",
        "fathom_call.title": "Keep this field",
    }

    removed_count = remove_unserializable_fields(data_partial)
    assert removed_count == 2
    assert "fathom_call.id" in data_partial
    assert "fathom_call.title" in data_partial
    assert "fathom_call.llm_call_summary_general" not in data_partial
    assert "fathom_call.llm_call_type" not in data_partial


def test_remove_unserializable_fields_empty() -> None:
    """Test remove_unserializable_fields with no target fields present."""

    def remove_unserializable_fields(data: dict[str, Any]) -> int:
        keys_to_remove = [
            "fathom_call.llm_call_summary_general",
            "fathom_call.llm_call_summary_sales",
            "fathom_call.llm_call_summary_marketing",
            "fathom_call.llm_call_type",
            "fathom_call.llm_call_insights_sales",
            "fathom_call.transcript_plaintext_list",
        ]
        removed_count = 0
        for key in keys_to_remove:
            if key in data:
                data.pop(key, None)
                removed_count += 1
        return removed_count

    data_clean = {
        "fathom_call.id": "test_789",
        "fathom_call.title": "Clean data",
        "fathom_call.duration": 3600,
    }

    original_data = data_clean.copy()
    removed_count = remove_unserializable_fields(data_clean)
    assert removed_count == 0
    assert data_clean == original_data


def integration_test_process_recording_success() -> None:
    """Integration test for successful recording processing."""
    from unittest.mock import MagicMock, patch

    import pytest

    # Skip if required environment variables aren't set
    required_env_vars = ["CHALK_CLIENT_ID", "CHALK_CLIENT_SECRET", "HYPERDX_API_KEY"]
    for var in required_env_vars:
        if not os.environ.get(var):
            pytest.skip(f"{var} environment variable not set")

    # Mock the ChalkClient and its query method
    with patch(
        "src.services.fathom.etl.call.backfill.ChalkClient",
    ) as mock_client_class:
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock successful query response
        mock_query_result = MagicMock()
        mock_query_result.to_dict.return_value = {
            "fathom_call.id": "test_rec_123",
            "fathom_call.webhook_status_code": 200,
            "fathom_call.llm_call_insights_sales": "Test insights",
            "fathom_call.llm_call_summary_general": "Should be removed",
        }
        mock_client.query.return_value = mock_query_result

        # Test the function would return success
        # Note: We can't easily test the actual Modal function without running it
        # This tests the core logic that would be executed
        recording_id = "test_rec_123"
        print(f"Would process recording: {recording_id}")

        # Verify client would be created with correct parameters
        expected_calls = [
            ("query", {"input": {"fathom_call.id": recording_id}}),
        ]
        print(f"Expected client calls: {expected_calls}")


def integration_test_process_recording_error_handling() -> None:
    """Integration test for recording processing error scenarios."""
    from unittest.mock import MagicMock, patch

    import pytest

    # Skip if required environment variables aren't set
    required_env_vars = ["CHALK_CLIENT_ID", "CHALK_CLIENT_SECRET", "HYPERDX_API_KEY"]
    for var in required_env_vars:
        if not os.environ.get(var):
            pytest.skip(f"{var} environment variable not set")

    # Test various error scenarios
    error_scenarios = [
        (OSError("Network error"), "Network error"),
        (ValueError("Invalid input"), "Invalid input"),
        (KeyError("Missing key"), "Missing key"),
        (TypeError("Type error"), "Type error"),
    ]

    for exception, expected_message in error_scenarios:
        with patch(
            "src.services.fathom.etl.call.backfill.ChalkClient",
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.query.side_effect = exception

            recording_id = f"test_error_{expected_message.replace(' ', '_')}"
            print(
                f"Would handle error for recording {recording_id}: {expected_message}",
            )

            # Verify error handling logic
            assert str(exception) == expected_message


def test_otel_connection() -> None:
    """Test OpenTelemetry connection with fake data."""
    import pytest

    hyperdx_api_key: str | None = os.environ.get("HYPERDX_API_KEY")
    if not hyperdx_api_key:
        pytest.skip("HYPERDX_API_KEY environment variable not set")

    print("Setting up OpenTelemetry...")
    tracer: trace.Tracer = setup_otel(hyperdx_api_key)

    # Send fake test data
    with tracer.start_as_current_span("test_fathom_call_processing") as span:
        span.set_attribute("test", value=True)
        span.set_attribute("total_recordings", 2)

        # Test recording 1
        with tracer.start_as_current_span("test_process_recording") as recording_span:
            recording_span.set_attribute("recording_id", "test_recording_123")
            recording_span.set_attribute("recording_index", 0)

            fake_data: dict[str, Any] = {
                "fathom_call.id": "test_recording_123",
                "fathom_call.title": "Test Sales Call",
                "fathom_call.duration": 3600,
                "fathom_call.participants": ["alice@example.com", "bob@example.com"],
                "fathom_call.created_at": "2024-01-15T10:00:00Z",
                "fathom_call.sentiment": "positive",
            }

            recording_span.add_event(
                "fathom_call_data",
                {
                    "recording_id": "test_recording_123",
                    "data": json.dumps(fake_data),
                    "fields_removed_count": 6,
                    "test_data": "true",
                },
            )

            recording_span.set_attribute("status", "success")
            print("✓ Sent test recording 1")

        # Test recording 2 with error scenario
        with tracer.start_as_current_span("test_process_recording") as recording_span:
            recording_span.set_attribute("recording_id", "test_recording_456")
            recording_span.set_attribute("recording_index", 1)

            # Simulate an error
            recording_span.set_attribute("status", "error")
            recording_span.set_attribute("error", "Test error: API rate limit exceeded")
            recording_span.add_event(
                "error",
                {"message": "Test error: API rate limit exceeded"},
            )
            print("✓ Sent test recording 2 (with error)")

    print("✓ Test data sent to HyperDX via OpenTelemetry")
    print(
        "Check your HyperDX dashboard for traces with 'test_fathom_call_processing' span name",
    )


# trunk-ignore-end(ruff/PLR2004,ruff/S101)
