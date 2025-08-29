from __future__ import annotations

import json
import os
from datetime import datetime
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
from opentelemetry.semconv.resource import ResourceAttributes

if TYPE_CHECKING:
    from chalk.client.response import OnlineQueryResult

APP_NAME: str = "chalk-fathom-calls"

image: Image = modal.Image.debian_slim().uv_pip_install(
    "chalkpy",
    "polars",
    "opentelemetry-api",
    "opentelemetry-sdk",
    "opentelemetry-exporter-otlp-proto-http",
    "opentelemetry-semantic-conventions",
)

app = modal.App(
    name=APP_NAME,
    image=image,
)

# ---------------------------------------------------------------------------


def setup_otel(hyperdx_api_key: str) -> trace.Tracer:
    """Setup OpenTelemetry tracing with HyperDX."""
    resource = Resource.create(
        {
            ResourceAttributes.SERVICE_NAME: "data_gen-fathom-calls",
            ResourceAttributes.SERVICE_VERSION: "1.0.0",
        },
    )

    trace.set_tracer_provider(TracerProvider(resource=resource))

    otlp_exporter = OTLPSpanExporter(
        endpoint="https://in-otel.hyperdx.io/v1/traces",
        headers={"authorization": hyperdx_api_key},
    )

    span_processor = BatchSpanProcessor(otlp_exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)  # type: ignore[union-attr]

    return trace.get_tracer(__name__)


def convert_datetimes(obj: Any) -> Any:  # trunk-ignore(ruff/ANN401)
    """Recursively convert datetime objects to ISO strings."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {key: convert_datetimes(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [convert_datetimes(item) for item in obj]
    return obj


def main(
    branch: str = "",
) -> None:
    # 1️⃣ Get environment variables (from Modal secrets)
    client_id: str = os.environ["CHALK_CLIENT_ID"]
    client_secret: str = os.environ["CHALK_CLIENT_SECRET"]
    hyperdx_api_key: str = os.environ["HYPERDX_API_KEY"]

    # 2️⃣ Setup OpenTelemetry
    tracer: trace.Tracer = setup_otel(hyperdx_api_key)

    # 3️⃣ Initialise Chalk client
    client: ChalkClient = ChalkClient(
        client_id=client_id,
        client_secret=client_secret,
        **({"branch": branch} if branch else {}),
    )

    # 4️⃣ Load recording IDs
    call_ids_data: pl.DataFrame = pl.read_csv(
        source="data/fathom/call_ids.csv",
        has_header=True,
        schema_overrides={"recording_id": pl.Utf8},
    )
    branch_used: str = branch if branch else "default"
    print(f"Loaded call IDs data with branch: {branch_used}")

    # 5️⃣ Iterate over recordings, query, clean & log
    with tracer.start_as_current_span("fathom_call_processing") as span:
        span.set_attribute("total_recordings", len(call_ids_data))
        span.set_attribute("branch", branch_used)

        for i, recording_id in enumerate(call_ids_data["recording_id"]):
            with tracer.start_as_current_span("process_recording") as recording_span:
                recording_span.set_attribute("recording_id", recording_id)
                recording_span.set_attribute("recording_index", i)

                try:
                    # Query Chalk
                    query: OnlineQueryResult = client.query(
                        input={"fathom_call.id": recording_id},
                        output=[
                            "fathom_call.id",
                            "fathom_call.webhook_status_code",
                        ],
                    )
                    data: dict[str, Any] = query.to_dict()

                    # Extract webhook status code for span attributes
                    webhook_status_code = data.get("fathom_call.webhook_status_code")
                    if webhook_status_code is not None:
                        recording_span.set_attribute(
                            "webhook_status_code",
                            webhook_status_code,
                        )
                    # Remove unwanted fields
                    keys_to_remove = [
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

                    cleaned_dict: Any = convert_datetimes(data)

                    # Log the data as JSON
                    recording_span.add_event(
                        "fathom_call_data",
                        {
                            "recording_id": recording_id,
                            "data": json.dumps(cleaned_dict),
                            "fields_removed_count": removed_count,
                            "webhook_status_code": webhook_status_code,
                        },
                    )

                    recording_span.set_attribute("status", "success")
                    print(
                        f"Logged data for recording {recording_id} with webhook status {webhook_status_code}",
                    )

                except Exception as e:
                    recording_span.set_attribute("status", "error")
                    recording_span.set_attribute("error", str(e))
                    recording_span.set_attribute("webhook_status_code", "unknown")
                    recording_span.add_event(
                        "error",
                        {
                            "message": str(e),
                            "recording_id": recording_id,
                        },
                    )
                    print(f"Error processing recording {recording_id}: {e}")
                    raise


@app.local_entrypoint()
def local(branch: str = "", mode: str = "") -> None:
    """Local entrypoint for testing the Fathom calls processor."""
    match mode.lower():
        case "test":
            test_otel_connection()

        case _:
            main(branch=branch)


if __name__ == "__main__":
    main()


# TESTS =========
def test_otel_connection() -> None:
    """Test OpenTelemetry connection with fake data."""
    hyperdx_api_key: str = os.environ["HYPERDX_API_KEY"]

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

            fake_data = {
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
