# trunk-ignore-all(trunk/ignore-does-nothing)
from __future__ import annotations

import re
from datetime import datetime, timezone  # trunk-ignore(ruff/TC003)
from re import Match
from typing import TYPE_CHECKING

import pyarrow as pa
from pydantic import BaseModel, ValidationError, validate_email

from .speaker import Speaker
from .transcript_message_watch_link_data import TranscriptMessageWatchLinkData

if TYPE_CHECKING:
    from collections.abc import Iterator


class TranscriptMessage(BaseModel):
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
    def lance_get_vector_index_type() -> str:
        return "IVF_HNSW_SQ"

    @staticmethod
    def lance_get_vector_index_cache_size() -> int:
        return 512

    @staticmethod
    def lance_get_vector_index_metric() -> str:
        return "cosine"

    @staticmethod
    def lance_get_vector_column_name() -> str:
        return "embedding"

    @staticmethod
    def lance_get_vector_dimension() -> int:
        return 768

    @staticmethod
    def lance_get_primary_key_index_type() -> str:
        return "BTREE"

    @staticmethod
    def lance_get_primary_key() -> str:
        primary_key: str = "id"
        fields: set[str] = set(TranscriptMessage.model_fields.keys())
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
                        TranscriptMessage.lance_get_vector_dimension(),
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
        cls: type[TranscriptMessage],
        line: str,
        recording_id: str,
        message_id: int,
        url: str,
        title: str,
        date: datetime,
        speaker_map: dict[str, str],
    ) -> TranscriptMessage | None:
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
            timestamp=TranscriptMessage.convert_timestamp_to_seconds(
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
        self: TranscriptMessage,
        line: str,
    ) -> None:
        """Process a content line and update the TranscriptMessage instance."""
        match line:
            case str() as content if content.startswith("ACTION ITEM:"):
                self.action_item = content[len("ACTION ITEM:") :].strip()

            case str() as content if content.startswith("- WATCH:"):
                watch_data: TranscriptMessageWatchLinkData = (
                    TranscriptMessageWatchLinkData.parse_watch_link(
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
    ) -> Iterator[TranscriptMessage]:
        line_index: int = 0
        message_index: int = 1
        current_transcript_message: TranscriptMessage | None = None
        while line_index < len(lines):
            line: str = lines[line_index].strip()
            if not line:
                line_index += 1
                continue

            new_transcript_message: TranscriptMessage | None = (
                TranscriptMessage.parse_timestamp_line(
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


# trunk-ignore-begin(ruff/PLR2004,ruff/S101,pyright/reportArgumentType)
def test_gemini_get_column_to_embed() -> None:
    assert TranscriptMessage.gemini_get_column_to_embed() == "message"


def test_lance_get_project_name() -> None:
    assert TranscriptMessage.lance_get_project_name() == "fathom-8ywo6z"


def test_lance_get_table_name() -> None:
    assert TranscriptMessage.lance_get_table_name() == "fathom-messages"


def test_lance_get_vector_index_type() -> None:
    assert TranscriptMessage.lance_get_vector_index_type() == "IVF_HNSW_SQ"


def test_lance_get_vector_index_cache_size() -> None:
    assert TranscriptMessage.lance_get_vector_index_cache_size() == 512


def test_lance_get_vector_index_metric() -> None:
    assert TranscriptMessage.lance_get_vector_index_metric() == "cosine"


def test_lance_get_vector_column_name() -> None:
    assert TranscriptMessage.lance_get_vector_column_name() == "embedding"


def test_lance_get_vector_dimension() -> None:
    assert TranscriptMessage.lance_get_vector_dimension() == 768


def test_lance_get_primary_key_index_type() -> None:
    assert TranscriptMessage.lance_get_primary_key_index_type() == "BTREE"


def test_lance_get_primary_key_success() -> None:
    assert TranscriptMessage.lance_get_primary_key() == "id"


def test_lance_get_primary_key_field_not_found() -> None:
    from unittest.mock import patch

    import pytest

    # Mock model_fields to not include 'id'
    with (
        patch.object(TranscriptMessage, "model_fields", {"other_field": None}),
        pytest.raises(ValueError, match="Primary key id not found in model fields"),
    ):
        TranscriptMessage.lance_get_primary_key()


def test_lance_get_schema() -> None:
    schema = TranscriptMessage.lance_get_schema()
    assert isinstance(schema, pa.Schema)

    field_names: list[str] = [field.name for field in schema]
    expected_fields: list[str] = [
        "id",
        "recording_id",
        "message_id",
        "url",
        "title",
        "date",
        "timestamp",
        "speaker",
        "organization",
        "message",
        "action_item",
        "watch_link",
        "embedding",
    ]
    assert field_names == expected_fields

    # Check specific field types
    assert schema.field("id").type == pa.string()
    assert schema.field("message_id").type == pa.int32()
    assert schema.field("date").type == pa.timestamp("us")
    assert schema.field("organization").nullable is True
    assert schema.field("action_item").nullable is True
    assert schema.field("watch_link").nullable is True
    assert schema.field("embedding").nullable is True


def test_convert_timestamp_to_seconds_mm_ss_format() -> None:
    # Test MM:SS format
    result: int = TranscriptMessage.convert_timestamp_to_seconds("05:30")
    assert result == 330  # 5*60 + 30


def test_convert_timestamp_to_seconds_hh_mm_ss_format() -> None:
    # Test HH:MM:SS format
    result: int = TranscriptMessage.convert_timestamp_to_seconds("1:05:30")
    assert result == 3930  # 1*3600 + 5*60 + 30


def test_convert_timestamp_to_seconds_with_decimal_seconds() -> None:
    # Test with decimal seconds
    result: int = TranscriptMessage.convert_timestamp_to_seconds("1:05:30.5")
    assert result == 3930  # int(float("30.5")) = 30


def test_convert_timestamp_to_seconds_invalid_format() -> None:
    import pytest

    with pytest.raises(ValueError, match="Invalid timestamp format: 5"):
        TranscriptMessage.convert_timestamp_to_seconds("5")

    with pytest.raises(ValueError, match="Invalid timestamp format: 1:2:3:4"):
        TranscriptMessage.convert_timestamp_to_seconds("1:2:3:4")


def test_parse_timestamp_line_success() -> None:
    line: str = "05:30 - John Doe (Acme Corp)"
    recording_id: str = "rec123"
    message_id: int = 1
    url: str = "https://example.com"
    title: str = "Test Meeting"
    date: datetime = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    speaker_map: dict[str, str] = {"john doe": "john@example.com"}

    result: TranscriptMessage | None = TranscriptMessage.parse_timestamp_line(
        line=line,
        recording_id=recording_id,
        message_id=message_id,
        url=url,
        title=title,
        date=date,
        speaker_map=speaker_map,
    )

    assert result is not None
    assert result.id == "rec123-00001"
    assert result.timestamp == 330  # 5*60 + 30
    assert result.speaker == "john@example.com"
    assert result.organization == "example.com"
    assert result.message == ""
    assert result.action_item is None
    assert result.watch_link is None
    assert result.recording_id == recording_id
    assert result.message_id == message_id
    assert result.url == url
    assert result.title == title
    assert result.date == date


def test_parse_timestamp_line_without_organization() -> None:
    line: str = "05:30 - John Doe"
    recording_id: str = "rec123"
    message_id: int = 1
    url: str = "https://example.com"
    title: str = "Test Meeting"
    date: datetime = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    speaker_map: dict[str, str] = {"john doe": "john@example.com"}

    result: TranscriptMessage | None = TranscriptMessage.parse_timestamp_line(
        line=line,
        recording_id=recording_id,
        message_id=message_id,
        url=url,
        title=title,
        date=date,
        speaker_map=speaker_map,
    )

    assert result is not None
    assert result.speaker == "john@example.com"
    assert result.organization == "example.com"


def test_parse_timestamp_line_invalid_email_with_org() -> None:
    line: str = "05:30 - Unknown Speaker (Acme Corp)"
    recording_id: str = "rec123"
    message_id: int = 1
    url: str = "https://example.com"
    title: str = "Test Meeting"
    date: datetime = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    speaker_map: dict[str, str] = {}

    result: TranscriptMessage | None = TranscriptMessage.parse_timestamp_line(
        line=line,
        recording_id=recording_id,
        message_id=message_id,
        url=url,
        title=title,
        date=date,
        speaker_map=speaker_map,
    )

    assert result is not None
    assert result.speaker == "Unknown Speaker"
    # Since email validation fails, organization should be None
    assert result.organization is None


def test_parse_timestamp_line_no_match() -> None:
    line: str = "This is not a valid timestamp line"
    recording_id: str = "rec123"
    message_id: int = 1
    url: str = "https://example.com"
    title: str = "Test Meeting"
    date: datetime = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    speaker_map: dict[str, str] = {}

    result: TranscriptMessage | None = TranscriptMessage.parse_timestamp_line(
        line=line,
        recording_id=recording_id,
        message_id=message_id,
        url=url,
        title=title,
        date=date,
        speaker_map=speaker_map,
    )

    assert result is None


def test_process_content_line_action_item() -> None:
    message: TranscriptMessage = TranscriptMessage(
        id="test-00001",
        recording_id="rec123",
        message_id=1,
        url="https://example.com",
        title="Test Meeting",
        date=datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        timestamp=300,
        speaker="john@example.com",
        organization="example.com",
        message="",
        action_item=None,
        watch_link=None,
    )

    message.process_content_line("ACTION ITEM: Follow up on budget")
    assert message.action_item == "Follow up on budget"


def test_process_content_line_watch_link() -> None:
    from unittest.mock import patch

    message: TranscriptMessage = TranscriptMessage(
        id="test-00001",
        recording_id="rec123",
        message_id=1,
        url="https://example.com",
        title="Test Meeting",
        date=datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        timestamp=300,
        speaker="john@example.com",
        organization="example.com",
        message="",
        action_item=None,
        watch_link=None,
    )

    with patch.object(
        TranscriptMessageWatchLinkData,
        "parse_watch_link",
        return_value=TranscriptMessageWatchLinkData(
            watch_link="https://watch.example.com",
            remaining_text="with additional context",
        ),
    ):
        message.process_content_line(
            "- WATCH: https://watch.example.com with additional context",
        )
        assert message.watch_link == "https://watch.example.com"
        assert message.message == "with additional context"


def test_process_content_line_watch_link_append_to_existing_message() -> None:
    from unittest.mock import patch

    message: TranscriptMessage = TranscriptMessage(
        id="test-00001",
        recording_id="rec123",
        message_id=1,
        url="https://example.com",
        title="Test Meeting",
        date=datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        timestamp=300,
        speaker="john@example.com",
        organization="example.com",
        message="Existing message",
        action_item=None,
        watch_link=None,
    )

    with patch.object(
        TranscriptMessageWatchLinkData,
        "parse_watch_link",
        return_value=TranscriptMessageWatchLinkData(
            watch_link="https://watch.example.com",
            remaining_text="additional context",
        ),
    ):
        message.process_content_line(
            "- WATCH: https://watch.example.com additional context",
        )
        assert message.watch_link == "https://watch.example.com"
        assert message.message == "Existing message additional context"


def test_process_content_line_regular_content() -> None:
    message: TranscriptMessage = TranscriptMessage(
        id="test-00001",
        recording_id="rec123",
        message_id=1,
        url="https://example.com",
        title="Test Meeting",
        date=datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        timestamp=300,
        speaker="john@example.com",
        organization="example.com",
        message="",
        action_item=None,
        watch_link=None,
    )

    message.process_content_line("This is a regular message")
    assert message.message == "This is a regular message"

    message.process_content_line("More content")
    assert message.message == "This is a regular message More content"


def test_process_content_line_empty_string() -> None:
    message: TranscriptMessage = TranscriptMessage(
        id="test-00001",
        recording_id="rec123",
        message_id=1,
        url="https://example.com",
        title="Test Meeting",
        date=datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        timestamp=300,
        speaker="john@example.com",
        organization="example.com",
        message="Original message",
        action_item=None,
        watch_link=None,
    )

    # Empty string should be skipped
    message.process_content_line("")
    assert message.message == "Original message"  # unchanged


def test_process_content_line_invalid_type() -> None:
    import pytest

    message: TranscriptMessage = TranscriptMessage(
        id="test-00001",
        recording_id="rec123",
        message_id=1,
        url="https://example.com",
        title="Test Meeting",
        date=datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        timestamp=300,
        speaker="john@example.com",
        organization="example.com",
        message="",
        action_item=None,
        watch_link=None,
    )

    with pytest.raises(ValueError, match="Invalid line: 123"):
        message.process_content_line(123)


def test_parse_transcript_lines_single_message() -> None:
    lines: list[str] = [
        "05:30 - John Doe (Acme Corp)",
        "This is the message content",
        "More content",
    ]
    recording_id: str = "rec123"
    url: str = "https://example.com"
    title: str = "Test Meeting"
    date: datetime = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    speaker_map: dict[str, str] = {"john doe": "john@example.com"}

    messages: list[TranscriptMessage] = list(
        TranscriptMessage.parse_transcript_lines(
            lines=lines,
            recording_id=recording_id,
            url=url,
            title=title,
            date=date,
            speaker_map=speaker_map,
        ),
    )

    assert len(messages) == 1
    message: TranscriptMessage = messages[0]
    assert message.id == "rec123-00001"
    assert message.timestamp == 330
    assert message.speaker == "john@example.com"
    assert message.organization == "example.com"
    assert message.message == "This is the message content More content"


def test_parse_transcript_lines_multiple_messages() -> None:
    lines: list[str] = [
        "05:30 - John Doe",
        "First message",
        "10:45 - Jane Smith",
        "Second message",
        "ACTION ITEM: Do something",
    ]
    recording_id: str = "rec123"
    url: str = "https://example.com"
    title: str = "Test Meeting"
    date: datetime = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    speaker_map: dict[str, str] = {
        "john doe": "john@example.com",
        "jane smith": "jane@example.com",
    }

    messages: list[TranscriptMessage] = list(
        TranscriptMessage.parse_transcript_lines(
            lines=lines,
            recording_id=recording_id,
            url=url,
            title=title,
            date=date,
            speaker_map=speaker_map,
        ),
    )

    assert len(messages) == 2

    first_message: TranscriptMessage = messages[0]
    assert first_message.id == "rec123-00001"
    assert first_message.speaker == "john@example.com"
    assert first_message.message == "First message"
    assert first_message.action_item is None

    second_message: TranscriptMessage = messages[1]
    assert second_message.id == "rec123-00002"
    assert second_message.speaker == "jane@example.com"
    assert second_message.message == "Second message"
    assert second_message.action_item == "Do something"


def test_parse_transcript_lines_empty_lines() -> None:
    lines: list[str] = [
        "",
        "05:30 - John Doe",
        "",
        "Message content",
        "",
        "10:45 - Jane Smith",
        "Another message",
        "",
    ]
    recording_id: str = "rec123"
    url: str = "https://example.com"
    title: str = "Test Meeting"
    date: datetime = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    speaker_map: dict[str, str] = {
        "john doe": "john@example.com",
        "jane smith": "jane@example.com",
    }

    messages: list[TranscriptMessage] = list(
        TranscriptMessage.parse_transcript_lines(
            lines=lines,
            recording_id=recording_id,
            url=url,
            title=title,
            date=date,
            speaker_map=speaker_map,
        ),
    )

    assert len(messages) == 2
    assert messages[0].message == "Message content"
    assert messages[1].message == "Another message"


def test_parse_transcript_lines_content_without_timestamp() -> None:
    import pytest

    lines: list[str] = [
        "This is content without a timestamp",
        "05:30 - John Doe",
        "Valid message",
    ]
    recording_id: str = "rec123"
    url: str = "https://example.com"
    title: str = "Test Meeting"
    date: datetime = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    speaker_map: dict[str, str] = {"john doe": "john@example.com"}

    with pytest.raises(
        ValueError,
        match="No transcript message found for line: This is content without a timestamp",
    ):
        list(
            TranscriptMessage.parse_transcript_lines(
                lines=lines,
                recording_id=recording_id,
                url=url,
                title=title,
                date=date,
                speaker_map=speaker_map,
            ),
        )


def test_parse_transcript_lines_empty_list() -> None:
    lines: list[str] = []
    recording_id: str = "rec123"
    url: str = "https://example.com"
    title: str = "Test Meeting"
    date: datetime = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    speaker_map: dict[str, str] = {}

    messages: list[TranscriptMessage] = list(
        TranscriptMessage.parse_transcript_lines(
            lines=lines,
            recording_id=recording_id,
            url=url,
            title=title,
            date=date,
            speaker_map=speaker_map,
        ),
    )

    assert len(messages) == 0


def test_transcript_message_model_validation() -> None:
    import pytest

    # Test that the Pydantic model validates required fields
    with pytest.raises(ValidationError):
        TranscriptMessage(  # trunk-ignore(pyright/reportCallIssue)
            # Creating without any required fields to trigger validation error
        )

    # Test valid model creation
    message: TranscriptMessage = TranscriptMessage(
        id="test-00001",
        recording_id="rec123",
        message_id=1,
        url="https://example.com",
        title="Test Meeting",
        date=datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        timestamp=300,
        speaker="john@example.com",
        organization="example.com",
        message="Test message",
        action_item=None,
        watch_link=None,
    )

    assert message.id == "test-00001"
    assert message.timestamp == 300
    assert message.speaker == "john@example.com"
    assert message.message == "Test message"


def test_parse_timestamp_line_hh_mm_ss_format() -> None:
    line: str = "1:05:30 - John Doe (Acme Corp)"
    recording_id: str = "rec123"
    message_id: int = 1
    url: str = "https://example.com"
    title: str = "Test Meeting"
    date: datetime = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    speaker_map: dict[str, str] = {"john doe": "john@example.com"}

    result: TranscriptMessage | None = TranscriptMessage.parse_timestamp_line(
        line=line,
        recording_id=recording_id,
        message_id=message_id,
        url=url,
        title=title,
        date=date,
        speaker_map=speaker_map,
    )

    assert result is not None
    assert result.timestamp == 3930  # 1*3600 + 5*60 + 30


# trunk-ignore-end(ruff/PLR2004,ruff/S101,pyright/reportArgumentType)
