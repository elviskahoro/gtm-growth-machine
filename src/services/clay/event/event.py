from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import polars as pl
from pydantic import BaseModel, field_validator

from src.services.local.filesystem_regex import sanitize_string

if TYPE_CHECKING:
    import polars as pl


class EventAttendee(BaseModel):
    name: str | None = None
    email: str | None = None
    source: str
    event_url: str | None = None
    created_at: datetime

    @field_validator("event_url")
    @classmethod
    def validate_event_url(
        cls: type[EventAttendee],
        v: str | None,
    ) -> str | None:
        """Validate and process the event URL."""
        if v is None:
            return v
        # Add any URL validation logic here if needed
        # For example: ensure it's a valid URL format
        return v

    @classmethod
    def get_field_names(cls: type[EventAttendee]) -> list[str]:
        """Get all field names from the EventAttendee model.

        Returns:
            List of field names in the model
        """
        return list(cls.model_fields.keys())

    @staticmethod
    def get_polars_columns_for_base_model(
        source: str,
        created_at: datetime,
        event_url: str | None = None,
    ) -> list[pl.Expr]:
        """Generate polars column expressions for EventAttendee base fields.

        Args:
            source: The source identifier
            created_at: The creation timestamp
            event_url: Optional event URL

        Returns:
            List of polars expressions to add the base model columns

        Raises:
            ValueError: If provided parameter names don't match model field names
        """
        import polars as pl

        # Get field names from the model to validate against
        field_names: list[str] = EventAttendee.get_field_names()

        # Define parameter mapping - what we're providing values for
        provided_params: dict[str, str | datetime | None] = {
            "source": source,
            "created_at": created_at,
            "event_url": event_url,
        }

        # Validate that all provided parameters are actual model fields
        invalid_params = set(provided_params.keys()) - set(field_names)
        if invalid_params:
            msg = (
                f"Invalid parameters {invalid_params}. Valid fields are: {field_names}"
            )
            raise ValueError(msg)

        # Build columns only for parameters that have non-None values
        columns: list[pl.Expr] = []
        for param_name, param_value in provided_params.items():
            if param_value is not None:
                columns.append(pl.lit(param_value).alias(param_name))

        return columns

    def etl_get_file_name(
        self: EventAttendee,
        extension: str = ".jsonl",
    ) -> str:
        """Generate a file name in format: event_title-date-name.

        Args:
            extension: The file extension to use. Defaults to ".jsonl".

        Returns:
            The generated file name with event title, date, and identifier.
        """
        # Use email if available, otherwise fall back to name
        identifier: str | None = self.email or self.name
        date_str: str = self.created_at.strftime("%Y%m%d")

        if identifier:
            # Clean the identifier for filesystem compatibility using sanitize_string
            cleaned_identifier: str = sanitize_string(identifier.replace("@", "·"))
            return f"{self.source}-{date_str}-{cleaned_identifier}{extension}".lower()

        # Fallback to original format if no identifier available
        return f"{self.source}-{date_str}{extension}".lower()


# trunk-ignore-begin(ruff/S101,ruff/PGH003)
def test_event_attendee_model_creation() -> None:
    """Test creating EventAttendee instances with various field combinations."""
    from datetime import timezone

    # Test with all fields
    attendee: EventAttendee = EventAttendee(
        name="John Doe",
        email="john.doe@example.com",
        source="conference_2024",
        event_url="https://example.com/event",
        created_at=datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc),
    )
    assert attendee.name == "John Doe"
    assert attendee.email == "john.doe@example.com"
    assert attendee.source == "conference_2024"
    assert attendee.event_url == "https://example.com/event"
    assert attendee.created_at == datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc)

    # Test with minimal fields (only required ones)
    minimal_attendee: EventAttendee = EventAttendee(
        source="webinar",
        created_at=datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc),
    )
    assert minimal_attendee.name is None
    assert minimal_attendee.email is None
    assert minimal_attendee.source == "webinar"
    assert minimal_attendee.event_url is None


def test_etl_get_file_name_with_email() -> None:
    """Test file name generation when email is provided."""
    from datetime import timezone

    attendee: EventAttendee = EventAttendee(
        name="John Doe",
        email="john.doe@example.com",
        source="conference_2024",
        created_at=datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc),
    )

    # Default extension
    filename: str = attendee.etl_get_file_name()
    # Dots are removed by sanitize_string
    assert filename == "conference_2024-20240315-johndoe·examplecom.jsonl"

    # Custom extension
    filename_csv: str = attendee.etl_get_file_name(".csv")
    assert filename_csv == "conference_2024-20240315-johndoe·examplecom.csv"


def test_etl_get_file_name_with_name_only() -> None:
    """Test file name generation when only name is provided (no email)."""
    from datetime import timezone

    attendee: EventAttendee = EventAttendee(
        name="Jane Smith",
        source="webinar_2024",
        created_at=datetime(2024, 3, 15, 14, 45, 30, tzinfo=timezone.utc),
    )

    filename: str = attendee.etl_get_file_name()
    assert filename == "webinar_2024-20240315-jane·smith.jsonl"


def test_etl_get_file_name_no_identifier() -> None:
    """Test file name generation when neither name nor email is provided."""
    from datetime import timezone

    attendee: EventAttendee = EventAttendee(
        source="anonymous_event",
        created_at=datetime(2024, 3, 15, 18, 0, 0, tzinfo=timezone.utc),
    )

    filename: str = attendee.etl_get_file_name()
    assert filename == "anonymous_event-20240315.jsonl"


def test_etl_get_file_name_special_characters() -> None:
    """Test file name generation with special characters in email/name."""
    from datetime import timezone

    # Test with email containing special characters
    attendee_special_email: EventAttendee = EventAttendee(
        email="user+tag@sub.domain.com",
        source="special_event",
        created_at=datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc),
    )
    filename: str = attendee_special_email.etl_get_file_name()
    # Dots are removed by sanitize_string
    assert filename == "special_event-20240315-user+tag·subdomaincom.jsonl"

    # Test with name containing special characters
    attendee_special_name: EventAttendee = EventAttendee(
        name="John O'Connor Jr.",
        source="irish_event",
        created_at=datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc),
    )
    filename: str = attendee_special_name.etl_get_file_name()
    # Apostrophe is removed, dots are removed
    assert filename == "irish_event-20240315-john·oconnor·jr.jsonl"


def test_etl_get_file_name_filesystem_unsafe_characters() -> None:
    """Test that filesystem-unsafe characters are properly sanitized."""
    from datetime import timezone

    # Test with various unsafe characters
    attendee: EventAttendee = EventAttendee(
        name="Test: User (With) <Special> Characters!",
        source="test/source",
        created_at=datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc),
    )
    filename: str = attendee.etl_get_file_name()
    # Note: source doesn't get sanitized, only the identifier (name/email) gets sanitized
    # Source slash remains as slash, and various special chars are removed from name
    assert filename == "test/source-20240315-test·user·with·special·characters.jsonl"


def test_etl_get_file_name_case_sensitivity() -> None:
    """Test that file names are converted to lowercase."""
    from datetime import timezone

    attendee: EventAttendee = EventAttendee(
        email="John.DOE@EXAMPLE.COM",
        source="UPPERCASE_EVENT",
        created_at=datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc),
    )
    filename: str = attendee.etl_get_file_name()
    # Dots are removed by sanitize_string
    assert filename == "uppercase_event-20240315-johndoe·examplecom.jsonl"
    assert filename.islower()


def test_event_attendee_pydantic_validation() -> None:
    """Test Pydantic validation for EventAttendee model."""
    from datetime import timezone

    import pytest
    from pydantic import ValidationError

    # Test missing required fields
    with pytest.raises(ValidationError) as exc_info:
        EventAttendee(source="test")  # type: ignore  # Missing created_at

    assert "created_at" in str(exc_info.value)

    with pytest.raises(ValidationError) as exc_info:
        EventAttendee(created_at=datetime.now(tz=timezone.utc))  # type: ignore  # Missing source

    assert "source" in str(exc_info.value)


def test_etl_get_file_name_datetime_formatting() -> None:
    """Test correct datetime formatting in file names."""
    from datetime import timezone

    # Test with single-digit day/month/hour/minute/second
    attendee: EventAttendee = EventAttendee(
        email="test@example.com",
        source="event",
        created_at=datetime(2024, 1, 5, 8, 5, 5, tzinfo=timezone.utc),
    )
    filename: str = attendee.etl_get_file_name()
    # Should have zero-padding, dots removed
    assert filename == "event-20240105-test·examplecom.jsonl"


def test_etl_get_file_name_email_priority() -> None:
    """Test that email takes priority over name when both are provided."""
    from datetime import timezone

    attendee: EventAttendee = EventAttendee(
        name="John Doe",
        email="different.email@example.com",
        source="priority_test",
        created_at=datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc),
    )
    filename: str = attendee.etl_get_file_name()
    # Should use email, not name. Dots are removed.
    assert filename == "priority_test-20240315-differentemail·examplecom.jsonl"
    assert "john·doe" not in filename


# trunk-ignore-end(ruff/S101,ruff/PGH003)
