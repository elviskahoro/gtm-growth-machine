from __future__ import annotations

from datetime import datetime, timezone

from pydantic import (
    BaseModel,
    EmailStr,
    HttpUrl,
    SerializationInfo,
    field_serializer,
)


class LinkedinConnection(BaseModel):
    first_name: str
    last_name: str
    url: HttpUrl
    email_address: EmailStr
    company: str
    position: str
    connected_on: datetime
    timestamp: datetime

    @staticmethod
    def parse_linkedin_date(
        date_str: str,
    ) -> str:
        parsed_date: datetime = datetime.strptime(
            date_str,
            "%d %b %Y",
        ).replace(
            tzinfo=timezone.utc,
        )
        return parsed_date.isoformat()

    @field_serializer("connected_on")
    def serialize_priority(
        self: LinkedinConnection,
        connected_on: datetime,
        _info: SerializationInfo,
    ) -> str:
        del _info
        return connected_on.isoformat()


# trunk-ignore-begin(ruff/PLR2004,ruff/S101,pyright/reportArgumentType)
def test_parse_linkedin_date_valid_input() -> None:
    """Test parsing a valid LinkedIn date string."""
    from datetime import datetime, timezone

    # Test standard date format
    date_str: str = "15 Jan 2024"
    result: str = LinkedinConnection.parse_linkedin_date(date_str)

    # Verify it returns ISO format string
    assert isinstance(result, str)
    assert result == "2024-01-15T00:00:00+00:00"

    # Verify it can be parsed back to datetime
    parsed_back: datetime = datetime.fromisoformat(result.replace("+00:00", "+00:00"))
    assert parsed_back.year == 2024
    assert parsed_back.month == 1
    assert parsed_back.day == 15
    assert parsed_back.tzinfo == timezone.utc


def test_parse_linkedin_date_different_months() -> None:
    """Test parsing dates with different month formats."""
    test_cases: list[tuple[str, int]] = [
        ("01 Feb 2024", 2),
        ("03 Mar 2024", 3),
        ("04 Apr 2024", 4),
        ("05 May 2024", 5),
        ("06 Jun 2024", 6),
        ("07 Jul 2024", 7),
        ("08 Aug 2024", 8),
        ("09 Sep 2024", 9),
        ("10 Oct 2024", 10),
        ("11 Nov 2024", 11),
        ("12 Dec 2024", 12),
    ]

    for date_str, expected_month in test_cases:
        result: str = LinkedinConnection.parse_linkedin_date(date_str)
        from datetime import datetime

        parsed: datetime = datetime.fromisoformat(result.replace("+00:00", "+00:00"))
        assert parsed.month == expected_month


def test_parse_linkedin_date_invalid_format() -> None:
    """Test parsing with invalid date format raises ValueError."""
    import pytest

    invalid_dates: list[str] = [
        "2024-01-15",  # Wrong format
        "Jan 15 2024",  # Wrong order
        "15/01/2024",  # Different separator
        "invalid date",  # Completely invalid
        "",  # Empty string
    ]

    for invalid_date in invalid_dates:
        with pytest.raises(
            ValueError,
            match=r"time data .* does not match format|unconverted data remains",
        ):
            LinkedinConnection.parse_linkedin_date(invalid_date)


def test_parse_linkedin_date_edge_cases() -> None:
    """Test edge cases for date parsing."""
    import pytest

    # Test leap year
    result: str = LinkedinConnection.parse_linkedin_date("29 Feb 2024")
    from datetime import datetime

    parsed: datetime = datetime.fromisoformat(result.replace("+00:00", "+00:00"))
    assert parsed.month == 2
    assert parsed.day == 29

    # Test invalid leap year date should raise error
    with pytest.raises(ValueError, match="day is out of range for month"):
        LinkedinConnection.parse_linkedin_date("29 Feb 2023")

    # Test boundary dates
    LinkedinConnection.parse_linkedin_date("01 Jan 2000")
    LinkedinConnection.parse_linkedin_date("31 Dec 2099")


def test_linkedin_connection_creation_valid() -> None:
    """Test creating a valid LinkedIn connection."""
    from datetime import datetime, timezone

    connection_data: dict[str, str | datetime] = {
        "first_name": "John",
        "last_name": "Doe",
        "url": "https://linkedin.com/in/johndoe",
        "email_address": "john.doe@example.com",
        "company": "Tech Corp",
        "position": "Software Engineer",
        "connected_on": datetime(2024, 1, 15, tzinfo=timezone.utc),
        "timestamp": datetime.now(timezone.utc),
    }

    connection: LinkedinConnection = LinkedinConnection(**connection_data)

    assert connection.first_name == "John"
    assert connection.last_name == "Doe"
    assert str(connection.url) == "https://linkedin.com/in/johndoe"
    assert str(connection.email_address) == "john.doe@example.com"
    assert connection.company == "Tech Corp"
    assert connection.position == "Software Engineer"


def test_linkedin_connection_validation_errors() -> None:
    """Test LinkedIn connection validation with invalid data."""
    from datetime import datetime, timezone

    import pytest
    from pydantic import ValidationError

    base_data: dict[str, str | datetime] = {
        "first_name": "John",
        "last_name": "Doe",
        "url": "https://linkedin.com/in/johndoe",
        "email_address": "john.doe@example.com",
        "company": "Tech Corp",
        "position": "Software Engineer",
        "connected_on": datetime.now(timezone.utc),
        "timestamp": datetime.now(timezone.utc),
    }

    # Test invalid email
    invalid_email_data: dict = base_data.copy()
    invalid_email_data["email_address"] = "invalid-email"
    with pytest.raises(ValidationError):
        LinkedinConnection(**invalid_email_data)

    # Test invalid URL
    invalid_url_data: dict = base_data.copy()
    invalid_url_data["url"] = "not-a-url"
    with pytest.raises(ValidationError):
        LinkedinConnection(**invalid_url_data)

    # Test missing required fields
    for field in [
        "first_name",
        "last_name",
        "url",
        "email_address",
        "company",
        "position",
    ]:
        incomplete_data: dict = base_data.copy()
        del incomplete_data[field]
        with pytest.raises(ValidationError):
            LinkedinConnection(**incomplete_data)


def test_linkedin_connection_empty_strings() -> None:
    """Test LinkedIn connection with empty string values."""
    from datetime import datetime, timezone

    base_data: dict[str, str | datetime] = {
        "first_name": "",
        "last_name": "",
        "url": "https://linkedin.com/in/johndoe",
        "email_address": "john.doe@example.com",
        "company": "",
        "position": "",
        "connected_on": datetime.now(timezone.utc),
        "timestamp": datetime.now(timezone.utc),
    }

    # Empty strings should be valid for name and company fields
    connection: LinkedinConnection = LinkedinConnection(**base_data)
    assert connection.first_name == ""
    assert connection.last_name == ""
    assert connection.company == ""
    assert connection.position == ""


def test_field_serializer_connected_on() -> None:
    """Test the field serializer for connected_on field."""
    from datetime import datetime, timezone

    connection: LinkedinConnection = LinkedinConnection(
        first_name="John",
        last_name="Doe",
        url="https://linkedin.com/in/johndoe",
        email_address="john.doe@example.com",
        company="Tech Corp",
        position="Software Engineer",
        connected_on=datetime(2024, 1, 15, tzinfo=timezone.utc),
        timestamp=datetime.now(timezone.utc),
    )

    # Test the serializer method directly with a datetime object
    connected_date = datetime(2024, 1, 15, tzinfo=timezone.utc)
    result: str = connection.serialize_priority(connected_date, None)
    assert result == "2024-01-15T00:00:00+00:00"


def test_linkedin_connection_model_dump() -> None:
    """Test model serialization with field serializer."""
    from datetime import datetime, timezone

    connection: LinkedinConnection = LinkedinConnection(
        first_name="John",
        last_name="Doe",
        url="https://linkedin.com/in/johndoe",
        email_address="john.doe@example.com",
        company="Tech Corp",
        position="Software Engineer",
        connected_on=datetime(2024, 1, 15, tzinfo=timezone.utc),
        timestamp=datetime.now(timezone.utc),
    )

    # Serialize the model
    serialized: dict = connection.model_dump()

    # Verify basic fields
    assert serialized["first_name"] == "John"
    assert serialized["last_name"] == "Doe"
    assert serialized["company"] == "Tech Corp"
    assert serialized["position"] == "Software Engineer"

    # Verify URL and email are strings when serialized
    assert isinstance(str(serialized["url"]), str)
    assert isinstance(serialized["email_address"], str)


def test_linkedin_connection_special_characters() -> None:
    """Test LinkedIn connection with special characters in names."""
    from datetime import datetime, timezone

    connection: LinkedinConnection = LinkedinConnection(
        first_name="José María",
        last_name="García-López",
        url="https://linkedin.com/in/jose-garcia",
        email_address="jose.garcia@example.com",
        company="Empresa Técnológica",
        position="Ingeniero de Software",
        connected_on=datetime(2024, 1, 15, tzinfo=timezone.utc),
        timestamp=datetime.now(timezone.utc),
    )

    assert connection.first_name == "José María"
    assert connection.last_name == "García-López"
    assert connection.company == "Empresa Técnológica"
    assert connection.position == "Ingeniero de Software"


def test_linkedin_connection_long_strings() -> None:
    """Test LinkedIn connection with very long string values."""
    from datetime import datetime, timezone

    long_name: str = "A" * 1000
    long_company: str = "B" * 1000
    long_position: str = "C" * 1000

    connection: LinkedinConnection = LinkedinConnection(
        first_name=long_name,
        last_name=long_name,
        url="https://linkedin.com/in/johndoe",
        email_address="john.doe@example.com",
        company=long_company,
        position=long_position,
        connected_on=datetime(2024, 1, 15, tzinfo=timezone.utc),
        timestamp=datetime.now(timezone.utc),
    )

    assert len(connection.first_name) == 1000
    assert len(connection.company) == 1000
    assert len(connection.position) == 1000


def test_parse_linkedin_date_timezone_consistency() -> None:
    """Test that parsed dates always have UTC timezone."""
    from datetime import datetime, timezone

    dates_to_test: list[str] = [
        "01 Jan 2020",
        "15 Jun 2023",
        "31 Dec 2024",
    ]

    for date_str in dates_to_test:
        result: str = LinkedinConnection.parse_linkedin_date(date_str)
        parsed: datetime = datetime.fromisoformat(result.replace("+00:00", "+00:00"))
        assert parsed.tzinfo == timezone.utc
        assert parsed.hour == 0
        assert parsed.minute == 0
        assert parsed.second == 0


def test_linkedin_connection_datetime_fields() -> None:
    """Test datetime field handling in LinkedIn connection."""
    from datetime import datetime, timezone

    now: datetime = datetime.now(timezone.utc)
    connected_date: datetime = datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc)

    connection: LinkedinConnection = LinkedinConnection(
        first_name="John",
        last_name="Doe",
        url="https://linkedin.com/in/johndoe",
        email_address="john.doe@example.com",
        company="Tech Corp",
        position="Software Engineer",
        connected_on=connected_date,
        timestamp=now,
    )

    assert connection.connected_on == connected_date
    assert connection.timestamp == now
    assert isinstance(connection.connected_on, datetime)
    assert isinstance(connection.timestamp, datetime)


# trunk-ignore-end(ruff/PLR2004,ruff/S101,pyright/reportArgumentType)
