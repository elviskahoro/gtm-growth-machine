from datetime import datetime

from pydantic import BaseModel, ValidationError


class ExternalDomain(BaseModel):
    domain_name: str


class Invitee(BaseModel):
    name: str
    email: str
    is_external: bool


class Meeting(BaseModel):
    scheduled_start_time: datetime
    scheduled_end_time: datetime | None
    scheduled_duration_in_minutes: int | None
    join_url: str
    title: str
    has_external_invitees: bool | None
    external_domains: list[ExternalDomain] | None
    invitees: list[Invitee] | None


# trunk-ignore-begin(ruff/PLR2004,ruff/S101,pyright/reportArgumentType)
def test_external_domain_valid() -> None:
    """Test creating a valid ExternalDomain instance."""
    domain: ExternalDomain = ExternalDomain(domain_name="example.com")
    assert domain.domain_name == "example.com"
    assert domain.model_dump() == {"domain_name": "example.com"}


def test_external_domain_validation() -> None:
    """Test ExternalDomain validation errors."""
    import pytest

    # Test missing required field
    with pytest.raises(ValidationError) as exc_info:
        ExternalDomain(domain_name=None)
    assert "domain_name" in str(exc_info.value)

    # Test empty string (should be allowed)
    domain: ExternalDomain = ExternalDomain(domain_name="")
    assert domain.domain_name == ""


def test_external_domain_serialization() -> None:
    """Test ExternalDomain serialization and deserialization."""
    domain: ExternalDomain = ExternalDomain(domain_name="test.org")

    # Test model_dump
    dumped: dict[str, str] = domain.model_dump()
    assert dumped == {"domain_name": "test.org"}

    # Test model_validate
    restored: ExternalDomain = ExternalDomain.model_validate(dumped)
    assert restored.domain_name == domain.domain_name


# Tests for Invitee
def test_invitee_valid() -> None:
    """Test creating a valid Invitee instance."""
    invitee: Invitee = Invitee(
        name="John Doe",
        email="john.doe@example.com",
        is_external=True,
    )
    assert invitee.name == "John Doe"
    assert invitee.email == "john.doe@example.com"
    assert invitee.is_external is True


def test_invitee_validation() -> None:
    """Test Invitee validation errors."""
    import pytest

    # Test missing required fields
    with pytest.raises(ValidationError) as exc_info:
        Invitee(name=None, email=None, is_external=None)
    error_str: str = str(exc_info.value)
    assert "name" in error_str
    assert "email" in error_str
    assert "is_external" in error_str

    # Test partial fields
    with pytest.raises(ValidationError) as exc_info:
        Invitee(name="John", email="john@example.com", is_external=None)
    assert "is_external" in str(exc_info.value)


def test_invitee_is_external_variations() -> None:
    """Test boolean variations for is_external field."""
    # Test with False
    invitee_internal: Invitee = Invitee(
        name="Jane Doe",
        email="jane@company.com",
        is_external=False,
    )
    assert invitee_internal.is_external is False

    # Test with boolean-like values
    invitee_true: Invitee = Invitee(
        name="External User",
        email="external@other.com",
        is_external=1,  # Should be coerced to True
    )
    assert invitee_true.is_external is True


def test_invitee_serialization() -> None:
    """Test Invitee serialization and deserialization."""
    invitee: Invitee = Invitee(
        name="Test User",
        email="test@example.com",
        is_external=False,
    )

    # Test model_dump
    dumped: dict[str, str | bool] = invitee.model_dump()
    assert dumped == {
        "name": "Test User",
        "email": "test@example.com",
        "is_external": False,
    }

    # Test model_validate
    restored: Invitee = Invitee.model_validate(dumped)
    assert restored.name == invitee.name
    assert restored.email == invitee.email
    assert restored.is_external == invitee.is_external


# Tests for Meeting
def test_meeting_minimal() -> None:
    """Test creating a Meeting with only required fields."""
    from datetime import timezone

    start_time: datetime = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    meeting: Meeting = Meeting(
        scheduled_start_time=start_time,
        scheduled_end_time=None,
        scheduled_duration_in_minutes=None,
        join_url="https://zoom.us/j/123456789",
        title="Team Standup",
        has_external_invitees=None,
        external_domains=None,
        invitees=None,
    )
    assert meeting.scheduled_start_time == start_time
    assert meeting.join_url == "https://zoom.us/j/123456789"
    assert meeting.title == "Team Standup"


def test_meeting_complete() -> None:
    """Test creating a Meeting with all fields populated."""
    from datetime import timezone

    start_time: datetime = datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc)
    end_time: datetime = datetime(2024, 1, 15, 15, 0, 0, tzinfo=timezone.utc)

    external_domains: list[ExternalDomain] = [
        ExternalDomain(domain_name="client.com"),
        ExternalDomain(domain_name="partner.org"),
    ]

    invitees: list[Invitee] = [
        Invitee(name="Alice", email="alice@company.com", is_external=False),
        Invitee(name="Bob", email="bob@client.com", is_external=True),
    ]

    meeting: Meeting = Meeting(
        scheduled_start_time=start_time,
        scheduled_end_time=end_time,
        scheduled_duration_in_minutes=60,
        join_url="https://meet.google.com/abc-defg-hij",
        title="Client Review Meeting",
        has_external_invitees=True,
        external_domains=external_domains,
        invitees=invitees,
    )

    assert meeting.scheduled_start_time == start_time
    assert meeting.scheduled_end_time == end_time
    assert meeting.scheduled_duration_in_minutes == 60
    assert meeting.has_external_invitees is True
    assert len(meeting.external_domains) == 2
    assert len(meeting.invitees) == 2


def test_meeting_optional_fields() -> None:
    """Test Meeting with None values for optional fields."""
    from datetime import timezone

    meeting: Meeting = Meeting(
        scheduled_start_time=datetime.now(tz=timezone.utc),
        scheduled_end_time=None,
        scheduled_duration_in_minutes=None,
        join_url="https://example.com/meeting",
        title="Quick Sync",
        has_external_invitees=None,
        external_domains=None,
        invitees=None,
    )

    assert meeting.scheduled_end_time is None
    assert meeting.scheduled_duration_in_minutes is None
    assert meeting.has_external_invitees is None
    assert meeting.external_domains is None
    assert meeting.invitees is None


def test_meeting_datetime_validation() -> None:
    """Test Meeting datetime parsing and validation."""
    import pytest

    # Test with string datetime (should be parsed)
    meeting: Meeting = Meeting(
        scheduled_start_time="2024-01-15T10:00:00",
        scheduled_end_time="2024-01-15T11:00:00",
        scheduled_duration_in_minutes=60,
        join_url="https://example.com",
        title="Meeting",
        has_external_invitees=False,
        external_domains=[],
        invitees=[],
    )

    assert isinstance(meeting.scheduled_start_time, datetime)
    assert isinstance(meeting.scheduled_end_time, datetime)
    assert meeting.scheduled_start_time.hour == 10
    assert meeting.scheduled_end_time.hour == 11

    # Test with invalid datetime string
    with pytest.raises(ValidationError) as exc_info:
        Meeting(
            scheduled_start_time="not-a-datetime",
            scheduled_end_time=None,
            scheduled_duration_in_minutes=None,
            join_url="https://example.com",
            title="Meeting",
            has_external_invitees=None,
            external_domains=None,
            invitees=None,
        )
    assert "scheduled_start_time" in str(exc_info.value)


def test_meeting_list_fields() -> None:
    """Test Meeting with empty and populated list fields."""
    from datetime import timezone

    # Test with empty lists
    meeting_empty: Meeting = Meeting(
        scheduled_start_time=datetime.now(tz=timezone.utc),
        scheduled_end_time=None,
        scheduled_duration_in_minutes=30,
        join_url="https://example.com",
        title="Empty Lists Meeting",
        has_external_invitees=False,
        external_domains=[],
        invitees=[],
    )

    assert meeting_empty.external_domains == []
    assert meeting_empty.invitees == []
    assert meeting_empty.has_external_invitees is False

    # Test with populated lists
    meeting_full: Meeting = Meeting(
        scheduled_start_time=datetime.now(tz=timezone.utc),
        scheduled_end_time=None,
        scheduled_duration_in_minutes=45,
        join_url="https://example.com",
        title="Full Lists Meeting",
        has_external_invitees=True,
        external_domains=[
            ExternalDomain(domain_name="external1.com"),
            ExternalDomain(domain_name="external2.com"),
        ],
        invitees=[
            Invitee(name="User1", email="user1@company.com", is_external=False),
            Invitee(name="User2", email="user2@external1.com", is_external=True),
        ],
    )

    assert len(meeting_full.external_domains) == 2
    assert len(meeting_full.invitees) == 2
    assert meeting_full.external_domains is not None
    assert meeting_full.invitees is not None
    assert meeting_full.external_domains[0].domain_name == "external1.com"
    assert meeting_full.invitees[1].is_external is True


def test_meeting_serialization() -> None:
    """Test Meeting JSON serialization with datetime handling."""
    from datetime import timezone

    start_time: datetime = datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
    end_time: datetime = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

    meeting: Meeting = Meeting(
        scheduled_start_time=start_time,
        scheduled_end_time=end_time,
        scheduled_duration_in_minutes=60,
        join_url="https://teams.microsoft.com/meet/123",
        title="Planning Session",
        has_external_invitees=False,
        external_domains=[],
        invitees=[
            Invitee(name="Team Member", email="member@company.com", is_external=False),
        ],
    )

    # Test model_dump with JSON mode
    json_data: dict[str, str | int | bool | list | None] = meeting.model_dump(
        mode="json",
    )
    assert isinstance(json_data["scheduled_start_time"], str)
    assert isinstance(json_data["scheduled_end_time"], str)

    # Test model_dump_json
    json_str: str = meeting.model_dump_json()
    assert "2024-01-15" in json_str
    assert "Planning Session" in json_str

    # Test round-trip serialization
    restored: Meeting = Meeting.model_validate_json(json_str)
    assert restored.title == meeting.title
    assert (
        restored.scheduled_duration_in_minutes == meeting.scheduled_duration_in_minutes
    )


def test_meeting_edge_cases() -> None:
    """Test Meeting edge cases and unusual inputs."""
    from datetime import timezone

    # Test with negative duration (should be allowed by the model)
    meeting_negative: Meeting = Meeting(
        scheduled_start_time=datetime.now(tz=timezone.utc),
        scheduled_end_time=None,
        scheduled_duration_in_minutes=-30,
        join_url="https://example.com",
        title="Negative Duration",
        has_external_invitees=None,
        external_domains=None,
        invitees=None,
    )
    assert meeting_negative.scheduled_duration_in_minutes == -30

    # Test with zero duration
    meeting_zero: Meeting = Meeting(
        scheduled_start_time=datetime.now(tz=timezone.utc),
        scheduled_end_time=None,
        scheduled_duration_in_minutes=0,
        join_url="https://example.com",
        title="Zero Duration",
        has_external_invitees=None,
        external_domains=None,
        invitees=None,
    )
    assert meeting_zero.scheduled_duration_in_minutes == 0

    # Test with very long title
    long_title: str = "A" * 1000
    meeting_long_title: Meeting = Meeting(
        scheduled_start_time=datetime.now(tz=timezone.utc),
        scheduled_end_time=None,
        scheduled_duration_in_minutes=30,
        join_url="https://example.com",
        title=long_title,
        has_external_invitees=None,
        external_domains=None,
        invitees=None,
    )
    assert len(meeting_long_title.title) == 1000

    # Test with empty join_url (should be allowed)
    meeting_empty_url: Meeting = Meeting(
        scheduled_start_time=datetime.now(tz=timezone.utc),
        scheduled_end_time=None,
        scheduled_duration_in_minutes=30,
        join_url="",
        title="Empty URL Meeting",
        has_external_invitees=None,
        external_domains=None,
        invitees=None,
    )
    assert meeting_empty_url.join_url == ""


# trunk-ignore-end(ruff/PLR2004,ruff/S101,pyright/reportArgumentType)
