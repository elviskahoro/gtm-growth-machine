from __future__ import annotations

import pytest
from pydantic import BaseModel, EmailStr, Field, ValidationError, field_validator


class Speaker(BaseModel):
    name: str = Field(
        ...,
        description="Name of the speaker",
        min_length=1,
    )
    email: EmailStr = Field(
        ...,
        description="Email address of the entity",
    )
    aliases: list[str] = Field(
        default_factory=list,
        description="List of alternative names or aliases",
    )

    @field_validator("name")
    @classmethod
    def name_must_not_be_empty(
        cls: type[Speaker],
        v: str,
    ) -> str:
        if not v or not v.strip():
            msg = "Name cannot be empty or whitespace only"
            raise ValueError(msg)
        return v

    @staticmethod
    def build_speaker_lookup_map(
        speakers: list[Speaker],
    ) -> dict[str, str]:
        lookup_map: dict[str, str] = {}
        for speaker in speakers:
            lookup_map[speaker.name.lower()] = speaker.email
            for alias in speaker.aliases:
                lookup_map[alias.lower()] = speaker.email

        return lookup_map

    @staticmethod
    def get_email_by_name_with_lookup(
        lookup_map: dict[str, str],
        search_name: str,
    ) -> str:
        return lookup_map.get(search_name.lower(), search_name)


# trunk-ignore-begin(ruff/S101,pyright/reportArgumentType,pyright/reportCallIssue)
def test_speaker_creation_with_required_fields() -> None:
    """Test creating a Speaker with only required fields."""
    speaker: Speaker = Speaker(
        name="John Doe",
        email="john.doe@example.com",
    )

    assert speaker.name == "John Doe"
    assert speaker.email == "john.doe@example.com"
    assert speaker.aliases == []


def test_speaker_creation_with_all_fields() -> None:
    """Test creating a Speaker with all fields including aliases."""
    speaker: Speaker = Speaker(
        name="Jane Smith",
        email="jane.smith@example.com",
        aliases=["J. Smith", "Jane S.", "JS"],
    )

    assert speaker.name == "Jane Smith"
    assert speaker.email == "jane.smith@example.com"
    assert speaker.aliases == ["J. Smith", "Jane S.", "JS"]


def test_speaker_creation_with_empty_aliases_list() -> None:
    """Test creating a Speaker with explicitly empty aliases list."""
    speaker: Speaker = Speaker(
        name="Bob Wilson",
        email="bob.wilson@example.com",
        aliases=[],
    )

    assert speaker.name == "Bob Wilson"
    assert speaker.email == "bob.wilson@example.com"
    assert speaker.aliases == []


def test_speaker_validation_missing_name() -> None:
    """Test that creating a Speaker without name raises ValidationError."""
    with pytest.raises(ValidationError) as exc_info:
        Speaker(
            email="test@example.com",
        )

    assert "name" in str(exc_info.value)
    assert "Field required" in str(exc_info.value)


def test_speaker_validation_missing_email() -> None:
    """Test that creating a Speaker without email raises ValidationError."""
    with pytest.raises(ValidationError) as exc_info:
        Speaker(
            name="Test User",
        )

    assert "email" in str(exc_info.value)
    assert "Field required" in str(exc_info.value)


def test_speaker_validation_invalid_email() -> None:
    """Test that creating a Speaker with invalid email raises ValidationError."""
    with pytest.raises(ValidationError) as exc_info:
        Speaker(
            name="Test User",
            email="invalid-email",
        )

    assert "email" in str(exc_info.value)


def test_speaker_validation_empty_name() -> None:
    """Test that creating a Speaker with empty name raises ValidationError."""
    with pytest.raises(ValidationError) as exc_info:
        Speaker(
            name="",
            email="test@example.com",
        )

    assert "name" in str(exc_info.value)


def test_build_speaker_lookup_map_empty_list() -> None:
    """Test building lookup map with empty speaker list."""
    speakers: list[Speaker] = []
    lookup_map: dict[str, str] = Speaker.build_speaker_lookup_map(speakers)

    assert lookup_map == {}


def test_build_speaker_lookup_map_single_speaker_no_aliases() -> None:
    """Test building lookup map with single speaker without aliases."""
    speakers: list[Speaker] = [
        Speaker(
            name="John Doe",
            email="john.doe@example.com",
        ),
    ]

    lookup_map: dict[str, str] = Speaker.build_speaker_lookup_map(speakers)

    expected: dict[str, str] = {"john doe": "john.doe@example.com"}
    assert lookup_map == expected


def test_build_speaker_lookup_map_single_speaker_with_aliases() -> None:
    """Test building lookup map with single speaker with aliases."""
    speakers: list[Speaker] = [
        Speaker(
            name="Jane Smith",
            email="jane.smith@example.com",
            aliases=["J. Smith", "Jane S."],
        ),
    ]

    lookup_map: dict[str, str] = Speaker.build_speaker_lookup_map(speakers)

    expected: dict[str, str] = {
        "jane smith": "jane.smith@example.com",
        "j. smith": "jane.smith@example.com",
        "jane s.": "jane.smith@example.com",
    }
    assert lookup_map == expected


def test_build_speaker_lookup_map_multiple_speakers() -> None:
    """Test building lookup map with multiple speakers."""
    speakers: list[Speaker] = [
        Speaker(
            name="John Doe",
            email="john.doe@example.com",
            aliases=["Johnny"],
        ),
        Speaker(
            name="Jane Smith",
            email="jane.smith@example.com",
            aliases=["J. Smith", "Jane S."],
        ),
        Speaker(
            name="Bob Wilson",
            email="bob.wilson@example.com",
        ),
    ]

    lookup_map: dict[str, str] = Speaker.build_speaker_lookup_map(speakers)

    expected: dict[str, str] = {
        "john doe": "john.doe@example.com",
        "johnny": "john.doe@example.com",
        "jane smith": "jane.smith@example.com",
        "j. smith": "jane.smith@example.com",
        "jane s.": "jane.smith@example.com",
        "bob wilson": "bob.wilson@example.com",
    }
    assert lookup_map == expected


def test_build_speaker_lookup_map_case_insensitive() -> None:
    """Test that lookup map keys are case-insensitive (lowercased)."""
    speakers: list[Speaker] = [
        Speaker(
            name="JOHN DOE",
            email="john.doe@example.com",
            aliases=["JOHNNY", "John D."],
        ),
    ]

    lookup_map: dict[str, str] = Speaker.build_speaker_lookup_map(speakers)

    expected: dict[str, str] = {
        "john doe": "john.doe@example.com",
        "johnny": "john.doe@example.com",
        "john d.": "john.doe@example.com",
    }
    assert lookup_map == expected


def test_build_speaker_lookup_map_duplicate_names() -> None:
    """Test building lookup map with duplicate names (later speaker overwrites)."""
    speakers: list[Speaker] = [
        Speaker(
            name="John Smith",
            email="john.smith1@example.com",
        ),
        Speaker(
            name="John Smith",
            email="john.smith2@example.com",
        ),
    ]

    lookup_map: dict[str, str] = Speaker.build_speaker_lookup_map(speakers)

    # The second speaker should overwrite the first
    expected: dict[str, str] = {"john smith": "john.smith2@example.com"}
    assert lookup_map == expected


def test_get_email_by_name_with_lookup_found() -> None:
    """Test getting email by name when name is found in lookup map."""
    lookup_map: dict[str, str] = {
        "john doe": "john.doe@example.com",
        "jane smith": "jane.smith@example.com",
    }

    result: str = Speaker.get_email_by_name_with_lookup(
        lookup_map,
        "John Doe",
    )

    assert result == "john.doe@example.com"


def test_get_email_by_name_with_lookup_not_found() -> None:
    """Test getting email by name when name is not found in lookup map."""
    lookup_map: dict[str, str] = {
        "john doe": "john.doe@example.com",
        "jane smith": "jane.smith@example.com",
    }

    result: str = Speaker.get_email_by_name_with_lookup(
        lookup_map,
        "Unknown Person",
    )

    assert result == "Unknown Person"


def test_get_email_by_name_with_lookup_case_insensitive() -> None:
    """Test that name lookup is case-insensitive."""
    lookup_map: dict[str, str] = {
        "john doe": "john.doe@example.com",
    }

    test_cases: list[str] = [
        "John Doe",
        "JOHN DOE",
        "john doe",
        "JoHn DoE",
    ]

    for search_name in test_cases:
        result: str = Speaker.get_email_by_name_with_lookup(
            lookup_map,
            search_name,
        )
        assert result == "john.doe@example.com", f"Failed for: {search_name}"


def test_get_email_by_name_with_lookup_empty_map() -> None:
    """Test getting email by name with empty lookup map."""
    lookup_map: dict[str, str] = {}

    result: str = Speaker.get_email_by_name_with_lookup(
        lookup_map,
        "Any Name",
    )

    assert result == "Any Name"


def test_get_email_by_name_with_lookup_empty_search_name() -> None:
    """Test getting email with empty search name."""
    lookup_map: dict[str, str] = {
        "john doe": "john.doe@example.com",
    }

    result: str = Speaker.get_email_by_name_with_lookup(
        lookup_map,
        "",
    )

    assert result == ""


def test_get_email_by_name_with_lookup_whitespace_handling() -> None:
    """Test that whitespace in search names is handled properly."""
    lookup_map: dict[str, str] = {
        "john doe": "john.doe@example.com",
    }

    # Test with extra whitespace - should not match
    result: str = Speaker.get_email_by_name_with_lookup(
        lookup_map,
        "  John Doe  ",
    )

    assert result == "  John Doe  "  # Returns as-is since no match


def test_integration_build_and_lookup() -> None:
    """Integration test: build lookup map and use it for lookups."""
    speakers: list[Speaker] = [
        Speaker(
            name="Alice Johnson",
            email="alice.johnson@example.com",
            aliases=["Alice J.", "AJ"],
        ),
        Speaker(
            name="Bob Brown",
            email="bob.brown@example.com",
        ),
    ]

    lookup_map: dict[str, str] = Speaker.build_speaker_lookup_map(speakers)

    # Test name lookup
    assert (
        Speaker.get_email_by_name_with_lookup(
            lookup_map,
            "Alice Johnson",
        )
        == "alice.johnson@example.com"
    )

    # Test alias lookup
    assert (
        Speaker.get_email_by_name_with_lookup(
            lookup_map,
            "Alice J.",
        )
        == "alice.johnson@example.com"
    )

    assert (
        Speaker.get_email_by_name_with_lookup(
            lookup_map,
            "AJ",
        )
        == "alice.johnson@example.com"
    )

    # Test name not found
    assert (
        Speaker.get_email_by_name_with_lookup(
            lookup_map,
            "Charlie Davis",
        )
        == "Charlie Davis"
    )


def test_speaker_aliases_type_validation() -> None:
    """Test that aliases field accepts list of strings."""
    speaker: Speaker = Speaker(
        name="Test User",
        email="test@example.com",
        aliases=["alias1", "alias2", "alias3"],
    )

    assert isinstance(speaker.aliases, list)
    assert all(isinstance(alias, str) for alias in speaker.aliases)


def test_speaker_model_immutability() -> None:
    """Test that Speaker model behaves correctly with field access."""
    speaker: Speaker = Speaker(
        name="John Doe",
        email="john.doe@example.com",
        aliases=["Johnny"],
    )

    # Test field access
    assert hasattr(speaker, "name")
    assert hasattr(speaker, "email")
    assert hasattr(speaker, "aliases")

    # Test that we can modify the aliases list (it's mutable)
    speaker.aliases.append("JD")
    assert "JD" in speaker.aliases


# trunk-ignore-end(ruff/S101,pyright/reportArgumentType,pyright/reportCallIssue)
