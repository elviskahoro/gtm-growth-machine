# trunk-ignore-all(trunk/ignore-does-nothing)
from __future__ import annotations

import pytest
from pydantic import BaseModel, Field, ValidationError

from .speaker import Speaker  # trunk-ignore(ruff/TC001)


class Storage(BaseModel):
    speakers_internal: list[Speaker] = Field(
        default_factory=list,
        description="List of speakers with their emails and aliases",
    )


# trunk-ignore-begin(ruff/PLR2004,ruff/S101,pyright/reportArgumentType)
def test_storage_creation_default() -> None:
    """Test creating Storage with default empty speakers list."""
    storage = Storage()

    assert storage.speakers_internal == []
    assert isinstance(storage.speakers_internal, list)


def test_storage_creation_empty_speakers_explicit() -> None:
    """Test creating Storage with explicitly empty speakers list."""
    storage = Storage(speakers_internal=[])

    assert storage.speakers_internal == []


def test_storage_creation_with_single_speaker() -> None:
    """Test creating Storage with a single speaker."""
    speaker = Speaker(
        name="John Doe",
        email="john.doe@example.com",
    )
    storage = Storage(speakers_internal=[speaker])

    assert len(storage.speakers_internal) == 1
    assert storage.speakers_internal[0] == speaker
    assert storage.speakers_internal[0].name == "John Doe"
    assert storage.speakers_internal[0].email == "john.doe@example.com"


def test_storage_creation_with_multiple_speakers() -> None:
    """Test creating Storage with multiple speakers."""
    speakers = [
        Speaker(
            name="Alice Johnson",
            email="alice.johnson@example.com",
            aliases=["Alice J.", "AJ"],
        ),
        Speaker(
            name="Bob Smith",
            email="bob.smith@example.com",
        ),
        Speaker(
            name="Carol Davis",
            email="carol.davis@example.com",
            aliases=["Carol D."],
        ),
    ]
    storage = Storage(speakers_internal=speakers)

    assert len(storage.speakers_internal) == 3
    assert storage.speakers_internal == speakers


def test_storage_speakers_list_mutability() -> None:
    """Test that speakers list can be modified after creation."""
    storage = Storage()
    speaker = Speaker(
        name="Test User",
        email="test@example.com",
    )

    # Add speaker to the list
    storage.speakers_internal.append(speaker)

    assert len(storage.speakers_internal) == 1
    assert storage.speakers_internal[0] == speaker


def test_storage_speakers_list_clear() -> None:
    """Test clearing speakers list after creation."""
    speakers = [
        Speaker(
            name="User1",
            email="user1@example.com",
        ),
        Speaker(
            name="User2",
            email="user2@example.com",
        ),
    ]
    storage = Storage(speakers_internal=speakers)

    assert len(storage.speakers_internal) == 2

    # Clear the list
    storage.speakers_internal.clear()

    assert len(storage.speakers_internal) == 0
    assert storage.speakers_internal == []


def test_storage_speakers_list_extend() -> None:
    """Test extending speakers list with additional speakers."""
    initial_speakers = [
        Speaker(
            name="User1",
            email="user1@example.com",
        ),
    ]
    storage = Storage(speakers_internal=initial_speakers)

    additional_speakers = [
        Speaker(
            name="User2",
            email="user2@example.com",
        ),
        Speaker(
            name="User3",
            email="user3@example.com",
        ),
    ]

    storage.speakers_internal.extend(additional_speakers)

    assert len(storage.speakers_internal) == 3
    assert storage.speakers_internal[0].name == "User1"
    assert storage.speakers_internal[1].name == "User2"
    assert storage.speakers_internal[2].name == "User3"


def test_storage_field_description() -> None:
    """Test that Storage field has the correct description."""
    # Access field info through the model's __fields__ attribute
    field_info = Storage.model_fields["speakers_internal"]

    assert field_info.description == "List of speakers with their emails and aliases"


def test_storage_default_factory() -> None:
    """Test that default factory creates separate list instances."""
    storage1 = Storage()
    storage2 = Storage()

    # Ensure they have separate list instances
    assert storage1.speakers_internal is not storage2.speakers_internal

    # Modify one and ensure the other is not affected
    speaker = Speaker(
        name="Test User",
        email="test@example.com",
    )
    storage1.speakers_internal.append(speaker)

    assert len(storage1.speakers_internal) == 1
    assert len(storage2.speakers_internal) == 0


def test_storage_invalid_speakers_type() -> None:
    """Test that invalid speaker types raise ValidationError."""
    with pytest.raises(ValidationError) as exc_info:
        Storage(
            speakers_internal=["not_a_speaker"],  # type: ignore[list-item]
        )

    assert "speakers_internal" in str(exc_info.value)


def test_storage_mixed_valid_invalid_speakers() -> None:
    """Test validation with mix of valid and invalid speakers."""
    valid_speaker = Speaker(
        name="Valid User",
        email="valid@example.com",
    )

    with pytest.raises(ValidationError) as exc_info:
        Storage(
            speakers_internal=[valid_speaker, "invalid_speaker"],  # type: ignore[list-item]
        )

    assert "speakers_internal" in str(exc_info.value)


def test_storage_none_speakers_list() -> None:
    """Test that None is not accepted for speakers_internal."""
    with pytest.raises(ValidationError) as exc_info:
        Storage(
            speakers_internal=None,  # type: ignore[arg-type]
        )

    assert "speakers_internal" in str(exc_info.value)


def test_storage_duplicate_speakers() -> None:
    """Test Storage with duplicate speakers (should be allowed)."""
    speaker = Speaker(
        name="John Doe",
        email="john.doe@example.com",
    )
    # Create two separate instances with same data
    duplicate_speaker = Speaker(
        name="John Doe",
        email="john.doe@example.com",
    )

    storage = Storage(speakers_internal=[speaker, duplicate_speaker])

    assert len(storage.speakers_internal) == 2
    assert storage.speakers_internal[0] == storage.speakers_internal[1]


def test_storage_speakers_with_complex_aliases() -> None:
    """Test Storage with speakers having complex aliases."""
    speakers = [
        Speaker(
            name="Dr. Jane Smith",
            email="jane.smith@example.com",
            aliases=[
                "Jane Smith, Ph.D.",
                "Dr. J. Smith",
                "Jane S.",
                "JS",
                "Professor Smith",
            ],
        ),
        Speaker(
            name="Robert Johnson Jr.",
            email="robert.johnson@example.com",
            aliases=[
                "Bob Johnson",
                "Bobby J.",
                "R.J. Jr.",
                "Robert Jr.",
            ],
        ),
    ]

    storage = Storage(speakers_internal=speakers)

    assert len(storage.speakers_internal) == 2
    assert len(storage.speakers_internal[0].aliases) == 5
    assert len(storage.speakers_internal[1].aliases) == 4
    assert "Professor Smith" in storage.speakers_internal[0].aliases
    assert "Bobby J." in storage.speakers_internal[1].aliases


def test_storage_empty_speaker_names() -> None:
    """Test that Storage validation handles speakers with edge case data."""
    # This should raise ValidationError due to empty name in Speaker
    with pytest.raises(ValidationError):
        Storage(
            speakers_internal=[
                Speaker(
                    name="",
                    email="empty.name@example.com",
                ),
            ],
        )


def test_storage_model_dump() -> None:
    """Test serialization of Storage model."""
    speakers = [
        Speaker(
            name="Alice",
            email="alice@example.com",
            aliases=["Al"],
        ),
    ]
    storage = Storage(speakers_internal=speakers)

    dumped = storage.model_dump()

    expected = {
        "speakers_internal": [
            {
                "name": "Alice",
                "email": "alice@example.com",
                "aliases": ["Al"],
            },
        ],
    }

    assert dumped == expected


def test_storage_model_rebuild_from_dict() -> None:
    """Test creating Storage from dictionary data."""
    data = {
        "speakers_internal": [
            {
                "name": "Bob Wilson",
                "email": "bob.wilson@example.com",
                "aliases": ["Bobby", "B.W."],
            },
        ],
    }

    storage = Storage.model_validate(data)

    assert len(storage.speakers_internal) == 1
    assert storage.speakers_internal[0].name == "Bob Wilson"
    assert storage.speakers_internal[0].email == "bob.wilson@example.com"
    assert storage.speakers_internal[0].aliases == ["Bobby", "B.W."]


def test_storage_equality() -> None:
    """Test equality comparison between Storage instances."""
    speaker1 = Speaker(
        name="John Doe",
        email="john.doe@example.com",
    )
    speaker2 = Speaker(
        name="John Doe",
        email="john.doe@example.com",
    )

    storage1 = Storage(speakers_internal=[speaker1])
    storage2 = Storage(speakers_internal=[speaker2])

    assert storage1 == storage2


def test_storage_inequality() -> None:
    """Test inequality comparison between Storage instances."""
    speaker1 = Speaker(
        name="John Doe",
        email="john.doe@example.com",
    )
    speaker2 = Speaker(
        name="Jane Smith",
        email="jane.smith@example.com",
    )

    storage1 = Storage(speakers_internal=[speaker1])
    storage2 = Storage(speakers_internal=[speaker2])

    assert storage1 != storage2


def test_storage_repr() -> None:
    """Test string representation of Storage."""
    speakers = [
        Speaker(
            name="Alice",
            email="alice@example.com",
        ),
    ]
    storage = Storage(speakers_internal=speakers)

    repr_str = repr(storage)

    assert "Storage" in repr_str
    assert "speakers_internal" in repr_str


# trunk-ignore-end(ruff/PLR2004,ruff/S101,pyright/reportArgumentType)
