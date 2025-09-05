from pydantic import BaseModel


class FathomUser(BaseModel):
    name: str
    email: str
    team: str


# trunk-ignore-begin(ruff/PLR2004,ruff/S101,pyright/reportArgumentType)
def test_fathom_user_creation() -> None:
    """Test basic FathomUser creation with valid data."""
    user: FathomUser = FathomUser(
        name="John Doe",
        email="john.doe@example.com",
        team="Engineering",
    )

    assert user.name == "John Doe"
    assert user.email == "john.doe@example.com"
    assert user.team == "Engineering"


def test_fathom_user_validation_missing_fields() -> None:
    """Test that FathomUser raises validation error when required fields are missing."""
    import pytest
    from pydantic import ValidationError

    # Missing all fields
    with pytest.raises(ValidationError) as exc_info:
        FathomUser()  # type: ignore[call-arg]  # Intentionally missing required args

    errors = exc_info.value.errors()
    error_fields: set[str] = {str(error["loc"][0]) for error in errors}
    assert error_fields == {"name", "email", "team"}


def test_fathom_user_validation_empty_strings() -> None:
    """Test FathomUser with empty string values."""
    user: FathomUser = FathomUser(
        name="",
        email="",
        team="",
    )

    # Pydantic allows empty strings by default
    assert user.name == ""
    assert user.email == ""
    assert user.team == ""


def test_fathom_user_validation_wrong_types() -> None:
    """Test that FathomUser raises validation error for wrong field types."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as exc_info:
        FathomUser(
            name=123,  # Should be string
            email=["not", "a", "string"],  # Should be string
            team=None,  # Should be string
        )

    errors = exc_info.value.errors()
    assert len(errors) == 3

    error_fields: set[str] = {str(error["loc"][0]) for error in errors}
    assert error_fields == {"name", "email", "team"}


def test_fathom_user_serialization() -> None:
    """Test FathomUser serialization to dict and JSON."""
    user: FathomUser = FathomUser(
        name="Jane Smith",
        email="jane.smith@company.com",
        team="Product",
    )

    # Test dict serialization
    user_dict: dict[str, str] = user.model_dump()
    expected_dict: dict[str, str] = {
        "name": "Jane Smith",
        "email": "jane.smith@company.com",
        "team": "Product",
    }
    assert user_dict == expected_dict

    # Test JSON serialization
    user_json: str = user.model_dump_json()
    import json

    parsed_json: dict[str, str] = json.loads(user_json)
    assert parsed_json == expected_dict


def test_fathom_user_from_dict() -> None:
    """Test creating FathomUser from dictionary."""
    user_data: dict[str, str] = {
        "name": "Bob Wilson",
        "email": "bob.wilson@test.com",
        "team": "Marketing",
    }

    user: FathomUser = FathomUser(**user_data)

    assert user.name == "Bob Wilson"
    assert user.email == "bob.wilson@test.com"
    assert user.team == "Marketing"


def test_fathom_user_equality() -> None:
    """Test FathomUser equality comparison."""
    user1: FathomUser = FathomUser(
        name="Alice Johnson",
        email="alice@example.com",
        team="Design",
    )

    user2: FathomUser = FathomUser(
        name="Alice Johnson",
        email="alice@example.com",
        team="Design",
    )

    user3: FathomUser = FathomUser(
        name="Alice Johnson",
        email="alice@example.com",
        team="Engineering",  # Different team
    )

    assert user1 == user2
    assert user1 != user3


def test_fathom_user_special_characters() -> None:
    """Test FathomUser with special characters and unicode."""
    user: FathomUser = FathomUser(
        name="José María O'Connor",
        email="jose.maria@tëst.com",
        team="Iñtërnâtiønàl",
    )

    assert user.name == "José María O'Connor"
    assert user.email == "jose.maria@tëst.com"
    assert user.team == "Iñtërnâtiønàl"


def test_fathom_user_extra_fields_ignored() -> None:
    """Test that FathomUser ignores extra fields by default."""
    user_data: dict[str, str] = {
        "name": "Test User",
        "email": "test@example.com",
        "team": "Testing",
        "extra_field": "should be ignored",
        "another_extra": "also ignored",
    }

    user: FathomUser = FathomUser(**user_data)

    assert user.name == "Test User"
    assert user.email == "test@example.com"
    assert user.team == "Testing"
    assert not hasattr(user, "extra_field")
    assert not hasattr(user, "another_extra")


# trunk-ignore-end(ruff/PLR2004,ruff/S101,pyright/reportArgumentType)
