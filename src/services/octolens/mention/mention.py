# trunk-ignore-all(ruff/ANN401)
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import AliasChoices, BaseModel, Field, field_validator

from src.services.local.filesystem import FileUtility


class Mention(BaseModel):
    url: str = Field(
        validation_alias=AliasChoices(
            "url",
            "URL",
        ),
    )
    title: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "title",
            "Title",
        ),
    )
    body: str = Field(
        validation_alias=AliasChoices(
            "body",
            "Body",
        ),
    )
    timestamp: datetime = Field(
        validation_alias=AliasChoices(
            "timestamp",
            "Timestamp",
        ),
    )
    image_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "image_url",
            "Image URL",
            "imageUrl",
        ),
    )
    source: str = Field(
        validation_alias=AliasChoices(
            "source",
            "Source",
        ),
    )
    source_id: str = Field(
        validation_alias=AliasChoices(
            "source_id",
            "Source ID",
            "sourceId",
        ),
    )
    author: str = Field(
        validation_alias=AliasChoices(
            "author",
            "Author",
        ),
    )
    author_avatar_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "author_avatar_url",
            "Author Avatar URL",
            "authorAvatarUrl",
        ),
    )
    author_profile_link: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "author_profile_link",
            "Author Profile Link",
            "authorProfileLink",
        ),
    )
    relevance_score: str = Field(
        validation_alias=AliasChoices(
            "relevance_score",
            "Relevance Score",
            "relevanceScore",
        ),
    )
    relevance_comment: str = Field(
        validation_alias=AliasChoices(
            "relevance_comment",
            "Relevance Comment",
            "relevanceComment",
        ),
    )
    language: str = Field(
        validation_alias=AliasChoices(
            "language",
            "Language",
        ),
    )
    keyword: str = Field(
        validation_alias=AliasChoices(
            "keyword",
            "Keyword",
        ),
    )
    bookmarked: bool = False

    @field_validator(
        "timestamp",
        mode="before",
    )
    @classmethod
    def parse_timestamp(
        cls: type[Mention],
        value: Any,
    ) -> datetime:
        if not isinstance(value, str):
            error_msg: str = f"Invalid timestamp format: {value}"
            raise TypeError(error_msg)

        # Try original format first
        try:
            return datetime.strptime(value, "%a %b %d %Y %H:%M:%S GMT%z")

        except ValueError:
            # Try ISO 8601 format
            try:
                return datetime.fromisoformat(value)

            except ValueError as e:
                error_msg: str = f"Invalid timestamp format: {value}"
                raise ValueError(error_msg) from e

    def get_file_name(
        self: Mention,
        extension: str = ".jsonl",
    ) -> str:
        source: str = FileUtility.file_clean_string(self.source)
        keyword: str = FileUtility.file_clean_string(self.keyword)
        author: str = FileUtility.file_clean_string(self.author)
        timestamp: str = FileUtility.file_clean_timestamp_from_datetime(self.timestamp)
        return f"{source}-{keyword}-{timestamp}-{author}{extension}"


# trunk-ignore-begin(ruff/PLR2004,ruff/S101,pyright/reportArgumentType,ruff/PLC0415)
def test_mention_initialization_with_required_fields() -> None:
    """Test Mention model initialization with all required fields."""
    mention_data: dict[str, str | bool] = {
        "url": "https://example.com/post/123",
        "body": "This is a test mention body content",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "twitter",
        "source_id": "tweet_123456",
        "author": "test_user",
        "relevance_score": "high",
        "relevance_comment": "Highly relevant to our product",
        "language": "en",
        "keyword": "test keyword",
    }

    mention: Mention = Mention(**mention_data)

    assert mention.url == "https://example.com/post/123"
    assert mention.body == "This is a test mention body content"
    assert mention.source == "twitter"
    assert mention.source_id == "tweet_123456"
    assert mention.author == "test_user"
    assert mention.relevance_score == "high"
    assert mention.relevance_comment == "Highly relevant to our product"
    assert mention.language == "en"
    assert mention.keyword == "test keyword"
    assert mention.bookmarked is False

    # Test optional fields are None by default
    assert mention.title is None
    assert mention.image_url is None
    assert mention.author_avatar_url is None
    assert mention.author_profile_link is None


def test_mention_initialization_with_all_fields() -> None:
    """Test Mention model initialization with all fields including optional ones."""
    mention_data: dict[str, str | bool] = {
        "url": "https://example.com/post/456",
        "title": "Complete Test Post",
        "body": "Full test content with all fields",
        "timestamp": "2024-02-20T14:45:30+00:00",
        "image_url": "https://example.com/image.jpg",
        "source": "reddit",
        "source_id": "post_789012",
        "author": "complete_user",
        "author_avatar_url": "https://example.com/avatar.jpg",
        "author_profile_link": "https://example.com/profile/complete_user",
        "relevance_score": "medium",
        "relevance_comment": "Moderately relevant content",
        "language": "en",
        "keyword": "complete test",
        "bookmarked": True,
    }

    mention: Mention = Mention(**mention_data)

    assert mention.title == "Complete Test Post"
    assert mention.image_url == "https://example.com/image.jpg"
    assert mention.author_avatar_url == "https://example.com/avatar.jpg"
    assert mention.author_profile_link == "https://example.com/profile/complete_user"
    assert mention.bookmarked is True


def test_mention_field_alias_choices() -> None:
    """Test that field aliases work correctly for different input formats."""
    # Test URL alias
    mention_data_url: dict[str, str] = {
        "URL": "https://test.com",  # Uppercase alias
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "test",
        "source_id": "123",
        "author": "user",
        "relevance_score": "high",
        "relevance_comment": "test",
        "language": "en",
        "keyword": "test",
    }
    mention_url: Mention = Mention(**mention_data_url)
    assert mention_url.url == "https://test.com"

    # Test title alias
    mention_data_title: dict[str, str] = {
        "url": "https://test.com",
        "Title": "Test Title",  # Capitalized alias
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "test",
        "source_id": "123",
        "author": "user",
        "relevance_score": "high",
        "relevance_comment": "test",
        "language": "en",
        "keyword": "test",
    }
    mention_title: Mention = Mention(**mention_data_title)
    assert mention_title.title == "Test Title"

    # Test camelCase aliases
    mention_data_camel: dict[str, str] = {
        "url": "https://test.com",
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "test",
        "sourceId": "123",  # camelCase alias
        "author": "user",
        "authorAvatarUrl": "https://avatar.com",  # camelCase alias
        "authorProfileLink": "https://profile.com",  # camelCase alias
        "relevanceScore": "high",  # camelCase alias
        "relevanceComment": "test",  # camelCase alias
        "language": "en",
        "keyword": "test",
    }
    mention_camel: Mention = Mention(**mention_data_camel)
    assert mention_camel.source_id == "123"
    assert mention_camel.author_avatar_url == "https://avatar.com"
    assert mention_camel.author_profile_link == "https://profile.com"
    assert mention_camel.relevance_score == "high"
    assert mention_camel.relevance_comment == "test"


def test_timestamp_validator_original_format() -> None:
    """Test timestamp validator with original GMT format."""
    mention_data: dict[str, str] = {
        "url": "https://test.com",
        "body": "Test body",
        "timestamp": "Mon Jan 15 2024 10:30:00 GMT+0000",
        "source": "test",
        "source_id": "123",
        "author": "user",
        "relevance_score": "high",
        "relevance_comment": "test",
        "language": "en",
        "keyword": "test",
    }

    mention: Mention = Mention(**mention_data)

    from datetime import timezone

    assert mention.timestamp.year == 2024
    assert mention.timestamp.month == 1
    assert mention.timestamp.day == 15
    assert mention.timestamp.hour == 10
    assert mention.timestamp.minute == 30
    assert mention.timestamp.second == 0
    assert mention.timestamp.tzinfo == timezone.utc


def test_timestamp_validator_iso_format() -> None:
    """Test timestamp validator with ISO 8601 format."""
    mention_data: dict[str, str] = {
        "url": "https://test.com",
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "test",
        "source_id": "123",
        "author": "user",
        "relevance_score": "high",
        "relevance_comment": "test",
        "language": "en",
        "keyword": "test",
    }

    mention: Mention = Mention(**mention_data)

    from datetime import timezone

    assert mention.timestamp.year == 2024
    assert mention.timestamp.month == 1
    assert mention.timestamp.day == 15
    assert mention.timestamp.hour == 10
    assert mention.timestamp.minute == 30
    assert mention.timestamp.second == 0
    assert mention.timestamp.tzinfo == timezone.utc


def test_timestamp_validator_invalid_type() -> None:
    """Test timestamp validator raises TypeError for non-string input."""
    import pytest

    mention_data: dict[str, str | int] = {
        "url": "https://test.com",
        "body": "Test body",
        "timestamp": 1705315800,  # Unix timestamp as int
        "source": "test",
        "source_id": "123",
        "author": "user",
        "relevance_score": "high",
        "relevance_comment": "test",
        "language": "en",
        "keyword": "test",
    }

    with pytest.raises(TypeError, match="Invalid timestamp format: 1705315800"):
        Mention(**mention_data)


def test_timestamp_validator_invalid_format() -> None:
    """Test timestamp validator raises ValueError for invalid string format."""
    import pytest
    from pydantic import ValidationError

    mention_data: dict[str, str] = {
        "url": "https://test.com",
        "body": "Test body",
        "timestamp": "invalid-timestamp-format",
        "source": "test",
        "source_id": "123",
        "author": "user",
        "relevance_score": "high",
        "relevance_comment": "test",
        "language": "en",
        "keyword": "test",
    }

    with pytest.raises(ValidationError):
        Mention(**mention_data)


def test_get_file_name_default_extension() -> None:
    """Test get_file_name method with default .jsonl extension."""
    from unittest.mock import patch

    mention_data: dict[str, str] = {
        "url": "https://example.com/post",
        "body": "Test body for filename",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "GitHub Issues",
        "source_id": "issue_456",
        "author": "Test User Name",
        "relevance_score": "high",
        "relevance_comment": "Very relevant",
        "language": "en",
        "keyword": "Bug Report",
    }

    mention: Mention = Mention(**mention_data)

    # Mock the FileUtility methods to control their return values
    with patch(
        "src.services.local.filesystem.FileUtility.file_clean_string",
    ) as mock_clean_string, patch(
        "src.services.local.filesystem.FileUtility.file_clean_timestamp_from_datetime",
    ) as mock_clean_timestamp:
        mock_clean_string.side_effect = lambda x: x.lower().replace(" ", "_")  # type: ignore[misc]
        mock_clean_timestamp.return_value = "2024_01_15_10_30_00"

        filename: str = mention.get_file_name()

        assert (
            filename
            == "github_issues-bug_report-2024_01_15_10_30_00-test_user_name.jsonl"
        )

        # Verify the utility methods were called correctly
        assert mock_clean_string.call_count == 3
        mock_clean_timestamp.assert_called_once()


def test_get_file_name_custom_extension() -> None:
    """Test get_file_name method with custom extension."""
    from unittest.mock import patch

    mention_data: dict[str, str] = {
        "url": "https://example.com/post",
        "body": "Test body",
        "timestamp": "2024-02-20T14:45:30+00:00",
        "source": "Twitter",
        "source_id": "tweet_789",
        "author": "social_user",
        "relevance_score": "medium",
        "relevance_comment": "Social content",
        "language": "en",
        "keyword": "social media",
    }

    mention: Mention = Mention(**mention_data)

    # Mock the FileUtility methods
    with patch(
        "src.services.local.filesystem.FileUtility.file_clean_string",
    ) as mock_clean_string, patch(
        "src.services.local.filesystem.FileUtility.file_clean_timestamp_from_datetime",
    ) as mock_clean_timestamp:
        mock_clean_string.side_effect = lambda x: x.lower().replace(" ", "_")  # type: ignore[misc]
        mock_clean_timestamp.return_value = "2024_02_20_14_45_30"

        filename: str = mention.get_file_name(extension=".json")

        assert filename == "twitter-social_media-2024_02_20_14_45_30-social_user.json"
        assert filename.endswith(".json")


def test_mention_model_validation_missing_required_fields() -> None:
    """Test Mention model validation with missing required fields."""
    import pytest
    from pydantic import ValidationError

    # Missing url field
    incomplete_data: dict[str, str] = {
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "test",
        "source_id": "123",
        "author": "user",
        "relevance_score": "high",
        "relevance_comment": "test",
        "language": "en",
        "keyword": "test",
    }

    with pytest.raises(ValidationError):
        Mention(**incomplete_data)


def test_mention_model_validation_empty_strings() -> None:
    """Test Mention model validation with empty string values."""
    mention_data: dict[str, str] = {
        "url": "",  # Empty string should still be valid
        "body": "",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "",
        "source_id": "",
        "author": "",
        "relevance_score": "",
        "relevance_comment": "",
        "language": "",
        "keyword": "",
    }

    # This should not raise an exception - empty strings are valid
    mention: Mention = Mention(**mention_data)
    assert mention.url == ""
    assert mention.body == ""
    assert mention.source == ""


def test_mention_bookmarked_field_default() -> None:
    """Test that bookmarked field defaults to False."""
    mention_data: dict[str, str] = {
        "url": "https://test.com",
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "test",
        "source_id": "123",
        "author": "user",
        "relevance_score": "high",
        "relevance_comment": "test",
        "language": "en",
        "keyword": "test",
    }

    mention: Mention = Mention(**mention_data)
    assert mention.bookmarked is False

    # Test explicit True value
    mention_data["bookmarked"] = True  # type: ignore[assignment]
    mention_bookmarked: Mention = Mention(**mention_data)
    assert mention_bookmarked.bookmarked is True


def test_mention_optional_fields_none() -> None:
    """Test that optional fields can be None and are handled properly."""
    mention_data: dict[str, str | None] = {
        "url": "https://test.com",
        "title": None,  # Explicit None
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "image_url": None,  # Explicit None
        "source": "test",
        "source_id": "123",
        "author": "user",
        "author_avatar_url": None,  # Explicit None
        "author_profile_link": None,  # Explicit None
        "relevance_score": "high",
        "relevance_comment": "test",
        "language": "en",
        "keyword": "test",
    }

    mention: Mention = Mention(**mention_data)
    assert mention.title is None
    assert mention.image_url is None
    assert mention.author_avatar_url is None
    assert mention.author_profile_link is None


def test_mention_field_types() -> None:
    """Test that all fields have correct types after initialization."""
    mention_data: dict[str, str | bool] = {
        "url": "https://example.com/test",
        "title": "Test Title",
        "body": "Test body content",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "image_url": "https://example.com/image.jpg",
        "source": "test_source",
        "source_id": "test_123",
        "author": "test_author",
        "author_avatar_url": "https://example.com/avatar.jpg",
        "author_profile_link": "https://example.com/profile",
        "relevance_score": "high",
        "relevance_comment": "Test relevance",
        "language": "en",
        "keyword": "test_keyword",
        "bookmarked": True,
    }

    mention: Mention = Mention(**mention_data)

    from datetime import datetime

    # Check required field types
    assert isinstance(mention.url, str)
    assert isinstance(mention.body, str)
    assert isinstance(mention.timestamp, datetime)
    assert isinstance(mention.source, str)
    assert isinstance(mention.source_id, str)
    assert isinstance(mention.author, str)
    assert isinstance(mention.relevance_score, str)
    assert isinstance(mention.relevance_comment, str)
    assert isinstance(mention.language, str)
    assert isinstance(mention.keyword, str)
    assert isinstance(mention.bookmarked, bool)

    # Check optional field types (when present)
    assert isinstance(mention.title, str)
    assert isinstance(mention.image_url, str)
    assert isinstance(mention.author_avatar_url, str)
    assert isinstance(mention.author_profile_link, str)


def test_mention_model_fields() -> None:
    """Test that Mention model has all expected fields."""
    fields: list[str] = list(Mention.model_fields.keys())

    expected_fields: list[str] = [
        "url",
        "title",
        "body",
        "timestamp",
        "image_url",
        "source",
        "source_id",
        "author",
        "author_avatar_url",
        "author_profile_link",
        "relevance_score",
        "relevance_comment",
        "language",
        "keyword",
        "bookmarked",
    ]

    for field in expected_fields:
        assert field in fields, f"Expected field '{field}' not found in model fields"

    assert len(fields) == len(expected_fields)


def test_mention_pydantic_serialization() -> None:
    """Test that Mention can be properly serialized and deserialized."""
    import json

    mention_data: dict[str, str | bool] = {
        "url": "https://serialize.test",
        "title": "Serialization Test",
        "body": "Test serialization content",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "serialize_source",
        "source_id": "serialize_123",
        "author": "serialize_author",
        "relevance_score": "high",
        "relevance_comment": "Serialization test",
        "language": "en",
        "keyword": "serialize",
        "bookmarked": True,
    }

    mention: Mention = Mention(**mention_data)

    # Test model_dump
    mention_dict: dict[str, str | bool] = mention.model_dump()  # type: ignore[assignment]
    assert mention_dict["url"] == "https://serialize.test"
    assert mention_dict["bookmarked"] is True

    # Test model_dump_json
    mention_json: str = mention.model_dump_json()
    assert isinstance(mention_json, str)

    # Verify it's valid JSON
    parsed_json: dict[str, str | bool] = json.loads(mention_json)  # type: ignore[assignment]
    assert parsed_json["url"] == "https://serialize.test"
    assert parsed_json["title"] == "Serialization Test"


def test_timestamp_edge_cases() -> None:
    """Test timestamp validator with various edge cases."""
    import pytest
    from pydantic import ValidationError

    base_data: dict[str, str] = {
        "url": "https://test.com",
        "body": "Test body",
        "source": "test",
        "source_id": "123",
        "author": "user",
        "relevance_score": "high",
        "relevance_comment": "test",
        "language": "en",
        "keyword": "test",
    }

    # Test empty string timestamp
    with pytest.raises(ValidationError):
        Mention(**{**base_data, "timestamp": ""})

    # Test whitespace-only timestamp
    with pytest.raises(ValidationError):
        Mention(**{**base_data, "timestamp": "   "})

    # Test partially valid timestamp (ISO format accepts date-only)
    # This should actually work because datetime.fromisoformat can parse date-only
    # So let's test with a clearly invalid format instead
    with pytest.raises(ValidationError):
        Mention(**{**base_data, "timestamp": "not-a-date"})


# trunk-ignore-end(ruff/PLR2004,ruff/S101,pyright/reportArgumentType,ruff/PLC0415)
