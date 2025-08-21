from __future__ import annotations

from pydantic import BaseModel

from src.services.octolens.mention import Mention


class Webhook(BaseModel):
    action: str = "mention_created"
    data: Mention

    @staticmethod
    def modal_get_secret_collection_names() -> list[str]:
        return [
            "devx-growth-gcp",
        ]

    @staticmethod
    def etl_get_bucket_name() -> str:
        return "chalk-ai-devx-octolens-mentions-etl"

    @staticmethod
    def storage_get_app_name() -> None:
        error: str = "Storage app name is not defined for Webhook."
        raise NotImplementedError(error)

    @staticmethod
    def storage_get_base_model_type() -> None:
        return None

    @staticmethod
    def etl_expects_storage_file() -> bool:
        return False

    def etl_is_valid_webhook(
        self: Webhook,
    ) -> bool:
        match self.action:
            case "mention_created":
                return True

            case _:
                return False

    def etl_get_invalid_webhook_error_msg(
        self: Webhook,
    ) -> str:
        return "Invalid webhook: " + self.action

    def etl_get_json(
        self: Webhook,
        storage: None,
    ) -> str:
        del storage
        return self.data.model_dump_json(
            indent=None,
        )

    def etl_get_file_name(
        self: Webhook,
        extension: str = ".jsonl",
    ) -> str:
        return self.data.get_file_name(
            extension=extension,
        )

    def etl_get_base_models(
        self: Webhook,
        storage: None,
    ) -> None:
        del storage
        error: str = "Webhook does not support getting base models."
        raise NotImplementedError(error)


# trunk-ignore-begin(ruff/PLR2004,ruff/S101,pyright/reportArgumentType,ruff/PLC0415)
def test_webhook_initialization() -> None:
    """Test Webhook model initialization with valid data."""
    mention_data: dict[str, str] = {
        "url": "https://example.com/post/123",
        "title": "Test Post Title",
        "body": "This is a test mention body content",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "image_url": "https://example.com/image.jpg",
        "source": "twitter",
        "source_id": "tweet_123456",
        "author": "test_user",
        "author_avatar_url": "https://example.com/avatar.jpg",
        "author_profile_link": "https://example.com/profile/test_user",
        "relevance_score": "high",
        "relevance_comment": "Highly relevant to our product",
        "language": "en",
        "keyword": "test keyword",
    }

    mention: Mention = Mention(**mention_data)
    webhook: Webhook = Webhook(action="mention_created", data=mention)

    assert webhook.action == "mention_created"
    assert webhook.data == mention
    assert isinstance(webhook.data, Mention)


def test_webhook_default_action() -> None:
    """Test Webhook model with default action value."""
    mention_data: dict[str, str] = {
        "url": "https://example.com/post/123",
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "reddit",
        "source_id": "post_789",
        "author": "reddit_user",
        "relevance_score": "medium",
        "relevance_comment": "Somewhat relevant",
        "language": "en",
        "keyword": "test",
    }

    mention: Mention = Mention(**mention_data)
    webhook: Webhook = Webhook(data=mention)

    assert webhook.action == "mention_created"


def test_modal_get_secret_collection_names() -> None:
    """Test modal_get_secret_collection_names returns expected list."""
    result: list[str] = Webhook.modal_get_secret_collection_names()

    assert isinstance(result, list)
    assert result == ["devx-growth-gcp"]
    assert len(result) == 1


def test_etl_get_bucket_name() -> None:
    """Test etl_get_bucket_name returns correct bucket name."""
    bucket_name: str = Webhook.etl_get_bucket_name()

    assert isinstance(bucket_name, str)
    assert bucket_name == "chalk-ai-devx-octolens-mentions-etl"


def test_storage_get_app_name_raises_not_implemented() -> None:
    """Test storage_get_app_name raises NotImplementedError."""
    import pytest

    with pytest.raises(
        NotImplementedError,
        match="Storage app name is not defined for Webhook",
    ):
        Webhook.storage_get_app_name()


def test_storage_get_base_model_type_returns_none() -> None:
    """Test storage_get_base_model_type returns None."""
    result: None = Webhook.storage_get_base_model_type()

    assert result is None


def test_etl_is_valid_webhook_mention_created() -> None:
    """Test etl_is_valid_webhook returns True for mention_created action."""
    mention_data: dict[str, str] = {
        "url": "https://example.com/post",
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "github",
        "source_id": "issue_456",
        "author": "developer",
        "relevance_score": "high",
        "relevance_comment": "Very relevant",
        "language": "en",
        "keyword": "bug",
    }

    mention: Mention = Mention(**mention_data)
    webhook: Webhook = Webhook(action="mention_created", data=mention)

    result: bool = webhook.etl_is_valid_webhook()
    assert result is True


def test_etl_is_valid_webhook_invalid_action() -> None:
    """Test etl_is_valid_webhook returns False for invalid actions."""
    mention_data: dict[str, str] = {
        "url": "https://example.com/post",
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "slack",
        "source_id": "msg_789",
        "author": "team_member",
        "relevance_score": "low",
        "relevance_comment": "Not very relevant",
        "language": "en",
        "keyword": "meeting",
    }

    mention: Mention = Mention(**mention_data)
    webhook: Webhook = Webhook(action="mention_deleted", data=mention)

    result: bool = webhook.etl_is_valid_webhook()
    assert result is False


def test_etl_is_valid_webhook_empty_action() -> None:
    """Test etl_is_valid_webhook returns False for empty action."""
    mention_data: dict[str, str] = {
        "url": "https://example.com/post",
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "linkedin",
        "source_id": "post_111",
        "author": "professional",
        "relevance_score": "medium",
        "relevance_comment": "Moderately relevant",
        "language": "en",
        "keyword": "career",
    }

    mention: Mention = Mention(**mention_data)
    webhook: Webhook = Webhook(action="", data=mention)

    result: bool = webhook.etl_is_valid_webhook()
    assert result is False


def test_etl_get_invalid_webhook_error_msg() -> None:
    """Test etl_get_invalid_webhook_error_msg returns correct error message."""
    mention_data: dict[str, str] = {
        "url": "https://example.com/post",
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "discord",
        "source_id": "msg_222",
        "author": "gamer",
        "relevance_score": "high",
        "relevance_comment": "Gaming related",
        "language": "en",
        "keyword": "game",
    }

    mention: Mention = Mention(**mention_data)
    webhook: Webhook = Webhook(action="invalid_action", data=mention)

    error_msg: str = webhook.etl_get_invalid_webhook_error_msg()
    assert error_msg == "Invalid webhook: invalid_action"
    assert isinstance(error_msg, str)


def test_etl_get_json() -> None:
    """Test etl_get_json returns JSON string of mention data."""
    import json

    mention_data: dict[str, str] = {
        "url": "https://example.com/post/test",
        "title": "Test JSON Post",
        "body": "JSON test content",
        "timestamp": "2024-02-20T14:45:30+00:00",
        "source": "facebook",
        "source_id": "fb_post_333",
        "author": "social_user",
        "relevance_score": "high",
        "relevance_comment": "Social media buzz",
        "language": "en",
        "keyword": "social",
    }

    mention: Mention = Mention(**mention_data)
    webhook: Webhook = Webhook(data=mention)

    json_result: str = webhook.etl_get_json(storage=None)

    assert isinstance(json_result, str)
    # Verify it's valid JSON
    parsed_json: dict[str, str] = json.loads(json_result)
    assert isinstance(parsed_json, dict)
    assert parsed_json["url"] == "https://example.com/post/test"
    assert parsed_json["title"] == "Test JSON Post"
    assert parsed_json["body"] == "JSON test content"
    assert parsed_json["source"] == "facebook"


def test_etl_get_file_name_default_extension() -> None:
    """Test etl_get_file_name with default .jsonl extension."""
    from unittest.mock import patch

    mention_data: dict[str, str] = {
        "url": "https://example.com/post",
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "youtube",
        "source_id": "video_444",
        "author": "content_creator",
        "relevance_score": "medium",
        "relevance_comment": "Video content",
        "language": "en",
        "keyword": "video",
    }

    mention: Mention = Mention(**mention_data)
    webhook: Webhook = Webhook(data=mention)

    # Mock the get_file_name method on the Mention class
    mock_filename: str = "test_file.jsonl"
    with patch.object(
        Mention,
        "get_file_name",
        return_value=mock_filename,
    ) as mock_method:
        result: str = webhook.etl_get_file_name()

        mock_method.assert_called_once_with(extension=".jsonl")
        assert result == mock_filename


def test_etl_get_file_name_custom_extension() -> None:
    """Test etl_get_file_name with custom extension."""
    from unittest.mock import patch

    mention_data: dict[str, str] = {
        "url": "https://example.com/post",
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "instagram",
        "source_id": "photo_555",
        "author": "photographer",
        "relevance_score": "high",
        "relevance_comment": "Photo content",
        "language": "en",
        "keyword": "photo",
    }

    mention: Mention = Mention(**mention_data)
    webhook: Webhook = Webhook(data=mention)

    mock_filename: str = "test_file.json"
    with patch.object(
        Mention,
        "get_file_name",
        return_value=mock_filename,
    ) as mock_method:
        result: str = webhook.etl_get_file_name(extension=".json")

        mock_method.assert_called_once_with(extension=".json")
        assert result == mock_filename


def test_etl_get_base_models_raises_not_implemented() -> None:
    """Test etl_get_base_models raises NotImplementedError."""
    import pytest

    mention_data: dict[str, str] = {
        "url": "https://example.com/post",
        "body": "Test body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "pinterest",
        "source_id": "pin_666",
        "author": "designer",
        "relevance_score": "low",
        "relevance_comment": "Design inspiration",
        "language": "en",
        "keyword": "design",
    }

    mention: Mention = Mention(**mention_data)
    webhook: Webhook = Webhook(data=mention)

    with pytest.raises(
        NotImplementedError,
        match="Webhook does not support getting base models",
    ):
        webhook.etl_get_base_models(storage=None)


def test_webhook_with_minimal_mention_data() -> None:
    """Test Webhook with minimal required mention data."""
    minimal_mention_data: dict[str, str] = {
        "url": "https://minimal.com",
        "body": "Minimal body",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "test_source",
        "source_id": "test_id",
        "author": "test_author",
        "relevance_score": "unknown",
        "relevance_comment": "No comment",
        "language": "unknown",
        "keyword": "test",
    }

    mention: Mention = Mention(**minimal_mention_data)
    webhook: Webhook = Webhook(data=mention)

    assert webhook.action == "mention_created"
    assert webhook.data.url == "https://minimal.com"
    assert webhook.data.title is None
    assert webhook.data.image_url is None
    assert webhook.data.author_avatar_url is None
    assert webhook.data.author_profile_link is None


def test_webhook_pydantic_validation() -> None:
    """Test Webhook model validation with invalid data."""
    import pytest
    from pydantic import ValidationError

    # Test missing required fields
    with pytest.raises(ValidationError):
        Webhook(
            action="mention_created",
        )  # Missing data field  # type: ignore[call-arg]

    # Test invalid action type
    mention_data: dict[str, str] = {
        "url": "https://example.com",
        "body": "Test",
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

    # Test that valid string action works
    webhook: Webhook = Webhook(action="valid_string_action", data=mention)
    assert webhook.action == "valid_string_action"
    assert isinstance(webhook.action, str)

    # Test that integer action type raises ValidationError
    with pytest.raises(ValidationError):
        Webhook(action=123, data=mention)  # type: ignore[arg-type]


def test_webhook_static_methods_are_callable_without_instance() -> None:
    """Test that all static methods can be called without creating an instance."""
    # Test static methods don't require instance
    secrets: list[str] = Webhook.modal_get_secret_collection_names()
    bucket: str = Webhook.etl_get_bucket_name()
    base_model_type: None = Webhook.storage_get_base_model_type()

    assert isinstance(secrets, list)
    assert isinstance(bucket, str)
    assert base_model_type is None

    # Test storage_get_app_name raises without instance
    import pytest

    with pytest.raises(NotImplementedError):
        Webhook.storage_get_app_name()


def test_webhook_model_fields() -> None:
    """Test Webhook model has expected fields and types."""
    mention_data: dict[str, str] = {
        "url": "https://test.com",
        "body": "Field test",
        "timestamp": "2024-01-15T10:30:00+00:00",
        "source": "field_test",
        "source_id": "field_123",
        "author": "field_author",
        "relevance_score": "high",
        "relevance_comment": "Field testing",
        "language": "en",
        "keyword": "field",
    }

    mention: Mention = Mention(**mention_data)
    webhook: Webhook = Webhook(data=mention)

    # Check field types
    assert isinstance(webhook.action, str)
    assert isinstance(webhook.data, Mention)

    # Check model info
    fields: list[str] = list(Webhook.model_fields.keys())
    assert "action" in fields
    assert "data" in fields
    assert len(fields) == 2


# trunk-ignore-end(ruff/PLR2004,ruff/S101,pyright/reportArgumentType,ruff/PLC0415)
