from __future__ import annotations

import os
from typing import TYPE_CHECKING

import lancedb

if TYPE_CHECKING:
    from lancedb.db import DBConnection


LANCEDB_REGION: str = "us-east-1"


def init_client(
    project_name: str,
    region: str = LANCEDB_REGION,
) -> DBConnection:
    lance_api_key: str | None = os.getenv("LANCEDB_API_KEY")
    if lance_api_key is None:
        error_msg: str = "LANCEDB_API_KEY is not set."
        raise ValueError(error_msg)

    return lancedb.connect(
        uri=f"db://{project_name}",
        api_key=lance_api_key,
        region=region,
    )


# trunk-ignore-begin(ruff/PLR2004,ruff/S101,ruff/PLC0415)
def test_lancedb_region_constant() -> None:
    """Test that LANCEDB_REGION constant has the expected value."""
    assert LANCEDB_REGION == "us-east-1"
    assert isinstance(LANCEDB_REGION, str)


def test_init_client_successful_connection() -> None:
    """Test init_client with valid API key creates connection successfully."""
    from unittest.mock import MagicMock, patch

    project_name: str = "test-project"
    mock_api_key: str = "test-api-key-123"
    mock_connection: MagicMock = MagicMock()

    with patch("os.getenv", return_value=mock_api_key) as mock_getenv, patch(
        "lancedb.connect",
        return_value=mock_connection,
    ) as mock_connect:
        result: DBConnection = init_client(project_name)

        # Verify environment variable was checked
        mock_getenv.assert_called_once_with("LANCEDB_API_KEY")

        # Verify lancedb.connect was called with correct parameters
        mock_connect.assert_called_once_with(
            uri="db://test-project",
            api_key=mock_api_key,
            region=LANCEDB_REGION,
        )

        # Verify return value
        assert result == mock_connection


def test_init_client_custom_region() -> None:
    """Test init_client with custom region parameter."""
    from unittest.mock import MagicMock, patch

    project_name: str = "custom-project"
    custom_region: str = "us-west-2"
    mock_api_key: str = "custom-api-key-456"
    mock_connection: MagicMock = MagicMock()

    with patch("os.getenv", return_value=mock_api_key), patch(
        "lancedb.connect",
        return_value=mock_connection,
    ) as mock_connect:
        result: DBConnection = init_client(project_name, region=custom_region)

        # Verify lancedb.connect was called with custom region
        mock_connect.assert_called_once_with(
            uri="db://custom-project",
            api_key=mock_api_key,
            region=custom_region,
        )

        assert result == mock_connection


def test_init_client_missing_api_key() -> None:
    """Test init_client raises ValueError when LANCEDB_API_KEY is not set."""
    from unittest.mock import patch

    import pytest

    project_name: str = "error-project"

    with patch("os.getenv", return_value=None) as mock_getenv:
        with pytest.raises(ValueError, match="LANCEDB_API_KEY is not set"):
            init_client(project_name)

        # Verify environment variable was checked
        mock_getenv.assert_called_once_with("LANCEDB_API_KEY")


def test_init_client_empty_string_api_key() -> None:
    """Test init_client with empty string API key (should work as it's truthy)."""
    from unittest.mock import MagicMock, patch

    project_name: str = "empty-key-project"
    empty_api_key: str = ""
    mock_connection: MagicMock = MagicMock()

    with patch("os.getenv", return_value=empty_api_key), patch(
        "lancedb.connect",
        return_value=mock_connection,
    ) as mock_connect:
        result: DBConnection = init_client(project_name)

        # Empty string should be passed to lancedb.connect
        mock_connect.assert_called_once_with(
            uri="db://empty-key-project",
            api_key=empty_api_key,
            region=LANCEDB_REGION,
        )

        assert result == mock_connection


def test_init_client_different_project_names() -> None:
    """Test init_client with various project name formats."""
    from unittest.mock import MagicMock, patch

    mock_api_key: str = "test-key"
    mock_connection: MagicMock = MagicMock()

    test_cases: list[tuple[str, str]] = [
        ("simple", "db://simple"),
        ("project-with-dashes", "db://project-with-dashes"),
        ("project_with_underscores", "db://project_with_underscores"),
        ("Project123", "db://Project123"),
        ("123-numeric-project", "db://123-numeric-project"),
    ]

    with patch("os.getenv", return_value=mock_api_key), patch(
        "lancedb.connect",
        return_value=mock_connection,
    ) as mock_connect:
        for project_name, expected_uri in test_cases:
            result: DBConnection = init_client(project_name)

            # Check that the URI was constructed correctly
            mock_connect.assert_called_with(
                uri=expected_uri,
                api_key=mock_api_key,
                region=LANCEDB_REGION,
            )

            assert result == mock_connection

            # Reset mock for next iteration
            mock_connect.reset_mock()


def test_init_client_uri_construction() -> None:
    """Test that init_client constructs URI correctly."""
    from unittest.mock import MagicMock, patch

    project_name: str = "uri-test-project"
    mock_api_key: str = "uri-test-key"
    mock_connection: MagicMock = MagicMock()

    with patch("os.getenv", return_value=mock_api_key), patch(
        "lancedb.connect",
        return_value=mock_connection,
    ) as mock_connect:
        init_client(project_name)

        # Extract the URI argument from the mock call
        call_args: tuple = mock_connect.call_args
        uri_arg: str = call_args[1]["uri"]  # keyword argument

        assert uri_arg == "db://uri-test-project"
        assert uri_arg.startswith("db://")
        assert project_name in uri_arg


def test_init_client_default_region_parameter() -> None:
    """Test that init_client uses LANCEDB_REGION as default region."""
    from unittest.mock import MagicMock, patch

    project_name: str = "default-region-project"
    mock_api_key: str = "default-region-key"
    mock_connection: MagicMock = MagicMock()

    with patch("os.getenv", return_value=mock_api_key), patch(
        "lancedb.connect",
        return_value=mock_connection,
    ) as mock_connect:
        # Call without specifying region
        init_client(project_name)

        # Verify default region was used
        call_args: tuple = mock_connect.call_args
        region_arg: str = call_args[1]["region"]  # keyword argument

        assert region_arg == LANCEDB_REGION
        assert region_arg == "us-east-1"


def test_init_client_return_type() -> None:
    """Test that init_client returns the correct type."""
    from unittest.mock import MagicMock, patch

    project_name: str = "type-test-project"
    mock_api_key: str = "type-test-key"
    mock_connection: MagicMock = MagicMock()

    with patch("os.getenv", return_value=mock_api_key), patch(
        "lancedb.connect",
        return_value=mock_connection,
    ):
        result: DBConnection = init_client(project_name)

        # Verify return value is what lancedb.connect returned
        assert result is mock_connection
        assert result == mock_connection


def test_init_client_special_characters_in_project_name() -> None:
    """Test init_client with special characters in project name."""
    from unittest.mock import MagicMock, patch

    mock_api_key: str = "special-char-key"
    mock_connection: MagicMock = MagicMock()

    special_project_names: list[str] = [
        "project@domain.com",
        "project with spaces",
        "project/with/slashes",
        "project\\with\\backslashes",
        "project#with#hashes",
        "unicode-prøject-ñame",
    ]

    with patch("os.getenv", return_value=mock_api_key), patch(
        "lancedb.connect",
        return_value=mock_connection,
    ) as mock_connect:
        for project_name in special_project_names:
            result: DBConnection = init_client(project_name)

            # Verify URI construction with special characters
            call_args: tuple = mock_connect.call_args
            uri_arg: str = call_args[1]["uri"]
            expected_uri: str = f"db://{project_name}"

            assert uri_arg == expected_uri
            assert result == mock_connection

            # Reset mock for next iteration
            mock_connect.reset_mock()


def test_init_client_environment_variable_handling() -> None:
    """Test that init_client properly handles environment variable retrieval."""
    from unittest.mock import MagicMock, patch

    project_name: str = "env-test-project"
    mock_api_key: str = "env-test-key"
    mock_connection: MagicMock = MagicMock()

    with patch("os.getenv", return_value=mock_api_key) as mock_getenv, patch(
        "lancedb.connect",
        return_value=mock_connection,
    ):
        init_client(project_name)

        # Verify os.getenv was called with correct environment variable name
        mock_getenv.assert_called_once_with("LANCEDB_API_KEY")


def test_init_client_lancedb_connect_call() -> None:
    """Test that init_client calls lancedb.connect with all expected parameters."""
    from unittest.mock import MagicMock, patch

    project_name: str = "connect-test-project"
    custom_region: str = "eu-west-1"
    mock_api_key: str = "connect-test-key"
    mock_connection: MagicMock = MagicMock()

    with patch("os.getenv", return_value=mock_api_key), patch(
        "lancedb.connect",
        return_value=mock_connection,
    ) as mock_connect:
        init_client(project_name, region=custom_region)

        # Verify all parameters were passed correctly
        mock_connect.assert_called_once()
        call_kwargs: dict[str, str] = mock_connect.call_args[1]

        assert call_kwargs["uri"] == f"db://{project_name}"
        assert call_kwargs["api_key"] == mock_api_key
        assert call_kwargs["region"] == custom_region
        assert len(call_kwargs) == 3  # Ensure no extra parameters


def test_init_client_error_message_content() -> None:
    """Test that init_client error message contains expected content."""
    from unittest.mock import patch

    import pytest

    project_name: str = "error-message-project"

    with patch("os.getenv", return_value=None):
        with pytest.raises(ValueError, match="LANCEDB_API_KEY is not set") as exc_info:
            init_client(project_name)

        error_message: str = str(exc_info.value)
        assert "LANCEDB_API_KEY is not set" in error_message
        assert isinstance(exc_info.value, ValueError)


# trunk-ignore-end(ruff/PLR2004,ruff/S101,ruff/PLC0415)
