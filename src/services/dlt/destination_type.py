from __future__ import annotations

from enum import Enum
from pathlib import Path

from src.services.dlt.filesystem_gcp import CloudGoogle


class DestinationType(str, Enum):
    LOCAL = "local"
    GCS = "gcs"

    @classmethod
    def from_string(cls, value: str) -> DestinationType:
        try:
            return cls(value)

        except ValueError as e:
            valid_values: str = ", ".join([member.value for member in cls])
            error_msg: str = (
                f"Invalid destination type: '{value}'. "
                f"Valid values are: {valid_values}"
            )
            raise ValueError(error_msg) from e

    @staticmethod
    def get_bucket_url_from_bucket_name_for_local(
        bucket_name: str,
    ) -> str:
        cwd: str = str(Path.cwd())
        return f"{cwd}/out/{bucket_name}"

    def get_bucket_url_from_bucket_name(
        self: DestinationType,
        bucket_name: str,
    ) -> str:
        match self:
            case DestinationType.LOCAL:
                return DestinationType.get_bucket_url_from_bucket_name_for_local(
                    bucket_name=bucket_name,
                )
            case DestinationType.GCS:
                return CloudGoogle.bucket_url_from_bucket_name(
                    bucket_name=bucket_name,
                )
            case _:
                error_msg: str = f"Invalid destination type: {self}"
                raise ValueError(error_msg)


# trunk-ignore-begin(ruff/PLR2004,ruff/S101)
def test_destination_type_enum_values() -> None:
    """Check that LOCAL and GCP enum values are correctly defined."""
    # Verify that the enum has LOCAL and GCP members
    assert hasattr(DestinationType, "LOCAL")
    assert hasattr(DestinationType, "GCP")

    # Verify the string values of the enum members
    assert DestinationType.LOCAL.value == "local"
    assert DestinationType.GCS.value == "gcp"

    # Additional check: verify these are the only members
    assert len(DestinationType) == 2
    assert {member.value for member in DestinationType} == {"local", "gcp"}


def test_destination_type_is_string_enum() -> None:
    """Verify that DestinationType inherits from str and Enum."""
    # Check inheritance from both str and Enum
    assert issubclass(DestinationType, str)
    assert issubclass(DestinationType, Enum)

    # Verify instances are both strings and enum members
    assert isinstance(DestinationType.LOCAL, str)
    assert isinstance(DestinationType.LOCAL, Enum)
    assert isinstance(DestinationType.GCS, str)
    assert isinstance(DestinationType.GCS, Enum)

    # Verify string behavior - enum members can be compared directly to strings
    assert DestinationType.LOCAL == "local"
    assert DestinationType.GCS == "gcp"

    # Verify the .value attribute returns the string value
    assert DestinationType.LOCAL.value == "local"
    assert DestinationType.GCS.value == "gcp"

    # Note: str() returns the full enum name, not just the value
    assert str(DestinationType.LOCAL) == "DestinationType.LOCAL"
    assert str(DestinationType.GCS) == "DestinationType.GCP"


def test_get_bucket_url_from_bucket_name_for_local() -> None:
    """Test the get_bucket_url_from_bucket_name_for_local static method."""
    from unittest import mock

    # Mock Path.cwd() to return a controlled path
    with mock.patch("src.services.dlt.destination_type.Path.cwd") as mock_cwd:
        mock_cwd.return_value = Path("/test/path")

        # Test with a simple bucket name
        bucket_name: str = "test-bucket"
        result: str = DestinationType.get_bucket_url_from_bucket_name_for_local(
            bucket_name,
        )
        assert result == "/test/path/out/test-bucket"

        # Test with different bucket names to ensure proper concatenation
        bucket_name = "my-data-bucket-123"
        result = DestinationType.get_bucket_url_from_bucket_name_for_local(bucket_name)
        assert result == "/test/path/out/my-data-bucket-123"

        # Test with bucket name containing special characters
        bucket_name = "bucket_with-special.chars"
        result = DestinationType.get_bucket_url_from_bucket_name_for_local(bucket_name)
        assert result == "/test/path/out/bucket_with-special.chars"

        # Test with empty bucket name (edge case)
        bucket_name = ""
        result = DestinationType.get_bucket_url_from_bucket_name_for_local(bucket_name)
        assert result == "/test/path/out/"

        # Verify mock was called the expected number of times
        assert mock_cwd.call_count == 4


def test_get_bucket_url_from_bucket_name_local() -> None:
    """Test get_bucket_url_from_bucket_name for LOCAL destination type."""
    from unittest import mock

    # Mock Path.cwd() to control the working directory
    with mock.patch("src.services.dlt.destination_type.Path.cwd") as mock_cwd:
        mock_cwd.return_value = Path("/mock/working/directory")

        # Create a LOCAL DestinationType instance
        local_destination: DestinationType = DestinationType.LOCAL

        # Test with a simple bucket name
        bucket_name: str = "test-bucket"
        result: str = local_destination.get_bucket_url_from_bucket_name(bucket_name)
        assert result == "/mock/working/directory/out/test-bucket"

        # Test with different bucket names
        bucket_name = "my-local-storage"
        result = local_destination.get_bucket_url_from_bucket_name(bucket_name)
        assert result == "/mock/working/directory/out/my-local-storage"

        # Test with bucket name containing numbers and special characters
        bucket_name = "bucket-123_test.data"
        result = local_destination.get_bucket_url_from_bucket_name(bucket_name)
        assert result == "/mock/working/directory/out/bucket-123_test.data"

        # Test with nested path-like bucket name
        bucket_name = "project/sub-bucket/data"
        result = local_destination.get_bucket_url_from_bucket_name(bucket_name)
        assert result == "/mock/working/directory/out/project/sub-bucket/data"

        # Test with empty bucket name (edge case)
        bucket_name = ""
        result = local_destination.get_bucket_url_from_bucket_name(bucket_name)
        assert result == "/mock/working/directory/out/"

        # Verify that the static method is being called internally
        # by checking the mock was called the expected number of times
        assert mock_cwd.call_count == 5


def test_get_bucket_url_from_bucket_name_gcp() -> None:
    """Test get_bucket_url_from_bucket_name for GCP destination type."""
    from unittest import mock

    # Mock CloudGoogle.bucket_url_from_bucket_name() to return a controlled value
    with mock.patch(
        "src.services.dlt.filesystem_gcp.CloudGoogle.bucket_url_from_bucket_name",
    ) as mock_bucket_url:
        # Create a GCP DestinationType instance
        gcp_destination: DestinationType = DestinationType.GCS

        # Test with a simple bucket name
        bucket_name: str = "test-bucket"
        expected_url: str = "gs://test-bucket"
        mock_bucket_url.return_value = expected_url

        result: str = gcp_destination.get_bucket_url_from_bucket_name(bucket_name)

        # Assert the mocked CloudGoogle method was called with correct parameters
        mock_bucket_url.assert_called_once_with(bucket_name=bucket_name)
        # Verify the returned value matches the mocked response
        assert result == expected_url

        # Test with different bucket names
        bucket_name = "my-gcp-storage-bucket"
        expected_url = "gs://my-gcp-storage-bucket"
        mock_bucket_url.return_value = expected_url

        result = gcp_destination.get_bucket_url_from_bucket_name(bucket_name)

        # Check the method was called again with the new bucket name
        mock_bucket_url.assert_called_with(bucket_name=bucket_name)
        assert result == expected_url

        # Test with bucket name containing numbers and special characters
        bucket_name = "project-123-data-bucket"
        expected_url = "gs://project-123-data-bucket"
        mock_bucket_url.return_value = expected_url

        result = gcp_destination.get_bucket_url_from_bucket_name(bucket_name)

        mock_bucket_url.assert_called_with(bucket_name=bucket_name)
        assert result == expected_url

        # Test with nested path-like bucket name
        bucket_name = "company/department/project-data"
        expected_url = "gs://company/department/project-data"
        mock_bucket_url.return_value = expected_url

        result = gcp_destination.get_bucket_url_from_bucket_name(bucket_name)

        mock_bucket_url.assert_called_with(bucket_name=bucket_name)
        assert result == expected_url

        # Test with empty bucket name (edge case)
        bucket_name = ""
        expected_url = "gs://"
        mock_bucket_url.return_value = expected_url

        result = gcp_destination.get_bucket_url_from_bucket_name(bucket_name)

        mock_bucket_url.assert_called_with(bucket_name=bucket_name)
        assert result == expected_url

        # Verify the mock was called the expected number of times
        assert mock_bucket_url.call_count == 5


def test_get_bucket_url_from_bucket_name_invalid() -> None:
    """Test get_bucket_url_from_bucket_name raises ValueError for invalid destination type."""
    from unittest import mock

    import pytest

    # Create a mock DestinationType instance with an invalid value
    # We need to bypass the enum validation by mocking the instance
    invalid_destination: mock.Mock = mock.Mock(spec=DestinationType)

    # Set the mock to return an invalid value when converted to string
    invalid_destination.__str__ = mock.Mock(return_value="DestinationType.INVALID")
    invalid_destination.value = "invalid"

    # Configure the mock to make it behave like an enum member in the match statement
    # The match statement uses identity comparison, so we need to ensure it doesn't match
    # LOCAL or GCP
    invalid_destination.__eq__ = mock.Mock(return_value=False)
    invalid_destination.__ne__ = mock.Mock(return_value=True)

    # Use pytest.raises to verify ValueError is raised
    with pytest.raises(
        ValueError,
        match="Invalid destination type",
    ) as exc_info:
        # Call the method using the bound method approach
        DestinationType.get_bucket_url_from_bucket_name(
            invalid_destination,
            "test-bucket",
        )

    # Verify the error message contains "Invalid destination type"
    assert "Invalid destination type" in str(exc_info.value)
    # Verify the error message includes the invalid destination
    assert "invalid" in str(exc_info.value) or "INVALID" in str(exc_info.value)


# trunk-ignore-end(ruff/PLR2004,ruff/S101)
