from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

import gcsfs

from src.services.dlt.filesystem_local import to_filesystem_local
from src.services.local.filesystem import DestinationFileData

if TYPE_CHECKING:
    from collections.abc import Iterator


class GCPCredentials(NamedTuple):
    project_id: str | None
    private_key: str | None
    client_email: str | None

    @classmethod
    def get_env_vars(cls: type[GCPCredentials]) -> GCPCredentials:
        """Get GCP credentials from environment variables.

        Returns:
            GCPCredentials containing project_id, private_key, and client_email
        """
        gcp_client_email = os.environ.get(
            "GCP_CLIENT_EMAIL",
            None,
        )
        gcp_project_id = os.environ.get(
            "GCP_PROJECT_ID",
            None,
        )
        gcp_private_key = os.environ.get(
            "GCP_PRIVATE_KEY",
            None,
        )
        if gcp_private_key:
            gcp_private_key = gcp_private_key.replace(
                "\\n",
                "\n",
            )

        return cls(
            project_id=gcp_project_id,
            private_key=gcp_private_key,
            client_email=gcp_client_email,
        )


class CloudGoogle:
    """Helper class for Google Cloud Platform operations."""

    @staticmethod
    def clean_bucket_name(bucket_name: str) -> str:
        """Clean bucket name by replacing hyphens with underscores.

        Args:
            bucket_name: The bucket name to clean

        Returns:
            The cleaned bucket name
        """
        return bucket_name.replace(
            "-",
            "_",
        )

    @staticmethod
    def bucket_url_from_bucket_name(bucket_name: str) -> str:
        """Generate a GCS bucket URL from bucket name.

        Args:
            bucket_name: The bucket name

        Returns:
            The GCS bucket URL
        """
        return f"gs://{bucket_name}"

    @staticmethod
    def strip_bucket_url(bucket_url: str) -> str:
        """Strip GCS prefix from bucket URL and clean the name.

        Args:
            bucket_url: The GCS bucket URL

        Returns:
            The cleaned bucket name
        """
        return bucket_url.replace(
            "gs://",
            "",
        ).replace(
            "-",
            "_",
        )

    @staticmethod
    def to_filesystem(
        destination_file_data: Iterator[DestinationFileData],
        bucket_url: str | None,
    ) -> str:
        """Export data to filesystem (GCS or local).

        Args:
            destination_file_data: Iterator of file data to export
            bucket_url: The target bucket URL or local path

        Returns:
            Success message

        Raises:
            ValueError: If bucket_url is invalid
        """
        match bucket_url:
            case str() as url if url.startswith("gs://"):
                CloudGoogle.to_filesystem_gcs(
                    destination_file_data=destination_file_data,
                )

            case str():
                bucket_url_path: Path = Path(bucket_url)
                bucket_url_path.mkdir(
                    parents=True,
                    exist_ok=True,
                )
                to_filesystem_local(
                    destination_file_data=destination_file_data,
                )

            case _:
                error_msg: str = f"Invalid bucket url: {bucket_url}"
                raise ValueError(error_msg)

        return "Successfully exported to filesystem."

    @staticmethod
    def to_filesystem_gcs(
        destination_file_data: Iterator[DestinationFileData],
    ) -> None:
        """Export data specifically to Google Cloud Storage.

        Args:
            destination_file_data: Iterator of file data to export

        Raises:
            ValueError: If GCP credentials are not properly set
        """
        credentials: GCPCredentials = GCPCredentials.get_env_vars()
        if (
            credentials.project_id is None
            or credentials.private_key is None
            or credentials.client_email is None
        ):
            error_msg: str = (
                "GCP_PROJECT_ID, GCP_PRIVATE_KEY, and GCP_CLIENT_EMAIL must be set"
            )
            raise ValueError(
                error_msg,
            )

        fs: gcsfs.GCSFileSystem = gcsfs.GCSFileSystem(
            project=credentials.project_id,
            token={
                "client_email": credentials.client_email,
                "private_key": credentials.private_key,
                "project_id": credentials.project_id,
                "token_uri": "https://oauth2.googleapis.com/token",
            },
        )
        for file_data in destination_file_data:
            with fs.open(
                path=file_data.path,
                mode="w",
            ) as f:
                f.write(
                    file_data.string,
                )

    @staticmethod
    def export_to_filesystem(
        destination_file_data: Iterator[DestinationFileData],
        bucket_url: str,
    ) -> str:
        """Export data to filesystem (GCS or local).

        Args:
            destination_file_data: Iterator of file data to export
            bucket_url: The target bucket URL or local path

        Returns:
            Success message

        Raises:
            ValueError: If bucket_url is invalid
        """
        return CloudGoogle.to_filesystem(destination_file_data, bucket_url)

    @staticmethod
    def export_to_gcs(
        destination_file_data: Iterator[DestinationFileData],
    ) -> None:
        """Export data specifically to Google Cloud Storage.

        Args:
            destination_file_data: Iterator of file data to export

        Raises:
            ValueError: If GCP credentials are not properly set
        """
        CloudGoogle.to_filesystem_gcs(destination_file_data)


# trunk-ignore-begin(ruff/PLR2004,ruff/S101)
def test_clean_bucket_name() -> None:
    """Test clean_bucket_name replaces hyphens with underscores."""
    assert CloudGoogle.clean_bucket_name("my-bucket-name") == "my_bucket_name"
    assert CloudGoogle.clean_bucket_name("my_bucket_name") == "my_bucket_name"
    assert CloudGoogle.clean_bucket_name("my-bucket-name-123") == "my_bucket_name_123"
    assert CloudGoogle.clean_bucket_name("") == ""


def test_bucket_url_from_bucket_name() -> None:
    """Test bucket_url_from_bucket_name generates correct GCS URLs."""
    assert CloudGoogle.bucket_url_from_bucket_name("my-bucket") == "gs://my-bucket"
    assert CloudGoogle.bucket_url_from_bucket_name("test_bucket") == "gs://test_bucket"
    assert CloudGoogle.bucket_url_from_bucket_name("") == "gs://"


def test_strip_bucket_url() -> None:
    """Test strip_bucket_url removes gs:// prefix and cleans bucket name."""
    assert CloudGoogle.strip_bucket_url("gs://my-bucket-name") == "my_bucket_name"
    assert CloudGoogle.strip_bucket_url("gs://my_bucket_name") == "my_bucket_name"
    assert CloudGoogle.strip_bucket_url("my-bucket-name") == "my_bucket_name"
    assert CloudGoogle.strip_bucket_url("") == ""


def test_get_env_vars_with_all_vars() -> None:
    """Test _get_env_vars returns credentials when all env vars are set."""
    from unittest.mock import patch

    with patch.dict(
        os.environ,
        {
            "GCP_PROJECT_ID": "test-project",
            "GCP_PRIVATE_KEY": "-----BEGIN PRIVATE KEY-----\\ntest-key\\n-----END PRIVATE KEY-----",
            "GCP_CLIENT_EMAIL": "test@test-project.iam.gserviceaccount.com",
        },
    ):
        creds: GCPCredentials = GCPCredentials.get_env_vars()
        assert creds.project_id == "test-project"
        assert (
            creds.private_key
            == "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----"
        )
        assert creds.client_email == "test@test-project.iam.gserviceaccount.com"


def test_get_env_vars_with_no_vars() -> None:
    """Test get_env_vars returns None values when env vars are not set."""
    from unittest.mock import patch

    with patch.dict(os.environ, {}, clear=True):
        creds: GCPCredentials = GCPCredentials.get_env_vars()
        assert creds.project_id is None
        assert creds.private_key is None
        assert creds.client_email is None


def test_get_env_vars_with_partial_vars() -> None:
    """Test get_env_vars with only some env vars set."""
    from unittest.mock import patch

    with patch.dict(
        os.environ,
        {"GCP_PROJECT_ID": "test-project"},
    ):
        creds: GCPCredentials = GCPCredentials.get_env_vars()
        assert creds.project_id == "test-project"
        assert creds.private_key is None
        assert creds.client_email is None


def test_get_env_vars_public_method() -> None:
    """Test that get_env_vars works as a class method."""
    from unittest.mock import patch

    test_credentials: GCPCredentials = GCPCredentials(
        project_id="test-project",
        private_key="-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----",
        client_email="test@test-project.iam.gserviceaccount.com",
    )

    with patch.dict(
        os.environ,
        {
            "GCP_PROJECT_ID": "test-project",
            "GCP_PRIVATE_KEY": "-----BEGIN PRIVATE KEY-----\\ntest-key\\n-----END PRIVATE KEY-----",
            "GCP_CLIENT_EMAIL": "test@test-project.iam.gserviceaccount.com",
        },
    ):
        result: GCPCredentials = GCPCredentials.get_env_vars()
        assert result == test_credentials


def test_to_filesystem_with_gcs_url() -> None:
    """Test to_filesystem routes to GCS when URL starts with gs://."""
    from unittest.mock import patch

    with patch.object(CloudGoogle, "to_filesystem_gcs") as mock_to_gcs:
        # Mock to_filesystem_gcs to avoid credential check
        mock_to_gcs.return_value = None

        file_data: list[DestinationFileData] = [
            DestinationFileData(string="test", path="test.json"),
        ]
        result: str = CloudGoogle.to_filesystem(iter(file_data), "gs://my-bucket")

        mock_to_gcs.assert_called_once()
        assert result == "Successfully exported to filesystem."


def test_to_filesystem_with_local_path() -> None:
    """Test to_filesystem routes to local filesystem for non-GCS paths."""
    import tempfile

    # Create a list to hold the data passed to to_filesystem_local
    captured_data: list[DestinationFileData] = []

    def mock_to_filesystem_local(
        *,
        destination_file_data: Iterator[DestinationFileData],
    ) -> None:
        # Consume the iterator to capture the data
        captured_data.extend(list(destination_file_data))

    # Store the original function
    original_func = globals()["to_filesystem_local"]

    with tempfile.TemporaryDirectory() as tmpdir:
        # Replace the function in globals
        globals()["to_filesystem_local"] = mock_to_filesystem_local

        try:
            # Use temp directory path
            local_path: Path = Path(tmpdir) / "local" / "path"

            file_data: list[DestinationFileData] = [
                DestinationFileData(string="test", path="test.json"),
            ]
            result: str = CloudGoogle.to_filesystem(iter(file_data), str(local_path))

            # Verify the directory was created
            assert local_path.exists()
            assert local_path.is_dir()

            # Verify to_filesystem_local was called
            assert len(captured_data) == 1
            assert captured_data[0].string == "test"
            assert result == "Successfully exported to filesystem."

        finally:
            # Restore the original function
            globals()["to_filesystem_local"] = original_func


def test_to_filesystem_with_invalid_url() -> None:
    """Test to_filesystem raises ValueError for None bucket_url."""
    import pytest

    file_data: list[DestinationFileData] = [
        DestinationFileData(string="test", path="test.json"),
    ]
    with pytest.raises(ValueError, match="Invalid bucket url: None"):
        CloudGoogle.to_filesystem(iter(file_data), None)  # type: ignore[arg-type]


def test_to_filesystem_gcs_success() -> None:
    """Test to_filesystem_gcs successfully writes files to GCS."""
    from unittest.mock import MagicMock, patch

    with (
        patch.dict(
            os.environ,
            {
                "GCP_PROJECT_ID": "test-project",
                "GCP_PRIVATE_KEY": "-----BEGIN PRIVATE KEY-----\\ntest-key\\n-----END PRIVATE KEY-----",
                "GCP_CLIENT_EMAIL": "test@test-project.iam.gserviceaccount.com",
            },
        ),
        patch("gcsfs.GCSFileSystem") as mock_gcs_fs,
    ):
        # Setup mock filesystem
        mock_fs_instance: MagicMock = MagicMock()
        mock_file: MagicMock = MagicMock()
        mock_fs_instance.open.return_value.__enter__.return_value = mock_file
        mock_gcs_fs.return_value = mock_fs_instance

        # Create test data
        file_data: list[DestinationFileData] = [
            DestinationFileData(
                string="test content 1",
                path="gs://bucket/file1.json",
            ),
            DestinationFileData(
                string="test content 2",
                path="gs://bucket/file2.json",
            ),
        ]

        # Execute
        CloudGoogle.to_filesystem_gcs(iter(file_data))

        # Verify
        assert mock_gcs_fs.call_count == 1
        assert mock_fs_instance.open.call_count == 2
        assert mock_file.write.call_count == 2
        mock_file.write.assert_any_call("test content 1")
        mock_file.write.assert_any_call("test content 2")


def test_to_filesystem_gcs_missing_credentials() -> None:
    """Test to_filesystem_gcs raises ValueError when credentials are missing."""
    from unittest.mock import patch

    import pytest

    file_data: list[DestinationFileData] = [
        DestinationFileData(string="test", path="test.json"),
    ]

    with (
        patch.dict(os.environ, {}, clear=True),
        pytest.raises(
            ValueError,
            match="GCP_PROJECT_ID, GCP_PRIVATE_KEY, and GCP_CLIENT_EMAIL must be set",
        ),
    ):
        CloudGoogle.to_filesystem_gcs(iter(file_data))


def test_to_filesystem_gcs_partial_credentials() -> None:
    """Test to_filesystem_gcs raises ValueError when some credentials are missing."""
    from unittest.mock import patch

    import pytest

    file_data: list[DestinationFileData] = [
        DestinationFileData(string="test", path="test.json"),
    ]

    with (
        patch.dict(
            os.environ,
            {
                "GCP_PROJECT_ID": "test-project",
                "GCP_CLIENT_EMAIL": "test@test-project.iam.gserviceaccount.com",
            },
        ),
        pytest.raises(
            ValueError,
            match="GCP_PROJECT_ID, GCP_PRIVATE_KEY, and GCP_CLIENT_EMAIL must be set",
        ),
    ):
        CloudGoogle.to_filesystem_gcs(iter(file_data))


def test_to_filesystem_gcs_empty_iterator() -> None:
    """Test to_filesystem_gcs handles empty iterator gracefully."""
    from unittest.mock import MagicMock, patch

    with (
        patch.dict(
            os.environ,
            {
                "GCP_PROJECT_ID": "test-project",
                "GCP_PRIVATE_KEY": "-----BEGIN PRIVATE KEY-----\\ntest-key\\n-----END PRIVATE KEY-----",
                "GCP_CLIENT_EMAIL": "test@test-project.iam.gserviceaccount.com",
            },
        ),
        patch("gcsfs.GCSFileSystem") as mock_gcs_fs,
    ):
        mock_fs_instance: MagicMock = MagicMock()
        mock_gcs_fs.return_value = mock_fs_instance

        # Empty iterator
        file_data: list[DestinationFileData] = []

        # Should not raise any errors
        CloudGoogle.to_filesystem_gcs(iter(file_data))

        # Filesystem should be created but no files opened
        assert mock_gcs_fs.call_count == 1
        assert mock_fs_instance.open.call_count == 0


def test_to_filesystem_gcs_write_error() -> None:
    """Test to_filesystem_gcs when file write fails."""
    from unittest.mock import MagicMock, patch

    import pytest

    with (
        patch.dict(
            os.environ,
            {
                "GCP_PROJECT_ID": "test-project",
                "GCP_PRIVATE_KEY": "-----BEGIN PRIVATE KEY-----\\ntest-key\\n-----END PRIVATE KEY-----",
                "GCP_CLIENT_EMAIL": "test@test-project.iam.gserviceaccount.com",
            },
        ),
        patch("gcsfs.GCSFileSystem") as mock_gcs_fs,
    ):
        mock_fs_instance: MagicMock = MagicMock()
        mock_file: MagicMock = MagicMock()
        mock_file.write.side_effect = OSError("Write failed")
        mock_fs_instance.open.return_value.__enter__.return_value = mock_file
        mock_gcs_fs.return_value = mock_fs_instance

        file_data: list[DestinationFileData] = [
            DestinationFileData(string="test", path="gs://bucket/file.json"),
        ]

        with pytest.raises(OSError, match="Write failed"):
            CloudGoogle.to_filesystem_gcs(iter(file_data))


def test_export_to_filesystem() -> None:
    """Test export_to_filesystem delegates to to_filesystem."""
    from unittest.mock import patch

    with patch.object(
        CloudGoogle,
        "to_filesystem",
        return_value="Success",
    ) as mock_to_fs:
        file_data: list[DestinationFileData] = [
            DestinationFileData(string="test", path="test.json"),
        ]
        result: str = CloudGoogle.export_to_filesystem(iter(file_data), "gs://bucket")

        mock_to_fs.assert_called_once()
        assert result == "Success"


def test_export_to_gcs() -> None:
    """Test export_to_gcs delegates to to_filesystem_gcs."""
    from unittest.mock import patch

    with patch.object(CloudGoogle, "to_filesystem_gcs") as mock_to_gcs:
        file_data: list[DestinationFileData] = [
            DestinationFileData(string="test", path="test.json"),
        ]
        CloudGoogle.export_to_gcs(iter(file_data))

        mock_to_gcs.assert_called_once()


def test_gcp_credentials_namedtuple() -> None:
    """Test GCPCredentials NamedTuple creation and access."""
    creds: GCPCredentials = GCPCredentials(
        project_id="test-project",
        private_key="test-key",
        client_email="test@example.com",
    )

    assert creds.project_id == "test-project"
    assert creds.private_key == "test-key"
    assert creds.client_email == "test@example.com"

    # Test with None values
    empty_creds: GCPCredentials = GCPCredentials(
        project_id=None,
        private_key=None,
        client_email=None,
    )

    assert empty_creds.project_id is None
    assert empty_creds.private_key is None
    assert empty_creds.client_email is None


def test_clean_bucket_name_parametrized() -> None:
    """Parametrized test for clean_bucket_name with various inputs."""
    test_cases: list[tuple[str, str]] = [
        ("simple-bucket", "simple_bucket"),
        ("bucket-with-many-hyphens", "bucket_with_many_hyphens"),
        ("bucket_already_clean", "bucket_already_clean"),
        ("mixed-bucket_name-123", "mixed_bucket_name_123"),
        ("", ""),
        ("a-b-c-d-e-f", "a_b_c_d_e_f"),
    ]

    for bucket_name, expected in test_cases:
        assert CloudGoogle.clean_bucket_name(bucket_name) == expected


def test_strip_bucket_url_parametrized() -> None:
    """Parametrized test for strip_bucket_url with various inputs."""
    test_cases: list[tuple[str, str]] = [
        ("gs://bucket-name", "bucket_name"),
        ("gs://bucket_name", "bucket_name"),
        ("bucket-name", "bucket_name"),
        ("gs://complex-bucket-name-123", "complex_bucket_name_123"),
        ("", ""),
        ("no-prefix-bucket", "no_prefix_bucket"),
    ]

    for url, expected in test_cases:
        assert CloudGoogle.strip_bucket_url(url) == expected


def test_to_filesystem_gcs_with_various_file_types() -> None:
    """Test to_filesystem_gcs with different file types."""
    import json
    from unittest.mock import MagicMock, patch

    def _create_test_destination_file_data() -> Iterator[DestinationFileData]:
        """Helper function to create test DestinationFileData instances.

        Yields various test cases including JSON, CSV, and text files.
        """
        # JSON file
        json_data: dict[str, str | int] = {"test": "data", "count": 42}
        yield DestinationFileData(
            string=json.dumps(json_data),
            path="gs://test-bucket/data/test.json",
        )

        # CSV file
        csv_content: str = "name,age,city\nJohn,30,NYC\nJane,25,LA"
        yield DestinationFileData(
            string=csv_content,
            path="gs://test-bucket/data/users.csv",
        )

        # Text file
        yield DestinationFileData(
            string="This is a test file content",
            path="gs://test-bucket/logs/test.log",
        )

    with (
        patch.dict(
            os.environ,
            {
                "GCP_PROJECT_ID": "test-project",
                "GCP_PRIVATE_KEY": "-----BEGIN PRIVATE KEY-----\\ntest-key\\n-----END PRIVATE KEY-----",
                "GCP_CLIENT_EMAIL": "test@test-project.iam.gserviceaccount.com",
            },
        ),
        patch("gcsfs.GCSFileSystem") as mock_gcs_fs,
    ):
        mock_fs_instance: MagicMock = MagicMock()
        mock_file: MagicMock = MagicMock()
        mock_fs_instance.open.return_value.__enter__.return_value = mock_file
        mock_gcs_fs.return_value = mock_fs_instance

        # Execute with test data
        CloudGoogle.to_filesystem_gcs(_create_test_destination_file_data())

        # Verify all files were processed
        assert mock_fs_instance.open.call_count == 3
        assert mock_file.write.call_count == 3


def test_to_filesystem_creates_directory_for_local_path() -> None:
    """Test that to_filesystem creates directories for local paths."""
    import tempfile

    # Create a list to hold the data passed to to_filesystem_local
    captured_data: list[DestinationFileData] = []

    def mock_to_filesystem_local(
        *,
        destination_file_data: Iterator[DestinationFileData],
    ) -> None:
        # Consume the iterator to capture the data
        captured_data.extend(list(destination_file_data))

    # Store the original function
    original_func: object = globals()["to_filesystem_local"]

    with tempfile.TemporaryDirectory() as tmpdir:
        # Replace the function in globals
        globals()["to_filesystem_local"] = mock_to_filesystem_local

        try:
            # Use temp directory path
            local_path: Path = Path(tmpdir) / "local" / "path" / "to" / "dir"

            file_data: list[DestinationFileData] = [
                DestinationFileData(string="test", path="test.json"),
            ]
            CloudGoogle.to_filesystem(iter(file_data), str(local_path))

            # Verify directory creation
            assert local_path.exists()
            assert local_path.is_dir()

            # Verify to_filesystem_local was called
            assert len(captured_data) == 1

        finally:
            # Restore the original function
            globals()["to_filesystem_local"] = original_func


# trunk-ignore-end(ruff/PLR2004,ruff/S101)
