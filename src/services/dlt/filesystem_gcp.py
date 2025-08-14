import os
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

import gcsfs

from src.services.dlt.filesystem_local import to_filesystem_local
from src.services.local.filesystem import DestinationFileData


class GCPCredentials(NamedTuple):
    project_id: str | None
    private_key: str | None
    client_email: str | None


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
    def _get_env_vars() -> GCPCredentials:
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

        return GCPCredentials(
            project_id=gcp_project_id,
            private_key=gcp_private_key,
            client_email=gcp_client_email,
        )

    @staticmethod
    def get_env_vars() -> GCPCredentials:
        """Get GCP credentials from environment variables.

        Returns:
            GCPCredentials containing project_id, private_key, and client_email
        """
        return CloudGoogle._get_env_vars()

    @staticmethod
    def to_filesystem(
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
        match bucket_url:
            case str() as url if url.startswith("gs://"):
                CloudGoogle.to_filesystem_gcs(
                    destination_file_data=destination_file_data,
                )

            case str():
                bucket_url_path: Path = Path(bucket_url)
                print(bucket_url_path)
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
        credentials: GCPCredentials = CloudGoogle._get_env_vars()
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


# Backward compatibility
gcp_clean_bucket_name = CloudGoogle.clean_bucket_name
gcp_bucket_url_from_bucket_name = CloudGoogle.bucket_url_from_bucket_name
gcp_strip_bucket_url = CloudGoogle.strip_bucket_url
to_filesystem = CloudGoogle.to_filesystem
to_filesystem_gcs = CloudGoogle.to_filesystem_gcs
