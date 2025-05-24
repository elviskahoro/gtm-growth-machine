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


def gcp_clean_bucket_name(
    bucket_name: str,
) -> str:
    return bucket_name.replace(
        "-",
        "_",
    )


def gcp_bucket_url_from_bucket_name(
    bucket_name: str,
) -> str:
    return f"gs://{bucket_name}"


def gcp_strip_bucket_url(
    bucket_url: str,
) -> str:
    return bucket_url.replace(
        "gs://",
        "",
    ).replace(
        "-",
        "_",
    )


def _get_env_vars() -> GCPCredentials:
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

    gcp_client_email = os.environ.get(
        "GCP_CLIENT_EMAIL",
        None,
    )
    return GCPCredentials(
        project_id=gcp_project_id,
        private_key=gcp_private_key,
        client_email=gcp_client_email,
    )


def to_filesystem(
    destination_file_data: Iterator[DestinationFileData],
    bucket_url: str,
) -> str:
    match bucket_url:
        case str() as url if url.startswith("gs://"):
            to_filesystem_gcs(
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

    return "Successfully uploaded"


def to_filesystem_gcs(
    destination_file_data: Iterator[DestinationFileData],
) -> None:
    credentials: GCPCredentials = _get_env_vars()
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
    for json_data in destination_file_data:
        with fs.open(
            path=json_data.path,
            mode="w",
        ) as f:
            f.write(
                json_data.json,
            )
