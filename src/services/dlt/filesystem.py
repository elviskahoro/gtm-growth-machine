import os
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

import gcsfs


class GCPCredentials(NamedTuple):
    project_id: str | None
    private_key: str | None
    client_email: str | None


def convert_bucket_url_to_pipeline_name(x: str) -> str:
    return x.replace(
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


def to_filesystem_gcs(
    data: Iterator[tuple[str, str]],
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
    json: str
    output_path: str
    for count, (json, output_path) in enumerate(
        data,
        start=1,
    ):
        print(f"{count:06d}: {output_path}")
        with fs.open(
            path=output_path,
            mode="w",
        ) as f:
            f.write(
                json,
            )


def to_filesystem_local(
    data: Iterator[tuple[str, str]],
) -> None:
    json: str
    output_path_str: str
    for count, (json, output_path_str) in enumerate(
        data,
        start=1,
    ):
        print(f"{count:06d}: {output_path_str}")
        output_path: Path = Path(output_path_str)
        with output_path.open(
            mode="w+",
        ) as f:
            f.write(
                json,
            )
