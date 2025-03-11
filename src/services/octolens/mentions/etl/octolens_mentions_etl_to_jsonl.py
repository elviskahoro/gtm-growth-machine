# trunk-ignore-all(ruff/PLW0603)
from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import gcsfs
import modal
from modal import Image

from src.services.local.filesystem import get_data_from_input_folder
from src.services.modal.filesystem import convert_bucket_url_to_pipeline_name
from src.services.modal.local_entrypoint import (
    DestinationType,
)
from src.services.octolens import Mention, MentionData

if TYPE_CHECKING:
    from collections.abc import Iterator

DLT_DESTINATION_URL_GCP: str = "gs://chalk-ai-devx-octolens-mentions-etl"
DEVX_PIPELINE_NAME: str = convert_bucket_url_to_pipeline_name(
    DLT_DESTINATION_URL_GCP,
)
MODAL_SECRET_COLLECTION_NAME: str = "devx-growth-gcp"  # trunk-ignore(ruff/S105)

image: Image = modal.Image.debian_slim().pip_install(
    "fastapi[standard]",
    "gcsfs",  # https://github.com/fsspec/gcsfs
    "uuid7",
)
image.add_local_python_source(
    *[
        "src",
    ],
)
app = modal.App(
    name=DEVX_PIPELINE_NAME,
    image=image,
)

gcp_project_id: str | None = None
gcp_private_key: str | None = None
gcp_client_email: str | None = None


def _get_env_vars() -> None:
    global gcp_project_id
    global gcp_private_key
    global gcp_client_email
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


def _to_filesystem_local(
    data_to_upload: Iterator[tuple[MentionData, str]],
) -> None:
    output_path_str: str
    mention_data: MentionData
    for count, (mention_data, output_path_str) in enumerate(
        data_to_upload,
        start=1,
    ):
        print(f"{count:06d}: {output_path_str}")
        output_path: Path = Path(output_path_str)
        with output_path.open(
            mode="w+",
        ) as f:
            f.write(
                mention_data.model_dump_json(
                    indent=None,
                ),
            )


def _to_filesystem_gcs(
    data_to_upload: Iterator[tuple[MentionData, str]],
) -> None:
    if gcp_project_id is None or gcp_private_key is None or gcp_client_email is None:
        error_msg: str = (
            "GCP_PROJECT_ID, GCP_PRIVATE_KEY, and GCP_CLIENT_EMAIL must be set"
        )
        raise ValueError(
            error_msg,
        )

    fs: gcsfs.GCSFileSystem = gcsfs.GCSFileSystem(
        project=gcp_project_id,
        token={
            "client_email": gcp_client_email,
            "private_key": gcp_private_key,
            "project_id": gcp_project_id,
            "token_uri": "https://oauth2.googleapis.com/token",
        },
    )
    output_path: str
    mention_data: MentionData
    for count, (mention_data, output_path) in enumerate(
        data_to_upload,
        start=1,
    ):
        print(f"{count:06d}: {output_path}")
        with fs.open(
            path=output_path,
            mode="w",
        ) as f:
            f.write(
                mention_data.model_dump_json(
                    indent=None,
                ),
            )


def to_filesystem(
    mentions_data: Iterator[MentionData],
    bucket_url: str = DLT_DESTINATION_URL_GCP,
) -> str:
    data_to_upload: Iterator[tuple[MentionData, str]] = (
        (
            mention_data,
            f"{bucket_url}/{mention_data.get_file_name()}",
        )
        for mention_data in mentions_data
    )

    match bucket_url:
        case str() as url if url.startswith("gs://"):
            _to_filesystem_gcs(
                data_to_upload=data_to_upload,
            )

        case _:
            bucket_url_path: Path = Path(bucket_url)
            bucket_url_path.mkdir(
                parents=True,
                exist_ok=True,
            )
            _to_filesystem_local(
                data_to_upload=data_to_upload,
            )

    return "Successfully uploaded all mentions"


@app.function(
    secrets=[
        modal.Secret.from_name(
            name=MODAL_SECRET_COLLECTION_NAME,
        ),
    ],
    # cloud="aws", This feature is available on the Team and Enterprise plans, read more at https://modal.com/docs/guide/region-selection
    # region="us-west-2", This feature is available on the Team and Enterprise plans, read more at https://modal.com/docs/guide/region-selection
    allow_concurrent_inputs=1000,
    enable_memory_snapshot=True,
)
@modal.web_endpoint(
    method="POST",
    docs=True,
)
def web(
    mention: Mention,  # MODAL: Change this BaseModel if you're bootstrapping a new pipeline
) -> str:
    def mention_created(
        mention: Mention,
    ) -> str:
        return to_filesystem(
            mentions_data=[mention.data],  # trunk-ignore(pyright/reportArgumentType)
            bucket_url=DLT_DESTINATION_URL_GCP,
        )

    match mention.action:
        case "mention_created":
            _get_env_vars()
            return mention_created(
                mention=mention,
            )

        case _:
            return "Invalid action: " + mention.action


@app.local_entrypoint()
def local(
    input_folder: str,
    destination_type: str,
) -> None:
    destination_type_enum: DestinationType = DestinationType(destination_type)
    bucket_url: str
    match destination_type_enum:
        case DestinationType.LOCAL:
            bucket_url = DestinationType.get_bucket_url_for_local(
                pipeline_name=DEVX_PIPELINE_NAME,
            )

        case DestinationType.GCP:
            _get_env_vars()
            bucket_url = DLT_DESTINATION_URL_GCP

        case _:
            error_msg: str = f"Invalid destination type: {destination_type_enum}"
            raise ValueError(error_msg)

    mentions: list[Mention] = (  # trunk-ignore(pyright/reportAssignmentType)
        get_data_from_input_folder(
            input_folder=input_folder,
            base_model=Mention,  # trunk-ignore(pyright/reportArgumentType)
        )
    )
    print(f"Exporting {len(mentions)} mentions to {bucket_url}")
    response: str = to_filesystem(
        mentions_data=(mention.data for mention in mentions),
        bucket_url=bucket_url,
    )
    print(response)
