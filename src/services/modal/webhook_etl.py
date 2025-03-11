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

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pydantic import BaseModel

from src.services.octolens import Mention


class WebhookModel(Mention): ...


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
    data_to_upload: Iterator[tuple[BaseModel, str]],
) -> None:
    etl_data: BaseModel
    output_path_str: str
    for count, (etl_data, output_path_str) in enumerate(
        data_to_upload,
        start=1,
    ):
        print(f"{count:06d}: {output_path_str}")
        output_path: Path = Path(output_path_str)
        with output_path.open(
            mode="w+",
        ) as f:
            f.write(
                etl_data.model_dump_json(
                    indent=None,
                ),
            )


def _to_filesystem_gcs(
    data_to_upload: Iterator[tuple[BaseModel, str]],
) -> None:
    _get_env_vars()
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
    etl_data: BaseModel
    for count, (etl_data, output_path) in enumerate(
        data_to_upload,
        start=1,
    ):
        print(f"{count:06d}: {output_path}")
        with fs.open(
            path=output_path,
            mode="w",
        ) as f:
            f.write(
                etl_data.model_dump_json(
                    indent=None,
                ),
            )


def to_filesystem(
    etl_data: Iterator[BaseModel],
    bucket_url: str = DLT_DESTINATION_URL_GCP,
) -> str:
    data_to_upload: Iterator[tuple[BaseModel, str]] = (
        (
            etl_data,
            f"{bucket_url}/{etl_data.get_file_name()}",
        )
        for etl_data in etl_data
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

    return "Successfully uploaded"


@app.function(
    secrets=[
        modal.Secret.from_name(
            name=MODAL_SECRET_COLLECTION_NAME,
        ),
    ],
    region="us-east4",  # This feature is available on the Team and Enterprise plans, read more at https://modal.com/docs/guide/region-selection
    allow_concurrent_inputs=1000,
    enable_memory_snapshot=True,
)
@modal.web_endpoint(
    method="POST",
    docs=True,
)
def web(
    webhook: WebhookModel,
) -> str:
    if not webhook.etl_is_valid_webhook():
        return webhook.etl_get_invalid_webhook_error_msg()

    etl_data: BaseModel = webhook.etl_get_data()
    return to_filesystem(
        etl_data=[etl_data],  # trunk-ignore(pyright/reportArgumentType)
        bucket_url=DLT_DESTINATION_URL_GCP,
    )


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
            bucket_url = DLT_DESTINATION_URL_GCP

        case _:
            error_msg: str = f"Invalid destination type: {destination_type_enum}"
            raise ValueError(error_msg)

    webhook_data: list[WebhookModel] = (  # trunk-ignore(pyright/reportAssignmentType)
        get_data_from_input_folder(
            input_folder=input_folder,
            base_model=WebhookModel,  # trunk-ignore(pyright/reportArgumentType)
        )
    )
    print(f"Exporting {len(webhook_data)} webhooks to {bucket_url}")
    response: str = to_filesystem(
        etl_data=(webhook.etl_get_data() for webhook in webhook_data),
        bucket_url=bucket_url,
    )
    print(response)
