from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import modal
from modal import Image
from src.services.dlt.destination_type import (
    DestinationType,
)
from src.services.dlt.filesystem import (
    convert_bucket_url_to_pipeline_name,
    to_filesystem_gcs,
    to_filesystem_local,
)
from src.services.local.filesystem import FileData, get_data_from_input_folder

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pydantic import BaseModel

from src.services.octolens.mentions.etl.webhook import Webhook


class WebhookModel(Webhook): ...


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


def to_filesystem(
    base_models: Iterator[BaseModel],
    bucket_url: str = DLT_DESTINATION_URL_GCP,
) -> str:
    data: Iterator[tuple[str, str]] = (
        (
            base_model.model_dump_json(
                indent=None,
            ),
            f"{bucket_url}/{base_model.etl_get_file_name()}",
        )
        for base_model in base_models
    )

    match bucket_url:
        case str() as url if url.startswith("gs://"):
            to_filesystem_gcs(
                data=data,
            )

        case _:
            bucket_url_path: Path = Path(bucket_url)
            bucket_url_path.mkdir(
                parents=True,
                exist_ok=True,
            )
            to_filesystem_local(
                data=data,
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
        base_models=[etl_data],  # trunk-ignore(pyright/reportArgumentType)
        bucket_url=DLT_DESTINATION_URL_GCP,
    )


def _process_file_data(
    file_data: Iterator[FileData],
) -> Iterator[BaseModel]:
    for individual_file_data in file_data:
        try:
            yield individual_file_data.base_model.etl_get_data()

        except (AttributeError, ValueError):
            error_msg: str = f"Error processing file: {individual_file_data.path}"
            print(error_msg)
            raise


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

    file_data: Iterator[FileData] = get_data_from_input_folder(
        input_folder=input_folder,
        base_model=WebhookModel,  # trunk-ignore(pyright/reportArgumentType)
    )
    base_models: Iterator[BaseModel] = _process_file_data(
        file_data=file_data,
    )
    response: str = to_filesystem(
        base_models=base_models,
        bucket_url=bucket_url,
    )
    print(response)
