from __future__ import annotations

from typing import TYPE_CHECKING

import modal
from modal import Image
from src.services.dlt.destination_type import (
    DestinationType,
)
from src.services.dlt.filesystem_gcp import (
    gcp_bucket_url_from_bucket_name,
    to_filesystem,
)
from src.services.local.filesystem import (
    DestinationFileData,
    SourceFileData,
    get_destination_file_data_from_source_file_data,
    get_source_file_data_from_input_folder,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


from src.services.octolens.mentions.etl.webhook import Webhook


class WebhookModel(Webhook): ...


BUCKET_NAME: str = "chalk-ai-devx-octolens-mentions-etl"
BUCKET_URL: str = gcp_bucket_url_from_bucket_name(
    bucket_name=BUCKET_NAME,
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
    name=BUCKET_NAME.replace("-", "_"),
    image=image,
)


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

    file_data: Iterator[SourceFileData] = iter(
        [
            SourceFileData(
                path=None,
                base_model=webhook,
            ),
        ],
    )
    data: Iterator[DestinationFileData] = (
        get_destination_file_data_from_source_file_data(
            source_file_data=file_data,
            bucket_url=BUCKET_URL,
        )
    )
    return to_filesystem(
        destination_file_data=data,
        bucket_url=BUCKET_URL,
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
            bucket_url = DestinationType.get_bucket_url_from_bucket_name_for_local(
                bucket_name=BUCKET_NAME,
            )

        case DestinationType.GCP:
            bucket_url = BUCKET_URL

        case _:
            error_msg: str = f"Invalid destination type: {destination_type_enum}"
            raise ValueError(error_msg)

    source_file_data: Iterator[SourceFileData] = get_source_file_data_from_input_folder(
        input_folder=input_folder,
        base_model=WebhookModel,  # trunk-ignore(pyright/reportArgumentType)
        extension=[
            ".json",
            ".jsonl",
        ],
    )
    destination_file_data: Iterator[DestinationFileData] = (
        get_destination_file_data_from_source_file_data(
            source_file_data=source_file_data,
            bucket_url=bucket_url,
        )
    )
    response: str = to_filesystem(
        destination_file_data=destination_file_data,
        bucket_url=bucket_url,
    )
    print(response)
