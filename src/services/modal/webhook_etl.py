# trunk-ignore-all(ruff/PGH003,trunk/ignore-does-nothing)
from __future__ import annotations

from typing import TYPE_CHECKING

import modal
from modal import Image
from src.services.dlt.destination_type import (
    DestinationType,
)
from src.services.dlt.filesystem_gcp import (
    gcp_clean_bucket_name,
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

# trunk-ignore-begin(ruff/F401,pyright/reportUnusedImport)
from src.services.fathom.transcript.etl.webhook import (
    Webhook as FathomTranscriptWebhookModel,
)
from src.services.octolens.mention.etl.webhook import (
    Webhook as OctolensMentionsWebhookModel,
)

# trunk-ignore-end(ruff/F401,pyright/reportUnusedImport)


class WebhookModel(WebhookModelToReplace):  # type: ignore # trunk-ignore(ruff/F821)
    pass


WebhookModel.model_rebuild()

BUCKET_NAME: str = WebhookModel.etl_get_bucket_name()

image: Image = modal.Image.debian_slim().uv_pip_install(
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
    name=gcp_clean_bucket_name(
        bucket_name=BUCKET_NAME,
    ),
    image=image,
)


@app.function(
    secrets=[
        modal.Secret.from_name(
            name=name,
        )
        for name in WebhookModel.modal_get_secret_collection_names()
    ],
    region="us-east4",
    enable_memory_snapshot=False,
)
@modal.fastapi_endpoint(
    method="POST",
    docs=True,
)
@modal.concurrent(
    max_inputs=1000,
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
    bucket_url: str = DestinationType.GCP.get_bucket_url_from_bucket_name(
        bucket_name=BUCKET_NAME,
    )
    data: Iterator[DestinationFileData] = (
        get_destination_file_data_from_source_file_data(
            source_file_data=file_data,
            bucket_url=bucket_url,
        )
    )
    return to_filesystem(
        destination_file_data=data,
        bucket_url=bucket_url,
    )


@app.local_entrypoint()
def local(
    input_folder: str,
    destination_type: str,
) -> None:
    destination_type_enum: DestinationType = DestinationType(destination_type)
    bucket_url: str = destination_type_enum.get_bucket_url_from_bucket_name(
        bucket_name=BUCKET_NAME,
    )

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
