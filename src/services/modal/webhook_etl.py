# trunk-ignore-all(ruff/PGH003,trunk/ignore-does-nothing)
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import modal
from modal import Image
from src.services.dlt.destination_type import DestinationType
from src.services.dlt.filesystem_gcp import gcp_clean_bucket_name, to_filesystem
from src.services.local.filesystem import DestinationFileData, SourceFileData

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pydantic import BaseModel


# trunk-ignore-begin(ruff/F401,ruff/I001,pyright/reportUnusedImport)
# fmt: off
from src.services.fathom.etl.message import (
    Webhook as FathomMessageWebhook,
)
from src.services.fathom.etl.call import (
    Webhook as FathomCallWebhook,
)
from src.services.octolens.etl import (
    Webhook as OctolensWebhook,
)
# fmt: on
# trunk-ignore-end(ruff/F401,ruff/I001,pyright/reportUnusedImport)


class WebhookModel(Webhook):  # type: ignore # trunk-ignore(ruff/F821)
    pass


WebhookModel.model_rebuild()

BUCKET_NAME: str = WebhookModel.etl_get_bucket_name()

image: Image = modal.Image.debian_slim().uv_pip_install(
    "fastapi[standard]",
    "flatsplode",
    "gcsfs",  # https://github.com/fsspec/gcsfs
    "orjson",
    "pyarrow",
    "uuid7",
)
image = image.add_local_python_source(
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

VOLUME: modal.Volume = modal.Volume.from_name(
    BUCKET_NAME,
    create_if_missing=True,
)


@app.function(
    volumes={
        f"/{BUCKET_NAME}": VOLUME,
    },
)
def _get_data_from_storage_remote() -> str:
    path: Path = Path(f"/{BUCKET_NAME}/storage.json")
    if not path.exists():
        error: str = "File not found in the volume"
        raise FileNotFoundError(error)

    return path.read_text()


def _get_storage_source_file_data(
    local_storage_path: str | None,
) -> SourceFileData | None:
    base_model_type: type[BaseModel] | None = WebhookModel.storage_get_base_model_type()
    if base_model_type is None:
        return None

    if local_storage_path is not None:
        return SourceFileData.from_local_storage_path(
            local_storage_path=local_storage_path,
            base_model_type=base_model_type,
        )

    return SourceFileData.from_json_data(
        json_data=_get_data_from_storage_remote.remote(),  # trunk-ignore(pyright/reportFunctionMemberAccess)
        base_model_type=base_model_type,
    )


@app.function(
    secrets=[
        modal.Secret.from_name(
            name=name,
        )
        for name in WebhookModel.modal_get_secret_collection_names()
    ],
    region="us-east-1",
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
    storage_file_data: SourceFileData | None = _get_storage_source_file_data(
        local_storage_path=None,
    )
    data: Iterator[DestinationFileData] = DestinationFileData.from_source_file_data(
        source_file_data=file_data,
        bucket_url=bucket_url,
        storage=storage_file_data.base_model if storage_file_data else None,
    )
    return to_filesystem(
        destination_file_data=data,
        bucket_url=bucket_url,
    )


@app.local_entrypoint()
def local(
    input_folder: str,
    destination_type: str,
    input_path_storage: str | None = None,
) -> None:
    destination_type_enum: DestinationType = DestinationType(destination_type)
    bucket_url: str = destination_type_enum.get_bucket_url_from_bucket_name(
        bucket_name=BUCKET_NAME,
    )
    source_file_data: Iterator[SourceFileData] = SourceFileData.from_input_folder(
        input_folder=input_folder,
        base_model=WebhookModel,  # trunk-ignore(pyright/reportArgumentType)
        extension=[
            ".json",
            ".jsonl",
        ],
    )
    storage_file_data: SourceFileData | None = _get_storage_source_file_data(
        local_storage_path=input_path_storage,
    )
    destination_file_data: Iterator[DestinationFileData] = (
        DestinationFileData.from_source_file_data(
            source_file_data=source_file_data,
            bucket_url=bucket_url,
            storage=storage_file_data.base_model if storage_file_data else None,
        )
    )
    response: str = to_filesystem(
        destination_file_data=destination_file_data,
        bucket_url=bucket_url,
    )
    print(response)
