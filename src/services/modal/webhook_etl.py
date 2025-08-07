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
from src.services.fathom.etl import (
    Webhook as FathomWebhookModel,
)
from src.services.octolens.etl import (
    Webhook as OctolensWebhookModel,
)
# fmt: on
# trunk-ignore-end(ruff/F401,ruff/I001,pyright/reportUnusedImport)


class WebhookModel(FathomWebhookModel):  # type: ignore # trunk-ignore(ruff/F821)
    pass


WebhookModel.model_rebuild()

BUCKET_NAME: str = WebhookModel.etl_get_bucket_name()

image: Image = modal.Image.debian_slim().uv_pip_install(
    "fastapi[standard]",
    "gcsfs",  # https://github.com/fsspec/gcsfs
    "uuid7",
    "pyarrow",
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

volume: modal.Volume = modal.Volume.from_name(
    BUCKET_NAME,
    create_if_missing=False,
)


def _get_data_storage_local(
    input_path_storage: str,
) -> str:
    cwd: str = str(Path.cwd())
    path: Path = Path(f"{cwd}/{input_path_storage}")
    if not path.exists():
        error: str = f"File not found at {path}"
        raise FileNotFoundError(error)

    json_data: str = path.read_text()
    if not json_data:
        error: str = "File is empty"
        raise ValueError(error)

    return json_data


@app.function(
    volumes={
        f"/{BUCKET_NAME}": volume,
    },
)
def _get_data_from_storage_remote() -> str:
    path: Path = Path(f"/{BUCKET_NAME}/storage.json")
    if not path.exists():
        error: str = "File not found in the volume"
        raise FileNotFoundError(error)

    return path.read_text()


def get_storage(input_path_storage: str | None) -> BaseModel | None:
    storage_base_model_type: type[BaseModel] | None = (
        WebhookModel.storage_get_base_model_type()
    )
    if (  # trunk-ignore(pyright/reportUnnecessaryComparison)
        storage_base_model_type is None
    ):
        return None

    def get_json_data() -> str:
        if input_path_storage is not None:
            return _get_data_storage_local(
                input_path_storage=input_path_storage,
            )

        return (
            _get_data_from_storage_remote.remote()  # trunk-ignore(pyright/reportFunctionMemberAccess)
        )

    return storage_base_model_type.model_validate_json(
        json_data=get_json_data(),
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
    storage: BaseModel | None = get_storage(input_path_storage=None)
    data: Iterator[DestinationFileData] = DestinationFileData.from_source_file_data(
        source_file_data=file_data,
        bucket_url=bucket_url,
        storage=storage,
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
    storage: BaseModel | None = get_storage(input_path_storage=input_path_storage)
    destination_file_data: Iterator[DestinationFileData] = (
        DestinationFileData.from_source_file_data(
            source_file_data=source_file_data,
            bucket_url=bucket_url,
            storage=storage,
        )
    )
    response: str = to_filesystem(
        destination_file_data=destination_file_data,
        bucket_url=bucket_url,
    )
    print(response)
