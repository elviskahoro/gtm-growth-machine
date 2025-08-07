# trunk-ignore-all(ruff/PGH003,trunk/ignore-does-nothing)
from __future__ import annotations

import time
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING

import modal
from modal import Image
from src.services.gemini.embed import (
    embed_with_gemini,
    init_client as gemini_init_client,
)
from src.services.lance.setup import init_client as lance_init_client
from src.services.lance.upload import upload_to_lance
from src.services.local.filesystem import SourceFileData

if TYPE_CHECKING:
    from collections.abc import Iterator

    from lancedb.db import DBConnection
    from pydantic import BaseModel

# trunk-ignore-begin(ruff/F401,ruff/I001,pyright/reportUnusedImport)
# fmt: off
from src.services.fathom.etl.message import (
    Webhook as FathomMessageWebhook,
)
from src.services.octolens.etl import (
    Webhook as OctolensWebhook,
)
# fmt: on
# trunk-ignore-end(ruff/F401,ruff/I001,pyright/reportUnusedImport)


class WebhookModel(FathomMessageWebhook):  # type: ignore # trunk-ignore(ruff/F821)
    pass


WebhookModel.model_rebuild()

BUCKET_NAME: str = WebhookModel.etl_get_bucket_name()

GEMINI_EMBED_BATCH_SIZE: int = 250  # Maximum allowed by Vertex AI text-embedding-005
UPLOAD_DELAY_SECONDS: float = 0.1  # Delay between uploads to prevent rate limiting
MAX_RETRY_ATTEMPTS: int = 5  # Maximum retry attempts for rate limited uploads

image: Image = modal.Image.debian_slim().uv_pip_install(
    "fastapi[standard]",
    "google-cloud-aiplatform",
    "lancedb",
    "pyarrow",
)
image = image.add_local_python_source(
    *[
        "src",
    ],
)
app = modal.App(
    name=WebhookModel.lance_get_project_name(),
    image=image,
)

VOLUME: modal.Volume = modal.Volume.from_name(
    BUCKET_NAME,
    create_if_missing=False,
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
    if local_storage_path is not None:
        return SourceFileData.from_local_storage_path(
            local_storage_path=local_storage_path,
            base_model_type=WebhookModel.storage_get_base_model_type(),
        )

    return SourceFileData.from_json_data(
        json_data=_get_data_from_storage_remote.remote(),  # trunk-ignore(pyright/reportFunctionMemberAccess)
        base_model_type=WebhookModel.storage_get_base_model_type(),
    )


def embed_with_gemini_and_upload_to_lance(
    source_file_data: Iterator[SourceFileData],
    embed_batch_size: int,
    storage: BaseModel | None,
    db: DBConnection,
    upload_delay: float = UPLOAD_DELAY_SECONDS,
) -> str:
    base_model_type: type[BaseModel] = WebhookModel.lance_get_base_model_type()
    table_name: str = WebhookModel.lance_get_table_name()
    primary_key: str = WebhookModel.lance_get_primary_key()
    primary_key_index_type: str = WebhookModel.lance_get_primary_key_index_type()

    gemini_init_client()
    chain_base_models_to_embed: chain[list[BaseModel]] = chain(
        list(
            source_file_data.base_model.etl_get_base_models(
                storage=storage,
            ),
        )
        for source_file_data in source_file_data
    )
    print(f"Batch size {embed_batch_size:04d}")

    count: int = 0
    base_models_to_embed: list[BaseModel]
    for base_models_to_embed in chain_base_models_to_embed:
        print(f"{len(base_models_to_embed):07d} Embeded with Gemini")
        for data_to_upload in embed_with_gemini(
            base_models_to_embed=iter(base_models_to_embed),
            embed_batch_size=embed_batch_size,
        ):
            upload_to_lance(
                data_to_upload=data_to_upload,
                base_model_type=base_model_type,
                db=db,
                table_name=table_name,
                primary_key=primary_key,
                primary_key_index_type=primary_key_index_type,
            )
            count += 1
            print(f"{count:07d} Uploaded to LanceDB")

            # Add a configurable delay between uploads to prevent rate limiting
            if upload_delay > 0:
                time.sleep(upload_delay)

    return "Successfully embeded with Gemini and uploaded to LanceDB."


@app.function(
    secrets=[
        modal.Secret.from_name(
            name=name,
        )
        for name in WebhookModel.modal_get_secret_collection_names()
    ],
    enable_memory_snapshot=False,
)
@modal.fastapi_endpoint(
    method="POST",
    docs=True,
)
@modal.concurrent(
    max_inputs=5,  # Balanced concurrency for web endpoints
)
def web(
    webhook: WebhookModel,
) -> str:
    if not webhook.etl_is_valid_webhook():
        return webhook.etl_get_invalid_webhook_error_msg()

    source_file_data: Iterator[SourceFileData] = iter(
        [
            SourceFileData(
                path=None,
                base_model=webhook,
            ),
        ],
    )
    storage_file_data: SourceFileData | None = _get_storage_source_file_data(
        local_storage_path=None,
    )
    return embed_with_gemini_and_upload_to_lance(
        source_file_data=source_file_data,
        storage=storage_file_data.base_model if storage_file_data else None,
        embed_batch_size=GEMINI_EMBED_BATCH_SIZE,
        db=lance_init_client(
            project_name=WebhookModel.lance_get_project_name(),
        ),
        upload_delay=UPLOAD_DELAY_SECONDS,
    )


@app.local_entrypoint()
def local(
    input_folder: str,
    embed_batch_size: int = GEMINI_EMBED_BATCH_SIZE,
    upload_delay: float = UPLOAD_DELAY_SECONDS,
) -> None:
    source_file_data: Iterator[SourceFileData] = SourceFileData.from_input_folder(
        input_folder=input_folder,
        base_model=WebhookModel,  # trunk-ignore(pyright/reportArgumentType)
        extension=[
            ".json",
            ".jsonl",
        ],
    )
    storage_file_data: SourceFileData | None = _get_storage_source_file_data(
        local_storage_path=None,
    )
    response: str = embed_with_gemini_and_upload_to_lance(
        source_file_data=source_file_data,
        storage=storage_file_data.base_model if storage_file_data else None,
        embed_batch_size=embed_batch_size,
        db=lance_init_client(
            project_name=WebhookModel.lance_get_project_name(),
        ),
        upload_delay=upload_delay,
    )
    print(response)
