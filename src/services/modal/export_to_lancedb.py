# trunk-ignore-all(ruff/PGH003,trunk/ignore-does-nothing)
from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING

import modal
from modal import Image
from src.services.gemini.embed import embed_with_gemini
from src.services.lancedb.upload import upload_to_lance
from src.services.local.filesystem import SourceFileData

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pydantic import BaseModel

# trunk-ignore-begin(ruff/F401,ruff/I001,pyright/reportUnusedImport)
# fmt: off
from src.services.fathom.etl import (
    Webhook as FathomTranscriptWebhookModel,
)
from src.services.octolens.etl import (
    Webhook as OctolensMentionsWebhookModel,
)
# fmt: on
# trunk-ignore-end(ruff/F401,ruff/I001,pyright/reportUnusedImport)


class WebhookModel(WebhookModel):  # type: ignore # trunk-ignore(ruff/F821)
    pass


WebhookModel.model_rebuild()

LANCEDB_PROJECT_NAME: str = WebhookModel.lance_get_project_name()
GEMINI_EMBED_BATCH_SIZE: int = 100

image: Image = modal.Image.debian_slim().pip_install(
    "fastapi[standard]",
    "google-cloud-aiplatform",
    "lancedb",
    "pandas",
    "pyarrow",
)
image.add_local_python_source(
    *[
        "src",
    ],
)
app = modal.App(
    name=LANCEDB_PROJECT_NAME,
    image=image,
)


def embed_with_gemini_and_upload_to_lance(
    source_file_data: Iterator[SourceFileData],
) -> str:
    chain_base_models_to_embed: chain[list[BaseModel]] = chain(
        list(source_file_data.base_model.etl_get_base_models())
        for source_file_data in source_file_data
    )
    print(f"Batch size {GEMINI_EMBED_BATCH_SIZE:04d}")

    count: int = 0
    base_models_to_embed: list[BaseModel]
    for base_models_to_embed in chain_base_models_to_embed:
        print(f"{len(base_models_to_embed):07d} Embeded with Gemini")
        for base_models_to_upload in embed_with_gemini(
            base_models_to_embed=iter(base_models_to_embed),
            batch_size=GEMINI_EMBED_BATCH_SIZE,
        ):
            upload_to_lance(
                base_models_to_upload=base_models_to_upload,
                base_model_type=WebhookModel.lance_get_base_model_type(),
            )
            count += 1
            print(f"{count:07d} Uploaded to LanceDB")

    return "Successfully embeded with Gemini and uploaded to LanceDB."


@app.function(
    secrets=[
        modal.Secret.from_name(
            name=name,
        )
        for name in WebhookModel.modal_get_secret_collection_names()
    ],
    allow_concurrent_inputs=1000,
    enable_memory_snapshot=False,
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

    source_file_data: Iterator[SourceFileData] = iter(
        [
            SourceFileData(
                path=None,
                base_model=webhook,
            ),
        ],
    )
    return embed_with_gemini_and_upload_to_lance(
        source_file_data=source_file_data,
    )


@app.local_entrypoint()
def local(
    input_folder: str,
) -> None:
    source_file_data: Iterator[SourceFileData] = SourceFileData.from_input_folder(
        input_folder=input_folder,
        base_model=WebhookModel,  # trunk-ignore(pyright/reportArgumentType)
        extension=[
            ".json",
            ".jsonl",
        ],
    )
    response: str = embed_with_gemini_and_upload_to_lance(
        source_file_data=source_file_data,
    )
    print(response)
