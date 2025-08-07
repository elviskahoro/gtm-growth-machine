# trunk-ignore-all(ruff/PGH003,trunk/ignore-does-nothing)

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import modal
from modal import Image

if TYPE_CHECKING:
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


class WebhookModel(WebhookModel):  # type: ignore # trunk-ignore(ruff/F821)
    pass


WebhookModel.model_rebuild()

BUCKET_NAME: str = WebhookModel.etl_get_bucket_name()

image: Image = modal.Image.debian_slim().pip_install(
    "fastapi[standard]",
    "pydantic[email]",
    "pyarrow",
)
image = image.add_local_python_source(
    *[
        "src",
    ],
)
app = modal.App(
    name=WebhookModel.storage_get_app_name(),
    image=image,
)

volume: modal.Volume = modal.Volume.from_name(
    BUCKET_NAME,
    create_if_missing=True,
)


@app.function(
    volumes={
        f"/{BUCKET_NAME}": volume,
    },
)
def export_model_to_storage(
    storage_model: BaseModel,
) -> None:
    model_json: str = storage_model.model_dump_json()
    print(model_json)
    path: Path = Path(
        f"/{WebhookModel.etl_get_bucket_name()}/storage.json",
    )
    path.write_text(model_json)
    print(f"Exported model to {path}")
    volume.commit()
    print("Committed volume changes")


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
    max_inputs=1000,
)
def web(
    webhook: dict,
) -> str:
    if not webhook.etl_is_valid_webhook():
        return webhook.etl_get_invalid_webhook_error_msg()

    print("Processing webhook data")
    print(dict)
    storage_model: (
        BaseModel
    ) = WebhookModel.storage_get_base_model_type().model_validate(
        obj=webhook,
    )
    export_model_to_storage.remote(  # trunk-ignore(pyright/reportFunctionMemberAccess)
        storage_model=storage_model,
    )
    return "Successfully updated storage"


@app.local_entrypoint()
def local(
    input_file: str,
) -> None:
    cwd: str = str(Path.cwd())
    path: Path = Path(f"{cwd}/{input_file}")
    print(f"Processing file: {path}")
    storage_model: (
        BaseModel
    ) = WebhookModel.storage_get_base_model_type().model_validate_json(
        json_data=path.read_text(),
    )
    export_model_to_storage.remote(  # trunk-ignore(pyright/reportFunctionMemberAccess)
        storage_model=storage_model,
    )
    print(f"Successfully updated storage with {input_file} data")
