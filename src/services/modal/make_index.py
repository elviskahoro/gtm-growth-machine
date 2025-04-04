# trunk-ignore-all(ruff/PGH003,trunk/ignore-does-nothing)
from __future__ import annotations

import time
from typing import TYPE_CHECKING

import modal
from modal import Image
from src.services.lancedb.setup import init_client

if TYPE_CHECKING:

    from lancedb.db import DBConnection
    from lancedb.table import Table
    from pydantic import BaseModel


# trunk-ignore-begin(ruff/F401,pyright/reportUnusedImport)
from src.services.fathom.transcript.etl.webhook import (
    Webhook as FathomTranscriptWebhookModel,
)
from src.services.octolens.mention.etl.webhook import (
    Webhook as OctolensMentionsWebhookModel,
)

# trunk-ignore-end(ruff/F401,pyright/reportUnusedImport)


class WebhookModel(FathomTranscriptWebhookModel):  # type: ignore # trunk-ignore(ruff/F821)
    pass


WebhookModel.model_rebuild()

LANCEDB_PROJECT_NAME: str = WebhookModel.lance_get_project_name()
GEMINI_EMBED_BATCH_SIZE: int = 50

image: Image = modal.Image.debian_slim().pip_install(
    "fastapi[standard]",
    "lancedb",
)
image.add_local_python_source(
    *[
        "src",
    ],
)
app = modal.App(
    name="make_index",
    image=image,
)


@app.local_entrypoint()
def local() -> None:
    base_model_type: type[BaseModel] = WebhookModel.lance_get_base_model_type()
    db: DBConnection = init_client(
        project_name=base_model_type.lance_get_project_name(),
    )
    tbl: Table = db.open_table(
        name=base_model_type.lance_get_table_name(),
    )
    # bitmap
    bitmap = [
        "url",
        "date",
        "speaker",
        "organization",
    ]
    for column in bitmap:
        tbl.create_scalar_index(
            column=column,
            index_type="BITMAP",
        )
        print(f"Creating index for column: {column}")
        time.sleep(2)

    print("Successfully made index")

    del bitmap
