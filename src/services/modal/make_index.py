# trunk-ignore-all(ruff/PGH003,trunk/ignore-does-nothing)
from __future__ import annotations

import modal
from modal import Image

# trunk-ignore-begin(ruff/F401,pyright/reportUnusedImport)
from src.services.fathom.transcript.etl.webhook import (
    Webhook as FathomTranscriptWebhookModel,
)
from src.services.lancedb.index import make_index_in_lance
from src.services.octolens.mention.etl.webhook import (
    Webhook as OctolensMentionsWebhookModel,
)

# trunk-ignore-end(ruff/F401,pyright/reportUnusedImport)


class WebhookModel(WebhookModelToReplace):  # type: ignore # trunk-ignore(ruff/F821)
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
    make_index_in_lance(
        base_model_type=WebhookModel.lance_get_base_model_type(),
    )
    print("Successfully made index")
