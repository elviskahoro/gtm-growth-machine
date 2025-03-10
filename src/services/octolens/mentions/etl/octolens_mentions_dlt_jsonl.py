from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

import dlt
import modal
from dlt.destinations import filesystem
from modal import Image
from pydantic import ValidationError

from src.services.local.filesystem import get_paths
from src.services.octolens import Mention

if TYPE_CHECKING:
    from collections.abc import Iterator


DLT_PIPELINE_NAME: str = "octolens_mentions_dlt"
DLT_DESTINATION_URL_GCP: str = "gs://chalk-ai-devx-octolens-mentions-dlt"

MODAL_SECRET_COLLECTION_NAME: str = "devx-growth-gcp"  # trunk-ignore(ruff/S105)

image: Image = modal.Image.debian_slim().pip_install(
    "fastapi[standard]",
    "dlt>=1.8.0",
    "dlt[gs]",  # https://github.com/fsspec/gcsfs
)
image.add_local_python_source(
    *[
        "src",
    ],
)
app = modal.App(
    name=DLT_PIPELINE_NAME,
    image=image,
)


def set_env_vars() -> None:
    project_id: str | None = os.environ.get(
        "GCP_PROJECT_ID",
        None,
    )
    private_key: str | None = os.environ.get(
        "GCP_PRIVATE_KEY",
        None,
    )
    if private_key:
        private_key = private_key.replace(
            "\\n",
            "\n",
        )

    client_email: str | None = os.environ.get(
        "GCP_CLIENT_EMAIL",
        None,
    )
    if project_id is None or private_key is None or client_email is None:
        error_msg: str = (
            "GCP_PROJECT_ID, GCP_PRIVATE_KEY, and GCP_CLIENT_EMAIL must be set"
        )
        raise ValueError(
            error_msg,
        )

    os.environ["DESTINATION__CREDENTIALS__PROJECT_ID"] = project_id
    os.environ["DESTINATION__CREDENTIALS__PRIVATE_KEY"] = private_key
    os.environ["DESTINATION__CREDENTIALS__CLIENT_EMAIL"] = client_email


def to_filesystem(
    base_models: list[Mention],
    bucket_url: str,
) -> str:
    # Needed to keep the data as a json and not .gz
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = str(True)
    pipeline = dlt.pipeline(
        pipeline_name=DLT_PIPELINE_NAME,
        destination=filesystem(
            bucket_url=bucket_url,
            destination_name=DLT_PIPELINE_NAME,
        ),
    )
    dlt_resource = dlt.resource(
        base_models,
        name=DLT_PIPELINE_NAME,
        write_disposition="merge",
    )
    try:
        return pipeline.run(
            data=dlt_resource,
            loader_file_format="jsonl",
        ).asstr()

    except Exception as e:
        print(e)
        raise


@app.function(
    secrets=[
        modal.Secret.from_name(
            name=MODAL_SECRET_COLLECTION_NAME,
        ),
    ],
    # cloud="aws", This feature is available on the Team and Enterprise plans, read more at https://modal.com/docs/guide/region-selection
    # region="us-west-2", This feature is available on the Team and Enterprise plans, read more at https://modal.com/docs/guide/region-selection
    allow_concurrent_inputs=1000,
    enable_memory_snapshot=True,
)
@modal.web_endpoint(
    method="POST",
    docs=True,
)
def web(
    data: Mention,  # MODAL: Change this BaseModel if you're bootstrapping a new pipeline
) -> str:
    set_env_vars()
    response: str = to_filesystem(
        base_models=[data],
        bucket_url=DLT_DESTINATION_URL_GCP,
    )
    return response


@app.local_entrypoint()
def local(
    input_folder: str,
    destination: str,
) -> None:

    def get_bucket_url(
        destination: str,
    ) -> str:
        class TestDestination(str, Enum):
            LOCAL = "local"
            GCP = "gcp"

        match destination:
            case TestDestination.LOCAL:
                cwd: str = str(Path.cwd())
                return f"{cwd}/out/{DLT_PIPELINE_NAME}"

            case TestDestination.GCP:
                return DLT_DESTINATION_URL_GCP

            case _:
                error_msg: str = f"Invalid destination: {destination}"
                raise ValueError(error_msg)

    def get_mentions(
        input_folder: str,
    ) -> list[Mention]:
        paths: Iterator[Path] = get_paths(input_folder)
        mentions: list[Mention] = []
        current_path: Path | None = None
        try:
            path: Path
            for path in paths:
                current_path = path
                mentions.append(
                    Mention.model_validate_json(
                        json_data=path.read_text(),
                    ),
                )

        except ValidationError as e:
            print(e)
            print(current_path)
            raise

        return mentions

    mentions: list[Mention] = get_mentions(
        input_folder=input_folder,
    )
    print(len(mentions))
    response: str = to_filesystem(
        base_models=mentions,
        bucket_url=get_bucket_url(
            destination=destination,
        ),
    )
    print(response)
