from __future__ import annotations

import os

import dlt
import modal
from dlt.destinations import filesystem
from modal import Image

from src.services.modal.local_entrypoint import (
    DestinationType,
    get_data_from_input_folder,
)
from src.services.octolens import Mention

DEVX_PIPELINE_NAME: str = "octolens_mentions_dlt"
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
    name=DEVX_PIPELINE_NAME,
    image=image,
)


def _set_env_vars() -> None:
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
        pipeline_name=DEVX_PIPELINE_NAME,
        destination=filesystem(
            bucket_url=bucket_url,
            destination_name=DEVX_PIPELINE_NAME,
        ),
    )
    dlt_resource = dlt.resource(
        base_models,
        name=DEVX_PIPELINE_NAME,
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
    _set_env_vars()
    response: str = to_filesystem(
        base_models=[data],
        bucket_url=DLT_DESTINATION_URL_GCP,
    )
    return response


@app.local_entrypoint()
def local(
    input_folder: str,
    destination_type: str,
) -> None:
    destination_type_enum: DestinationType = DestinationType(destination_type)
    bucket_url: str
    match destination_type_enum:
        case DestinationType.LOCAL:
            bucket_url = DestinationType.get_bucket_url_for_local(
                pipeline_name=DEVX_PIPELINE_NAME,
            )

        case DestinationType.GCP:
            bucket_url = DLT_DESTINATION_URL_GCP

        case _:
            error_msg: str = f"Invalid destination type: {destination_type_enum}"
            raise ValueError(error_msg)

    mentions: list[Mention] = (  # trunk-ignore(pyright/reportAssignmentType)
        get_data_from_input_folder(
            input_folder=input_folder,
            base_model=Mention,  # trunk-ignore(pyright/reportArgumentType)
        )
    )
    print(f"Exporting {len(mentions)} mentions to {bucket_url}")
    response: str = to_filesystem(
        base_models=mentions,
        bucket_url=bucket_url,
    )
    print(response)
