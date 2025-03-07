from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

import dlt
import modal
from dlt.destinations import filesystem
from modal import Image

if TYPE_CHECKING:
    from pydantic import BaseModel

from src.services.octolens import Mention

BASE_MODEL: type[BaseModel] = Mention

DLT_DESTINATION_NAME: str = "dlt_octolens_mentions"
DLT_DESTINATION_URL_GCP: str = "gs://chalk-ai-devx-dlt-octolens-mentions"

DLT_DESTINATION_URL_FILESYSTEM_RELATIVE_TO_CWD: str = f"out/{DLT_DESTINATION_NAME}"

MODAL_SECRET_COLLECTION_NAME: str = "devx-growth-gcp"

image: Image = modal.Image.debian_slim().pip_install(
    "fastapi[standard]",
    "dlt>=1.8.0",
    "dlt[gs]",
    "python-dotenv",
)
image.add_local_python_source(
    *[
        "data",
        "out",
        "src",
    ],
)
app = modal.App(
    name=DLT_DESTINATION_NAME,
    image=image,
)


def to_filesystem(
    base_models: list[BaseModel],
    bucket_url: str,
    destination_name: str,
) -> str:
    # Needed to keep the data as a json and not .gz
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = str(True)
    pipeline = dlt.pipeline(
        pipeline_name=DLT_DESTINATION_NAME,
        destination=filesystem(
            bucket_url=bucket_url,
            destination_name=destination_name,
        ),
    )
    dlt_resource = dlt.resource(
        base_models,
        name=DLT_DESTINATION_NAME,
    )
    return pipeline.run(
        data=dlt_resource,
        loader_file_format="jsonl",
    ).asstr()


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
    data: Mention,  # DEVX: Change this BaseModel if you're bootstrapping a new pipeline
) -> str:

    project_id: str | None = os.environ.get(
        "GCP_PROJECT_ID",
        None,
    )
    private_key: str | None = os.environ.get(
        "GCP_PRIVATE_KEY",
        None,
    )
    if private_key:
        private_key = private_key.replace("\\n", "\n")

    client_email: str | None = os.environ.get(
        "GCP_CLIENT_EMAIL",
        None,
    )
    if project_id is None or private_key is None or client_email is None:
        raise ValueError(
            "GCP_PROJECT_ID, GCP_PRIVATE_KEY, and GCP_CLIENT_EMAIL must be set"
        )

    os.environ["DESTINATION__CREDENTIALS__PROJECT_ID"] = project_id
    os.environ["DESTINATION__CREDENTIALS__PRIVATE_KEY"] = private_key
    os.environ["DESTINATION__CREDENTIALS__CLIENT_EMAIL"] = client_email
    response: str = to_filesystem(
        base_models=[data],
        bucket_url=DLT_DESTINATION_URL_GCP,
        destination_name=DLT_DESTINATION_NAME,
    )
    return response


def local_paths(
    input_file: str,
) -> tuple[
    Path,
    Path,
]:
    cwd: str = str(Path.cwd())
    input_file_path: Path = Path(f"{cwd}{input_file}")
    print(f"File path: {input_file_path}")
    if not input_file_path.is_file():
        raise AssertionError(f"File {input_file_path} does not exist")

    output_file_path: Path = Path(
        f"{cwd}{DLT_DESTINATION_URL_FILESYSTEM_RELATIVE_TO_CWD}",
    )
    return input_file_path, output_file_path


class TestDestination(str, Enum):
    LOCAL_FILESYSTEM = "local_filesystem"
    GCS = "gcs"


@app.local_entrypoint()
def local(
    input_file: str,
    destination: str,
) -> None:
    input_file_path: Path
    output_file_path: Path
    input_file_path, output_file_path = local_paths(
        input_file=input_file,
    )

    bucket_url: str
    destination_name: str
    match destination:
        case TestDestination(destination):
            bucket_url = str(output_file_path)
            destination_name = "local_filesystem"

        case TestDestination.GCS:
            bucket_url = DLT_DESTINATION_URL_GCP
            destination_name = DLT_DESTINATION_NAME

        case _:
            error_msg: str = f"Invalid destination: {destination}"
            raise ValueError(error_msg)

    base_model: Mention = Mention.model_validate_json(
        json_data=input_file_path.read_text(),
    )
    response: str = to_filesystem(
        base_models=[base_model],
        bucket_url=bucket_url,
        destination_name=destination_name,
    )
    print("--- response ---")
    print(response)
