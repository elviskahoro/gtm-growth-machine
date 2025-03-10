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
    from collections.abc import Iterator

    from pydantic import BaseModel

from pydantic import ValidationError

from src.services.octolens import Mention

BASE_MODEL: type[BaseModel] = Mention

PIPELINE_NAME: str = "octolens_mentions_dlt"
DLT_DESTINATION_URL_GCP: str = "gs://chalk-ai-devx-octolens-mentions-dlt"

DLT_DESTINATION_URL_FILESYSTEM_RELATIVE_TO_CWD: str = f"out/{PIPELINE_NAME}"

MODAL_SECRET_COLLECTION_NAME: str = "devx-growth-gcp"

image: Image = modal.Image.debian_slim().pip_install(
    "fastapi[standard]",
    "dlt>=1.8.0",
    "dlt[gs]",  # https://github.com/fsspec/gcsfs
    "python-dotenv",
)
image.add_local_python_source(
    *[
        "src",
    ],
)
app = modal.App(
    name=PIPELINE_NAME,
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
        pipeline_name=PIPELINE_NAME,
        destination=filesystem(
            bucket_url=bucket_url,
            destination_name=destination_name,
        ),
    )
    dlt_resource = dlt.resource(
        base_models,
        name=PIPELINE_NAME,
        write_disposition="merge",
    )
    return pipeline.run(
        data=dlt_resource,
        loader_file_format="jsonl",
    ).asstr()


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
        raise ValueError(
            "GCP_PROJECT_ID, GCP_PRIVATE_KEY, and GCP_CLIENT_EMAIL must be set"
        )

    os.environ["DESTINATION__CREDENTIALS__PROJECT_ID"] = project_id
    os.environ["DESTINATION__CREDENTIALS__PRIVATE_KEY"] = private_key
    os.environ["DESTINATION__CREDENTIALS__CLIENT_EMAIL"] = client_email


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
    set_env_vars()
    response: str = to_filesystem(
        base_models=[data],
        bucket_url=DLT_DESTINATION_URL_GCP,
        destination_name=PIPELINE_NAME,
    )
    return response


def get_paths(
    input_folder: str,
) -> Iterator[Path]:
    cwd: str = str(Path.cwd())
    input_folder_path: Path = Path(f"{cwd}/{input_folder}")
    if not input_folder_path.exists() or not input_folder_path.is_dir():
        raise AssertionError(f"Input folder '{input_folder_path}' does not exist")

    return (f for f in input_folder_path.iterdir() if f.is_file())


def ensure_output_dir(
    output_dir: str,
) -> Path:
    cwd: str = str(Path.cwd())
    output_path: Path = Path(f"{cwd}/{output_dir}")
    output_path.mkdir(
        parents=True,
        exist_ok=True,
    )
    return output_path


class TestDestination(str, Enum):
    LOCAL = "local"
    GCP = "gcp"


@app.local_entrypoint()
def local(
    input_folder: str,
    destination: str,
) -> None:
    bucket_url: str
    destination_name: str
    match destination:
        case TestDestination.LOCAL:
            cwd: str = str(Path.cwd())
            bucket_url = f"{cwd}/{DLT_DESTINATION_URL_FILESYSTEM_RELATIVE_TO_CWD}"
            destination_name = "local_filesystem"

        case TestDestination.GCP:
            bucket_url = DLT_DESTINATION_URL_GCP
            destination_name = PIPELINE_NAME

        case _:
            error_msg: str = f"Invalid destination: {destination}"
            raise ValueError(error_msg)

    paths: Iterator[Path] = get_paths(input_folder)
    base_models: list[BaseModel] = []
    current_path: Path | None = None
    try:
        path: Path
        for path in paths:
            current_path = path
            base_models.append(
                Mention.model_validate_json(
                    json_data=path.read_text(),
                ),
            )

    except ValidationError as e:
        print(e)
        print(current_path)
        raise

    print(len(base_models))
    response: str = to_filesystem(
        base_models=base_models,
        bucket_url=bucket_url,
        destination_name=destination_name,
    )
    print(response)
