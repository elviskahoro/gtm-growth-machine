# trunk-ignore-all(ruff/PLW0603)
from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import gcsfs
import orjson
from pydantic import ValidationError
from uuid_extensions import uuid7

import modal
from modal import Image
from src.services.local.filesystem import get_paths
from src.services.modal.filesystem import convert_bucket_url_to_pipeline_name
from src.services.modal.local_entrypoint import (
    DestinationType,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

DLT_DESTINATION_URL_GCP: str = "gs://chalk-ai-devx-octolens-mentions-raw"
DEVX_PIPELINE_NAME: str = convert_bucket_url_to_pipeline_name(
    DLT_DESTINATION_URL_GCP,
)
MODAL_SECRET_COLLECTION_NAME: str = "devx-growth-gcp"  # trunk-ignore(ruff/S105)


image: Image = modal.Image.debian_slim().pip_install(
    "fastapi[standard]",
    "gcsfs",  # https://github.com/fsspec/gcsfs
    "orjson",
    "uuid7",
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

gcp_project_id: str | None = None
gcp_private_key: str | None = None
gcp_client_email: str | None = None


def _get_env_vars() -> None:
    global gcp_project_id
    global gcp_private_key
    global gcp_client_email
    gcp_project_id = os.environ.get(
        "GCP_PROJECT_ID",
        None,
    )
    gcp_private_key = os.environ.get(
        "GCP_PRIVATE_KEY",
        None,
    )
    if gcp_private_key:
        gcp_private_key = gcp_private_key.replace(
            "\\n",
            "\n",
        )

    gcp_client_email = os.environ.get(
        "GCP_CLIENT_EMAIL",
        None,
    )


def _stream_read_json_as_string(
    path: Path,
) -> str:
    with path.open(
        mode="r",
        encoding="utf-8",
    ) as f_in:
        return "".join(line.strip() for line in f_in)


def _get_data_from_input_folder(
    input_folder: str,
) -> list[str]:
    paths: Iterator[Path] = get_paths(
        input_folder=input_folder,
        extension=".json",
    )
    data: list[str] = []
    current_path: Path | None = None
    try:
        path: Path
        for path in paths:
            current_path = path
            data.append(
                _stream_read_json_as_string(
                    path=path,
                ),
            )

    except ValidationError as e:
        print(e)
        print(current_path)
        raise

    return data


def _to_filesystem_local(
    data: zip[tuple[str, str]],
) -> None:
    json: str
    output_path_str: str
    for count, (json, output_path_str) in enumerate(
        data,
        start=1,
    ):
        print(f"{count:06d}: {output_path_str}")
        output_path: Path = Path(output_path_str)
        with output_path.open(
            mode="w+",
        ) as f:
            f.write(
                json,
            )


def _to_filesystem_gcs(
    data: zip[tuple[str, str]],
) -> None:
    if gcp_project_id is None or gcp_private_key is None or gcp_client_email is None:
        error_msg: str = (
            "GCP_PROJECT_ID, GCP_PRIVATE_KEY, and GCP_CLIENT_EMAIL must be set"
        )
        raise ValueError(
            error_msg,
        )

    fs: gcsfs.GCSFileSystem = gcsfs.GCSFileSystem(
        project=gcp_project_id,
        token={
            "client_email": gcp_client_email,
            "private_key": gcp_private_key,
            "project_id": gcp_project_id,
            "token_uri": "https://oauth2.googleapis.com/token",
        },
    )
    json: str
    output_path: str
    for count, (json, output_path) in enumerate(
        data,
        start=1,
    ):
        print(f"{count:06d}: {output_path}")
        with fs.open(
            path=output_path,
            mode="w",
        ) as f:
            f.write(
                json,
            )


def to_filesystem(
    jsons: list[str],
    bucket_url: str = DLT_DESTINATION_URL_GCP,
) -> str:
    output_paths: Iterator[str] = (
        bucket_url + "/" + str(uuid7()) + ".jsonl" for _ in range(len(jsons))
    )
    match bucket_url:
        case str() as url if url.startswith("gs://"):
            _to_filesystem_gcs(
                data=zip(jsons, output_paths),
            )

        case _:
            bucket_url_path: Path = Path(bucket_url)
            bucket_url_path.mkdir(
                parents=True,
                exist_ok=True,
            )
            _to_filesystem_local(
                data=zip(jsons, output_paths),
            )

    return "Successfully uploaded all mentions"


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
    data: dict,
) -> str:
    _get_env_vars()
    jsons: str = orjson.dumps(data).decode("utf-8")
    response: str = to_filesystem(
        jsons=[jsons],
        bucket_url=DLT_DESTINATION_URL_GCP,
    )
    return response


@app.local_entrypoint()
def local(
    input_folder: str,
    destination_type: str,
) -> None:
    bucket_url: str
    destination_type_enum: DestinationType = DestinationType(destination_type)
    match destination_type_enum:
        case DestinationType.LOCAL:
            bucket_url = DestinationType.get_bucket_url_for_local(
                pipeline_name=DEVX_PIPELINE_NAME,
            )

        case DestinationType.GCP:
            bucket_url = DLT_DESTINATION_URL_GCP
            _get_env_vars()

        case _:
            error_msg: str = f"Invalid destination type: {destination_type_enum}"
            raise ValueError(error_msg)

    data: list[str] = _get_data_from_input_folder(
        input_folder=input_folder,
    )
    print(f"Exporting {len(data)} mentions to {bucket_url}")
    response: str = to_filesystem(
        jsons=data,
        bucket_url=bucket_url,
    )
    print(response)
