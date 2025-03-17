from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import orjson
from pydantic import ValidationError
from uuid_extensions import uuid7

import modal
from modal import Image
from src.services.dlt.destination_type import (
    DestinationType,
)
from src.services.dlt.filesystem import (
    convert_bucket_url_to_pipeline_name,
    to_filesystem_gcs,
    to_filesystem_local,
)
from src.services.local.filesystem import get_paths

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


def to_filesystem(
    jsons: list[str],
    bucket_url: str = DLT_DESTINATION_URL_GCP,
) -> str:
    output_paths: Iterator[str] = (
        bucket_url + "/" + str(uuid7()) + ".jsonl" for _ in range(len(jsons))
    )
    match bucket_url:
        case str() as url if url.startswith("gs://"):
            to_filesystem_gcs(
                data=zip(jsons, output_paths),
            )

        case _:
            bucket_url_path: Path = Path(bucket_url)
            bucket_url_path.mkdir(
                parents=True,
                exist_ok=True,
            )
            to_filesystem_local(
                data=zip(jsons, output_paths),
            )

    return "Successfully uploaded"


@app.function(
    secrets=[
        modal.Secret.from_name(
            name=MODAL_SECRET_COLLECTION_NAME,
        ),
    ],
    region="us-east4",  # This feature is available on the Team and Enterprise plans, read more at https://modal.com/docs/guide/region-selection
    allow_concurrent_inputs=1000,
    enable_memory_snapshot=True,
)
@modal.web_endpoint(
    method="POST",
    docs=True,
)
def web(
    json_data: dict,
) -> str:
    json: str = orjson.dumps(json_data).decode("utf-8")
    response: str = to_filesystem(
        jsons=[json],
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

        case _:
            error_msg: str = f"Invalid destination type: {destination_type_enum}"
            raise ValueError(error_msg)

    jsons: list[str] = _get_data_from_input_folder(
        input_folder=input_folder,
    )
    print(f"Exporting {len(jsons)} webhooks to {bucket_url}")
    response: str = to_filesystem(
        jsons=jsons,
        bucket_url=bucket_url,
    )
    print(response)
