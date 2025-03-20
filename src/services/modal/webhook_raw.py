from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

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
    to_filesystem,
)
from src.services.local.filesystem import DestinationFileData, get_paths

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

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


class SourceFileRaw(NamedTuple):
    file: Path
    content: str


def _get_data_from_input_folder(
    input_folder: str,
) -> Iterator[SourceFileRaw]:
    paths: Iterator[Path] = get_paths(
        input_folder=input_folder,
        extension=".json",
    )
    current_path: Path | None = None
    try:
        path: Path
        for path in paths:
            current_path = path
            yield SourceFileRaw(
                file=path,
                content=_stream_read_json_as_string(path),
            )

    except ValidationError as e:
        print(e)
        print(current_path)
        raise


def _get_json_data_from_file_data(
    file_data: Iterator[SourceFileRaw],
    bucket_url: str,
) -> Iterator[DestinationFileData]:
    for individual_file_data in file_data:
        try:
            yield DestinationFileData(
                json=individual_file_data.content,
                path=f"{bucket_url}/{uuid7()!s}.jsonl",
            )

        except (AttributeError, ValueError):
            error_msg: str = f"Error processing file: {individual_file_data.path}"
            print(error_msg)
            raise


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
    json: str = orjson.dumps(json_data).decode(
        encoding="utf-8",
    )
    data: Iterator[DestinationFileData] = iter(
        [
            DestinationFileData(
                json=json,
                path=f"{DLT_DESTINATION_URL_GCP}/{uuid7()!s}.jsonl",
            ),
        ],
    )
    return to_filesystem(
        data=data,
        bucket_url=DLT_DESTINATION_URL_GCP,
    )


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

    file_data: Iterator[SourceFileRaw] = _get_data_from_input_folder(
        input_folder=input_folder,
    )
    data: Iterator[DestinationFileData] = _get_json_data_from_file_data(
        file_data=file_data,
        bucket_url=bucket_url,
    )
    response: str = to_filesystem(
        data=data,
        bucket_url=bucket_url,
    )
    print(response)
