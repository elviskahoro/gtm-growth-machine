from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

import modal
from modal import Image
from pydantic import ValidationError
from uuid_extensions import uuid7

from src.services.dlt.destination_type import (
    DestinationType,
)
from src.services.dlt.filesystem_gcp import CloudGoogle
from src.services.dlt.filesystem_local import to_filesystem_local
from src.services.fathom.etl.message._srt_file import SrtFile
from src.services.fathom.etl.message.webhook import (
    Webhook,
)
from src.services.fathom.meeting import Meeting
from src.services.fathom.recording import Recording
from src.services.fathom.transcript import Transcript
from src.services.fathom.user import FathomUser
from src.services.local.filesystem import DestinationFileData, FileUtility

if TYPE_CHECKING:
    from collections.abc import Iterator

BUCKET_NAME: str = "devx-fathom-transcripts-from_srt"
BUCKET_URL: str = CloudGoogle.bucket_url_from_bucket_name(
    bucket_name=BUCKET_NAME,
)

image: Image = modal.Image.debian_slim().pip_install(
    "fastapi[standard]",
    "gcsfs",  # https://github.com/fsspec/gcsfs
    "uuid7",
)
image.add_local_python_source(
    *[
        "src",
    ],
)
app = modal.App(
    name=CloudGoogle.clean_bucket_name(
        bucket_name=BUCKET_NAME,
    ),
    image=image,
)


class FileContent(NamedTuple):
    lines: list[str]
    full_text: str


def _read_file_preserve_lines(
    path: Path,
) -> FileContent:
    with path.open(
        mode="r",
        encoding="utf-8",
    ) as f_in:
        full_text: str
        delimiter_index: int
        content_lines: list[str]
        lines: list[str] = f_in.readlines()
        try:
            delimiter_index = next(i for i, line in enumerate(lines) if "---" in line)
            delimiter_index += 1
            while not lines[delimiter_index].strip().replace("\n", ""):
                delimiter_index += 1

            content_lines = lines[delimiter_index:]
            full_text = "".join(content_lines)

        except StopIteration:
            content_lines = lines
            full_text = "".join(lines)

    return FileContent(
        lines=lines,
        full_text=full_text,
    )


def _get_data_from_input_folder(
    input_folder: str,
) -> Iterator[SrtFile]:
    paths: Iterator[Path] = FileUtility.get_paths(
        input_folder=input_folder,
        extension=[".srt"],
    )
    current_path: Path | None = None
    try:
        path: Path
        for path in paths:
            current_path = path
            file_content: FileContent = _read_file_preserve_lines(
                path=path,
            )
            if len(file_content.lines) > 0:
                yield SrtFile.from_file_content(
                    lines=file_content.lines,
                    path=path,
                    full_text=file_content.full_text,
                )

    except ValidationError as e:
        print(e)
        print(current_path)
        raise


def _to_filesystem(
    jsons: Iterator[str],
    bucket_url: str = BUCKET_URL,
) -> str:
    destination_file_data: Iterator[DestinationFileData] = (
        DestinationFileData(
            string=json,
            path=bucket_url + "/" + str(uuid7()) + ".jsonl",
        )
        for json in jsons
    )
    match bucket_url:
        case str() as url if url.startswith("gs://"):
            CloudGoogle.to_filesystem_gcs(
                destination_file_data=destination_file_data,
            )

        case _:
            bucket_url_path: Path = Path(bucket_url)
            print(bucket_url_path)
            bucket_url_path.mkdir(
                parents=True,
                exist_ok=True,
            )
            to_filesystem_local(
                destination_file_data=destination_file_data,
            )

    return "Successfully uploaded"


def _get_jsons_from_srt_files(
    input_folder: str,
) -> Iterator[str]:
    count: int
    srt_file: SrtFile
    for count, srt_file in enumerate(
        _get_data_from_input_folder(
            input_folder=input_folder,
        ),
    ):
        fathom_webhook_raw_model: Webhook = Webhook(
            id=count,
            recording=Recording(
                url=srt_file.url,
                duration_in_minutes=srt_file.duration_minutes,
            ),
            meeting=Meeting(
                scheduled_start_time=srt_file.date,
                scheduled_end_time=None,
                scheduled_duration_in_minutes=None,
                join_url="elvis-backfill",
                title=srt_file.title,
                has_external_invitees=None,
                external_domains=None,
                invitees=None,
            ),
            fathom_user=FathomUser(
                name="elvis-backfill",
                email="elvisk@chalk.ai",
                team="devx",
            ),
            transcript=Transcript(
                plaintext=srt_file.full_text,
            ),
        )
        yield fathom_webhook_raw_model.model_dump_json(
            indent=None,
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
            bucket_url = DestinationType.get_bucket_url_from_bucket_name_for_local(
                bucket_name=BUCKET_NAME,
            )

        case DestinationType.GCP:
            bucket_url = BUCKET_URL

        case _:
            error_msg: str = f"Invalid destination type: {destination_type_enum}"
            raise ValueError(error_msg)

    response: str = _to_filesystem(
        jsons=_get_jsons_from_srt_files(
            input_folder=input_folder,
        ),
        bucket_url=bucket_url,
    )
    print(response)
