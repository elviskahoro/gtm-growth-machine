from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from pydantic import BaseModel, ValidationError

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


def get_paths(
    input_folder: str,
    extension: Iterable[str] | None,
) -> Iterator[Path]:
    cwd: str = str(Path.cwd())
    input_folder_path: Path = Path(f"{cwd}/{input_folder}")
    if not input_folder_path.exists() or not input_folder_path.is_dir():
        error_msg: str = f"Input folder '{input_folder_path}' does not exist"
        raise AssertionError(
            error_msg,
        )

    return (
        f
        for f in input_folder_path.iterdir()
        if f.is_file() and (extension is None or f.suffix in extension)
    )


class SourceFileData(NamedTuple):
    path: Path | None
    base_model: BaseModel


def get_file_data_from_input_folder(
    input_folder: str,
    base_model: BaseModel,
    extension: Iterable[str] | None,
) -> Iterator[SourceFileData]:
    paths: Iterator[Path] = get_paths(
        input_folder=input_folder,
        extension=extension,
    )
    current_path: Path | None = None
    try:
        path: Path
        for path in paths:
            current_path = path
            yield SourceFileData(
                path=path,
                base_model=base_model.model_validate_json(
                    json_data=path.read_text(),
                ),
            )

    except ValidationError as e:
        print(e)
        print(current_path)
        raise


class DestinationFileData(NamedTuple):
    json: str
    path: str


def get_json_data_from_file_data(
    source_file_data: Iterator[SourceFileData],
    bucket_url: str,
) -> Iterator[DestinationFileData]:
    for individual_file_data in source_file_data:
        try:
            yield DestinationFileData(
                json=individual_file_data.base_model.etl_get_json(),
                path=f"{bucket_url}/{individual_file_data.base_model.etl_get_file_name()}",
            )

        except (AttributeError, ValueError):
            error_msg: str = f"Error processing file: {individual_file_data.path}"
            print(error_msg)
            raise
