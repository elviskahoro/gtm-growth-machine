from __future__ import annotations

from typing import NamedTuple

from pydantic import BaseModel, ValidationError
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


def get_paths(
    input_folder: str,
    extension: str | None,
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
        if f.is_file() and (extension is None or f.suffix == extension)
    )


class FileData(NamedTuple):
    path: Path
    data: BaseModel


def get_data_from_input_folder(
    input_folder: str,
    base_model: BaseModel,
) -> Iterator[FileData]:
    paths: Iterator[Path] = get_paths(
        input_folder=input_folder,
        extension=".json",
    )
    current_path: Path | None = None
    try:
        path: Path
        for path in paths:
            current_path = path
            yield FileData(
                path=path,
                data=base_model.model_validate_json(
                    json_data=path.read_text(),
                ),
            )

    except ValidationError as e:
        print(e)
        print(current_path)
        raise
