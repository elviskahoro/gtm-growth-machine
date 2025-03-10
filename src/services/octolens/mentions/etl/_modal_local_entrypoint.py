from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, ValidationError

from src.services.local.filesystem import get_paths

if TYPE_CHECKING:
    from collections.abc import Iterator


class DestinationType(str, Enum):
    LOCAL = "local"
    GCP = "gcp"

    @staticmethod
    def get_bucket_url_for_local(
        pipeline_name: str,
    ) -> str:
        cwd: str = str(Path.cwd())
        return f"{cwd}/out/{pipeline_name}"


def get_data_from_input_folder(
    input_folder: str,
    base_model: BaseModel,
) -> list[BaseModel]:
    paths: Iterator[Path] = get_paths(input_folder)
    data: list[BaseModel] = []
    current_path: Path | None = None
    try:
        path: Path
        for path in paths:
            current_path = path
            data.append(
                base_model.model_validate_json(
                    json_data=path.read_text(),
                ),
            )

    except ValidationError as e:
        print(e)
        print(current_path)
        raise

    return data
