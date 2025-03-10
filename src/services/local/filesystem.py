from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


def get_paths(
    input_folder: str,
) -> Iterator[Path]:
    cwd: str = str(Path.cwd())
    input_folder_path: Path = Path(f"{cwd}/{input_folder}")
    if not input_folder_path.exists() or not input_folder_path.is_dir():
        error_msg: str = f"Input folder '{input_folder_path}' does not exist"
        raise AssertionError(
            error_msg,
        )

    return (f for f in input_folder_path.iterdir() if f.is_file())
