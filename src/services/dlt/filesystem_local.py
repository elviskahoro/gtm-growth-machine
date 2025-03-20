from collections.abc import Iterator
from pathlib import Path

from src.services.local.filesystem import DestinationFileData


def to_filesystem_local(
    destination_file_data: Iterator[DestinationFileData],
) -> None:
    # cwd = Path.cwd()
    for json_data in destination_file_data:
        file_path: Path = Path(json_data.path)
        # relative_path: Path = file_path.relative_to(cwd)
        # print(relative_path)
        with file_path.open(
            mode="w+",
        ) as f:
            f.write(
                json_data.json,
            )
