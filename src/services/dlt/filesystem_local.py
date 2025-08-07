from collections.abc import Iterator
from pathlib import Path

from src.services.local.filesystem import DestinationFileData


def to_filesystem_local(
    destination_file_data: Iterator[DestinationFileData],
) -> None:
    # cwd = Path.cwd()
    for file_data in destination_file_data:
        file_path: Path = Path(file_data.path)
        # relative_path: Path = file_path.relative_to(cwd)
        # print(relative_path)
        with file_path.open(
            mode="w+",
        ) as f:
            f.write(
                file_data.string,
            )
