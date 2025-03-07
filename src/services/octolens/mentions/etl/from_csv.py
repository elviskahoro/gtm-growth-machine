from __future__ import annotations
from pathlib import Path

import polars as pl

from src.services.octolens import Mention

from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

import modal
from modal import Image

if TYPE_CHECKING:
    from pydantic import BaseModel

from src.services.octolens import MentionData

image: Image = modal.Image.debian_slim().pip_install(
    "fastapi[standard]",
)
image.add_local_python_source(
    *[
        "src",
    ],
)
app = modal.App(
    name="",
    image=image,
)


def get_sub_paths(
    input_folder: str,
) -> list[Path]:
    cwd: str = str(Path.cwd())
    input_folder_path: Path = Path(f"{cwd}/{input_folder}")
    if not input_folder_path.exists() or not input_folder_path.is_dir():
        raise AssertionError(f"Input folder '{input_folder_path}' does not exist")

    return [f for f in input_folder_path.iterdir() if f.is_file()]


def ensure_output_dir(
    output_dir: str,
) -> Path:
    cwd: str = str(Path.cwd())
    output_path: Path = Path(f"{cwd}/{output_dir}")
    output_path.mkdir(
        parents=True,
        exist_ok=True,
    )
    return output_path


@app.local_entrypoint()
def local(
    input_folder: str,
) -> None:
    sub_paths: list[Path] = get_sub_paths(input_folder)
    df_list: list[pl.DataFrame] = [pl.read_csv(path) for path in sub_paths]
    df_full: pl.DataFrame = pl.concat(
        df_list,
        how="align",
    )
    df: pl.DataFrame = df_full.unique(
        subset=["URL"],
    )
    mention_data_list: list[MentionData] = list(
        MentionData.model_validate(row)
        for row in df.iter_rows(
            named=True,
        )
    )
    output_dir: Path = ensure_output_dir(
        output_dir="out",
    )
    count: int = 0
    for mention_data in mention_data_list:
        print(count)
        output_file_path: Path = output_dir / f"{count:06d}.json"
        mention: Mention = Mention(
            action="mention_created",
            data=mention_data,
        )
        with open(
            file=output_file_path,
            mode="w+",
        ) as f:
            f.write(mention.model_dump_json())

        count += 1
