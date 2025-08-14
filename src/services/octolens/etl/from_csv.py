from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import modal
import polars as pl
from modal import Image

from src.services.local.filesystem import FileUtility
from src.services.octolens import Mention, Webhook

BUCKET_NAME: str = "chalk-ai-devx-octolens-mentions-from_csv"

if TYPE_CHECKING:
    from collections.abc import Iterator

image: Image = modal.Image.debian_slim().pip_install(
    "fastapi[standard]",
)
image = image.add_local_python_source(
    *[
        "src",
    ],
)
app = modal.App(
    name=__name__,
    image=image,
)


@app.local_entrypoint()
def local(
    input_folder: str,
) -> None:
    sub_paths: Iterator[Path] = FileUtility.get_paths(
        input_folder=input_folder,
        extension=[".csv"],
    )
    df_list: Iterator[pl.DataFrame] = (pl.read_csv(path) for path in sub_paths)
    df_full: pl.DataFrame = pl.concat(
        df_list,
        how="align",
    )
    df: pl.DataFrame = df_full.unique(
        subset=["URL"],
    )
    mentions: Iterator[Mention] = (
        Mention.model_validate(row)
        for row in df.iter_rows(
            named=True,
        )
    )
    cwd: str = str(Path.cwd())

    output_dir: Path = Path(f"{cwd}/out/{BUCKET_NAME}")
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    count: int = 0
    mention: Mention
    for mention in mentions:
        count += 1
        webhook: Webhook = Webhook(
            action="mention_created",
            data=mention,
        )
        output_file_path: Path = output_dir / webhook.etl_get_file_name(
            extension=".jsonl",
        )

        with output_file_path.open(
            mode="w",
        ) as f:
            f.write(
                webhook.model_dump_json(
                    indent=None,
                ),
            )

    print(f"{count:06d}: {output_dir}")
