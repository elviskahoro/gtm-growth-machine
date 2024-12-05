from __future__ import annotations

import os
from pathlib import Path
from typing import TypeVar

import dlt
import dotenv
import modal
import orjson
from dlt.destinations import filesystem
from pydantic import BaseModel

from web.services.octolens.mentions import Mention

T = TypeVar(
    "T",
    bound=BaseModel,
)

APP_NAME: str = "linkedin_connections"

image = modal.Image.debian_slim().pip_install(
    "fastapi[standard]",
    "dlt",
    "dlt[s3]",
    "python-dotenv",
)
app = modal.App(
    name=APP_NAME,
    image=image,
)


def to_filesystem(
    octolens_mentions: list[Mention],
    bucket_url: str,
    destination_name: str,
) -> str:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = str(True)
    pipeline = dlt.pipeline(
        pipeline_name=APP_NAME,
        destination=filesystem(
            bucket_url=bucket_url,
            destination_name=destination_name,
        ),
    )
    dlt_resource = dlt.resource(
        octolens_mentions,
        name=APP_NAME,
    )
    return pipeline.run(
        data=dlt_resource,
        loader_file_format="jsonl",
    ).asstr()


@app.function(
    secrets=[
        modal.Secret.from_name(APP_NAME),
    ],
    # cloud="aws", This feature is available on the Team and Enterprise plans, read more at https://modal.com/docs/guide/region-selection
    # region="us-west-2", This feature is available on the Team and Enterprise plans, read more at https://modal.com/docs/guide/region-selection
    allow_concurrent_inputs=1000,
    enable_memory_snapshot=True,
)
@modal.web_endpoint(
    method="POST",
    docs=True,
)
def web(
    data: Mention,
) -> str:
    os.environ["DESTINATION__CREDENTIALS__AWS_ACCESS_KEY_ID"] = os.environ.get(
        "AWS_ACCESS_KEY_ID",
        "",
    )
    os.environ["DESTINATION__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = os.environ.get(
        "AWS_SECRET_ACCESS_KEY",
        "",
    )
    response: str = to_filesystem(
        octolens_mentions=[data],
        bucket_url="s3://elviskahoro-ai-octolens",
        destination_name="aws-bucket-elviskahoro-ai-octolens",
    )
    return response


@app.local_entrypoint()
def local(
    input_file: str,
) -> None:
    dotenv.load_dotenv()
    file_path = Path.cwd() / input_file

    octolens_mention_obj: dict = orjson.loads(file_path.read_text())
    octolens_mention: Mention = Mention.model_validate(
        obj=octolens_mention_obj,
    )
    response: str = to_filesystem(
        octolens_mentions=[octolens_mention],
        bucket_url="file://Users/elvis/Documents/elviskahoro/growthmachine/data/out",
        destination_name="local_filesystem",
    )
    print(response)


def load_csv_to_pydantic(
    csv_path: str,
    model_class: type[T],
) -> list[T]:
    """Loads CSV data into a list of Pydantic models.

    Args:
        csv_path: Path to the CSV file
        model_class: The Pydantic model class to use

    Returns:
        List of instantiated Pydantic models
    """
    # Read CSV into pandas DataFrame
    df = pd.read_csv(csv_path)

    # Convert DataFrame records to list of dicts
    records = df.to_dict("records")

    # Create Pydantic models from records
    models = [model_class.model_validate(record) for record in records]

    return models


# Example usage
class YourModel(BaseModel):
    field1: str
    field2: int


models = load_csv_to_pydantic("path/to/your.csv", YourModel)

T = TypeVar("T", bound=BaseModel)


def load_csv_to_pydantic(csv_path: str, model_class: type[T]) -> list[T]:
    """Loads CSV data into a list of Pydantic models.

    Args:
        csv_path: Path to the CSV file
        model_class: The Pydantic model class to use

    Returns:
        List of instantiated Pydantic models
    """
    # Read CSV into pandas DataFrame
    df = pd.read_csv(csv_path)

    # Convert DataFrame records to list of dicts
    records = df.to_dict("records")

    # Create Pydantic models from records
    models = [model_class.model_validate(record) for record in records]

    return models
