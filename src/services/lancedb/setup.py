from __future__ import annotations

import os
from typing import TYPE_CHECKING

import lancedb

if TYPE_CHECKING:
    from lancedb.db import DBConnection


LANCEDB_REGION: str = "us-east-1"


def init_client(
    project_name: str,
    region: str = LANCEDB_REGION,
) -> DBConnection:
    lance_api_key: str | None = os.getenv("LANCEDB_API_KEY")
    if lance_api_key is None:
        error_msg: str = "LANCEDB_API_KEY is not set."
        raise ValueError(error_msg)

    return lancedb.connect(
        uri=f"db://{project_name}",
        api_key=lance_api_key,
        region=region,
    )
