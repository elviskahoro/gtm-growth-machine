# trunk-ignore-all(ruff/PGH003,trunk/ignore-does-nothing)
from __future__ import annotations

from typing import TYPE_CHECKING

from src.services.lancedb.setup import init_client

if TYPE_CHECKING:

    from pydantic import BaseModel

    from lancedb.db import DBConnection
    from lancedb.table import Table


def make_index_in_lance(
    base_model_type: type[BaseModel],
) -> None:
    db: DBConnection = init_client(
        project_name=base_model_type.lance_get_project_name(),
    )
    tbl: Table = db.open_table(
        name=base_model_type.lance_get_table_name(),
    )
    tbl.create_index(
        vector_column_name=base_model_type.lance_get_vector_column_name(),
        metric=base_model_type.lance_get_index_metric(),
        index_type=base_model_type.lance_get_index_type(),
        index_cache_size=base_model_type.lance_get_index_cache_size(),
    )
