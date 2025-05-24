# trunk-ignore-all(ruff/PGH003,trunk/ignore-does-nothing)
from __future__ import annotations

from enum import Enum, auto
from typing import TYPE_CHECKING

from src.services.lancedb.setup import init_client

if TYPE_CHECKING:

    from pydantic import BaseModel

    from lancedb.db import DBConnection
    from lancedb.table import Table


class LanceTableExistenceErrorType(Enum):
    EXISTS = auto()
    NOT_FOUND = auto()

    @classmethod
    def parse_existence_error(
        cls: type[LanceTableExistenceErrorType],
        exception: ValueError,
    ) -> LanceTableExistenceErrorType:
        exception_message: str = str(exception)
        print(exception_message)
        match exception_message:
            case msg if "already exists" in msg:
                return LanceTableExistenceErrorType.EXISTS

            case msg if "was not found" in msg:
                return LanceTableExistenceErrorType.NOT_FOUND

            case _:
                error_msg: str = f"Unexpected error message: {exception_message}"
                raise ValueError(error_msg)


def upload_to_lance(
    base_models_to_upload: list[BaseModel],
    base_model_type: type[BaseModel],
) -> None:
    project_name: str = base_model_type.lance_get_project_name()
    table_name: str = base_model_type.lance_get_table_name()
    primary_key: str = base_model_type.lance_get_primary_key()

    db: DBConnection = init_client(
        project_name=project_name,
    )

    tbl: Table | None = None
    should_create_table: bool = False
    lance_exception: LanceTableExistenceErrorType | None = None
    try:
        tbl = db.open_table(
            name=table_name,
        )

    except ValueError as exception:
        lance_exception = LanceTableExistenceErrorType.parse_existence_error(
            exception=exception,
        )
        match lance_exception:
            case LanceTableExistenceErrorType.NOT_FOUND:
                should_create_table = True

            case LanceTableExistenceErrorType.EXISTS:
                error_msg = "Table exists should not be reachable, as we should have been able to successfully open the table"
                raise ValueError(
                    error_msg,
                ) from exception

            case _:
                error_msg = "Could not parse exception. This code path should not be reachable, error should have been already caught"
                raise ValueError(
                    error_msg,
                ) from exception

    data: list[dict] = [
        base_model_to_upload.model_dump()
        for base_model_to_upload in base_models_to_upload
    ]
    if should_create_table:
        tbl = db.create_table(
            name=table_name,
            data=data,
            schema=base_model_type.lance_get_schema(),
        )
        print(f"Successfully created table: {table_name}")

    if tbl is not None and not should_create_table:
        tbl.merge_insert(
            on=primary_key,
        ).when_matched_update_all().when_not_matched_insert_all().execute(
            new_data=data,
        )
