# trunk-ignore-all(ruff/PGH003,trunk/ignore-does-nothing)
from __future__ import annotations

import time
from datetime import timedelta
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:

    from lancedb.db import DBConnection
    from lancedb.table import Table
    from pydantic import BaseModel


class LanceTableExistenceErrorType(Enum):
    EXISTS = auto()
    NOT_FOUND = auto()
    MAX_UNINDEXED_ROWS_EXCEEDED = auto()
    RATE_LIMITED = auto()
    TIMEOUT = auto()


class LanceTableExistenceError:

    def __init__(
        self,
        error_type: LanceTableExistenceErrorType,
        exception: Exception,
    ) -> None:
        """Initialize a LanceTableExistenceError instance.

        Args:
            error_type: The type of error that occurred (EXISTS, NOT_FOUND,
                       MAX_UNINDEXED_ROWS_EXCEEDED, or RATE_LIMITED)
            exception: The original exception that was raised
        """
        self.error_type = error_type
        self.exception = exception

    @classmethod
    def parse_existence_error(
        cls,
        exception: Exception,
    ) -> "LanceTableExistenceError":
        exception_message: str = str(exception)
        print(exception_message)
        match exception_message:
            case msg if "already exists" in msg:
                return cls(
                    LanceTableExistenceErrorType.EXISTS,
                    exception,
                )

            case msg if "was not found" in msg:
                return cls(
                    LanceTableExistenceErrorType.NOT_FOUND,
                    exception,
                )

            case (
                msg
            ) if "number of un-indexed rows" in msg and "exceeds the maximum" in msg:
                return cls(
                    LanceTableExistenceErrorType.MAX_UNINDEXED_ROWS_EXCEEDED,
                    exception,
                )

            case (
                msg
            ) if "429" in msg or "Too many concurrent writes" in msg or "retry limit" in msg.lower():
                return cls(
                    LanceTableExistenceErrorType.RATE_LIMITED,
                    exception,
                )

            case (
                msg
            ) if "timeout" in msg.lower() or "timed out" in msg.lower():
                return cls(
                    LanceTableExistenceErrorType.TIMEOUT,
                    exception,
                )

            case _:
                error_msg: str = f"Unexpected error message: {exception_message}"
                raise ValueError(error_msg)


def _execute_merge_insert_with_retry(
    tbl: Table,
    primary_key: str,
    data_to_upload: list[dict],
    max_retries: int = 5,
    base_delay: float = 1.0,
) -> None:
    """Execute merge insert with exponential backoff retry for rate limiting."""
    for attempt in range(max_retries + 1):
        try:
            tbl.merge_insert(
                on=primary_key,
            ).when_matched_update_all().when_not_matched_insert_all().execute(
                new_data=data_to_upload,
            )

        except (ValueError, RuntimeError, ConnectionError, TimeoutError) as e:
            # Parse the error to determine if it's rate limited
            # This catches both requests.HTTPError and LanceDB's HttpError/RetryError
            try:
                error = LanceTableExistenceError.parse_existence_error(e)
                if error.error_type == LanceTableExistenceErrorType.RATE_LIMITED:
                    if attempt < max_retries:
                        delay = base_delay * (2**attempt)  # Exponential backoff
                        print(
                            f"Rate limited (attempt {attempt + 1}/{max_retries + 1}), retrying in {delay:.1f}s...",
                        )
                        time.sleep(delay)
                        continue

                    print(f"Rate limit exceeded after {max_retries + 1} attempts")
                    raise

                # If it's not a rate limiting error, re-raise
                raise
            except ValueError:
                # If we can't parse the error (unexpected error type),
                # check if it's a known rate limiting pattern in the message
                error_msg = str(e).lower()
                if (
                    "429" in error_msg
                    or "too many" in error_msg
                    or "rate limit" in error_msg
                    or "retry" in error_msg
                ):
                    if attempt < max_retries:
                        delay = base_delay * (2**attempt)
                        print(
                            f"Rate limited (attempt {attempt + 1}/{max_retries + 1}), retrying in {delay:.1f}s...",
                        )
                        time.sleep(delay)
                        continue

                    print(f"Rate limit exceeded after {max_retries + 1} attempts")
                    raise

                # Not a rate limiting error, re-raise
                raise

        else:
            return  # Success, exit the retry loop


def _get_or_create_table(
    db: DBConnection,
    table_name: str,
    data_to_upload: list[dict],
    base_model_type: type[BaseModel],
) -> tuple[Table, bool]:
    """Get existing table or create new one. Returns (table, was_created)."""
    try:
        tbl = db.open_table(name=table_name)
    except ValueError as exception:
        lance_exception = LanceTableExistenceError.parse_existence_error(
            exception=exception,
        )
        match lance_exception.error_type:
            case LanceTableExistenceErrorType.NOT_FOUND:
                tbl = db.create_table(
                    name=table_name,
                    data=data_to_upload,
                    schema=base_model_type.lance_get_schema(),
                )
                print(f"Successfully created table: {table_name}")
                return tbl, True

            case LanceTableExistenceErrorType.EXISTS:
                error_msg = "Table exists should not be reachable, as we should have been able to successfully open the table"
                raise ValueError(error_msg) from exception

            case _:
                error_msg = "Could not parse exception. This code path should not be reachable, error should have been already caught"
                raise ValueError(error_msg) from exception
    else:
        return tbl, False


def _handle_merge_insert_error(
    tbl: Table,
    primary_key: str,
    primary_key_index_type: str,
    data_to_upload: list[dict],
    exception: Exception,
) -> None:
    """Handle merge insert errors by creating indexes if needed."""
    try:
        error = LanceTableExistenceError.parse_existence_error(exception)
        match error.error_type:
            case LanceTableExistenceErrorType.RATE_LIMITED:
                # Re-raise to let the retry logic in _execute_merge_insert_with_retry handle it
                raise exception
            case LanceTableExistenceErrorType.MAX_UNINDEXED_ROWS_EXCEEDED:
                # Create index and retry
                print(f"Creating scalar index on column '{primary_key}' with type '{primary_key_index_type}' (this may take a while for large datasets...)")
                tbl.create_scalar_index(
                    column=primary_key,
                    index_type=primary_key_index_type,  # type: ignore[arg-type]
                    replace=True,
                    wait_timeout=timedelta(minutes=10),
                )
                print("Index creation completed successfully")
                # Retry the operation after creating the index
                _execute_merge_insert_with_retry(
                    tbl=tbl,
                    primary_key=primary_key,
                    data_to_upload=data_to_upload,
                )
            case _:
                raise exception
    except ValueError:
        # If we can't parse the exception, re-raise the original
        raise exception from None


def upload_to_lance(
    data_to_upload: list[dict],
    base_model_type: type[BaseModel],
    db: DBConnection,
    primary_key: str,
    primary_key_index_type: str,
    table_name: str,
) -> None:
    """Upload data to Lance table, creating table or indexes as needed."""
    tbl, was_created = _get_or_create_table(
        db=db,
        table_name=table_name,
        data_to_upload=data_to_upload,
        base_model_type=base_model_type,
    )

    if not was_created:
        try:
            _execute_merge_insert_with_retry(
                tbl=tbl,
                primary_key=primary_key,
                data_to_upload=data_to_upload,
            )
        except (ValueError, RuntimeError, ConnectionError, TimeoutError) as exception:
            _handle_merge_insert_error(
                tbl=tbl,
                primary_key=primary_key,
                primary_key_index_type=primary_key_index_type,
                data_to_upload=data_to_upload,
                exception=exception,
            )
