# trunk-ignore-all(ruff/PGH003,trunk/ignore-does-nothing)
from __future__ import annotations

import time
from datetime import timedelta
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from unittest.mock import Mock

    import pyarrow as pa
    from lancedb.db import DBConnection
    from lancedb.table import Table
    from pydantic import BaseModel


class LanceTableExistenceErrorType(Enum):
    EXISTS = auto()
    NOT_FOUND = auto()
    MAX_UNINDEXED_ROWS_EXCEEDED = auto()
    RATE_LIMITED = auto()
    TIMEOUT = auto()

    @staticmethod
    def parse_existence_error(exception: Exception) -> LanceTableExistenceErrorType:
        """Parse an exception and return the corresponding error type.

        Args:
            exception: The original exception that was raised

        Returns:
            LanceTableExistenceErrorType: The corresponding error type

        Raises:
            ValueError: If the exception message doesn't match any known patterns
        """
        exception_message: str = str(exception)
        match exception_message:
            case msg if "already exists" in msg:
                return LanceTableExistenceErrorType.EXISTS

            case msg if "was not found" in msg:
                return LanceTableExistenceErrorType.NOT_FOUND

            case msg if (
                "number of un-indexed rows" in msg and "exceeds the maximum" in msg
            ):
                return LanceTableExistenceErrorType.MAX_UNINDEXED_ROWS_EXCEEDED

            case msg if (
                "429" in msg
                or "Too many concurrent writes" in msg
                or "retry limit" in msg.lower()
            ):
                return LanceTableExistenceErrorType.RATE_LIMITED

            case msg if "timeout" in msg.lower() or "timed out" in msg.lower():
                return LanceTableExistenceErrorType.TIMEOUT

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
    attempt: int
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
                error_type: LanceTableExistenceErrorType = (
                    LanceTableExistenceErrorType.parse_existence_error(e)
                )

                if error_type == LanceTableExistenceErrorType.RATE_LIMITED:
                    if attempt < max_retries:
                        delay: float = base_delay * (2**attempt)  # Exponential backoff
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
                error_msg: str = str(e).lower()
                if (
                    "429" in error_msg

                    or "too many" in error_msg
                    or "rate limit" in error_msg
                    or "retry" in error_msg
                ):
                    if attempt < max_retries:
                        delay: float = base_delay * (2**attempt)
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
        tbl: Table = db.open_table(name=table_name)

    except ValueError as exception:
        error_type: LanceTableExistenceErrorType = (
            LanceTableExistenceErrorType.parse_existence_error(exception)
        )

        match error_type:
            case LanceTableExistenceErrorType.NOT_FOUND:
                tbl: Table = db.create_table(
                    name=table_name,
                    data=data_to_upload,
                    schema=base_model_type.lance_get_schema(),
                )
                print(f"Successfully created table: {table_name}")
                return tbl, True

            case LanceTableExistenceErrorType.EXISTS:
                error_msg: str = (
                    "Table exists should not be reachable, as we should have been able to successfully open the table"
                )
                raise ValueError(error_msg) from exception

            case _:
                error_msg: str = (
                    "Could not parse exception. This code path should not be reachable, error should have been already caught"
                )
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
        error_type: LanceTableExistenceErrorType = (
            LanceTableExistenceErrorType.parse_existence_error(exception)
        )
        match error_type:
            case LanceTableExistenceErrorType.RATE_LIMITED:
                # Re-raise to let the retry logic in _execute_merge_insert_with_retry handle it
                raise exception

            case LanceTableExistenceErrorType.MAX_UNINDEXED_ROWS_EXCEEDED:
                # Create index and retry
                print(
                    f"Creating scalar index on column '{primary_key}' with type '{primary_key_index_type}' (this may take a while for large datasets...)",
                )
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
        # Handle the case where parse_existence_error raises ValueError
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
    tbl: Table
    was_created: bool
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


# trunk-ignore-begin(ruff/ANN002,ruff/ANN003,ruff/BLE001,ruff/PLC0415,ruff/PLR0912,ruff/PLR0915,ruff/PLR2004,ruff/S101)
class TestLanceUpload:
    """Test helper class containing setup methods for lance upload tests."""

    @staticmethod
    def setup_test_model() -> type[BaseModel]:
        """Returns the TestModel class for testing purposes.

        Returns:
            type[BaseModel]: TestModel class with id and name fields
        """
        return TestLanceUpload.get_test_model_class()  # Fix: call the correct method

    @staticmethod
    def create_mock_db_and_table() -> tuple[Mock, Mock]:
        """Creates standard mock DB and table objects.

        Returns:
            tuple[Mock, Mock]: A tuple of (mock_db, mock_table)
        """
        from unittest.mock import Mock

        mock_db: Mock = Mock()
        mock_table: Mock = Mock()
        return mock_db, mock_table

    @staticmethod
    def create_merge_insert_mock() -> Mock:
        """Creates the merge_insert mock chain.

        Returns:
            Mock: Mock merge_insert object with chained methods
        """
        from unittest.mock import Mock

        merge_insert_mock: Mock = Mock()
        merge_insert_mock.when_matched_update_all.return_value = merge_insert_mock
        merge_insert_mock.when_not_matched_insert_all.return_value = merge_insert_mock
        return merge_insert_mock

    @staticmethod
    def create_merge_insert_mock_with_error() -> Mock:
        """Creates merge_insert mock that always fails.

        Returns:
            Mock: Mock merge_insert object that fails when execute() is called
        """
        from unittest.mock import Mock

        merge_insert_mock: Mock = Mock()
        merge_insert_mock.when_matched_update_all.return_value = merge_insert_mock
        merge_insert_mock.when_not_matched_insert_all.return_value = merge_insert_mock
        merge_insert_mock.execute.side_effect = ValueError(
            "429 Too many concurrent writes",
        )
        return merge_insert_mock

    @staticmethod
    def get_test_model_class() -> type[BaseModel]:  # type: ignore[reportUnusedFunction] # Used in tests
        """Get TestModel class for testing purposes.

        Returns:
            type[BaseModel]: TestModel class with id and name fields
        """
        from pydantic import BaseModel

        class TestModel(BaseModel):
            id: str
            name: str

            @staticmethod
            def lance_get_schema() -> "pa.Schema":
                import pyarrow as pa

                return pa.schema(
                    [
                        pa.field("id", pa.string()),
                        pa.field("name", pa.string()),
                    ],
                )

        return TestModel

    @staticmethod
    def get_common_test_data() -> list[dict[str, str]]:  # type: ignore[reportUnusedFunction] # Used in tests
        """Get common test data used across test functions.

        Returns:
            list[dict[str, str]]: Test data with single record containing id and name
        """
        return [{"id": "1", "name": "test"}]


def test_table_not_found_triggers_creation() -> None:
    test_model: type[BaseModel] = TestLanceUpload.setup_test_model()
    test_data: list[dict[str, str]] = TestLanceUpload.get_common_test_data()
    mock_db: Mock
    mock_table: Mock
    mock_db, mock_table = TestLanceUpload.create_mock_db_and_table()

    # First call raises "not found", second call returns the table
    mock_db.open_table.side_effect = [
        ValueError("Table 'test' was not found"),
        mock_table,
    ]
    mock_db.create_table.return_value = mock_table

    _: Table
    was_created: bool
    _, was_created = _get_or_create_table(
        db=mock_db,
        table_name="test_table",
        data_to_upload=test_data,
        base_model_type=test_model,
    )
    assert was_created is True
    mock_db.create_table.assert_called_once()


def test_rate_limiting_triggers_retry() -> None:
    """Test case 2: Rate limiting errors trigger retry logic with exponential backoff."""
    import time
    from unittest.mock import Mock

    test_data: list[dict[str, str]] = TestLanceUpload.get_common_test_data()
    mock_table: Mock = Mock()

    # Mock merge_insert to raise rate limit error twice, then succeed
    merge_insert_mock: Mock = TestLanceUpload.create_merge_insert_mock()

    call_count: int = 0

    def mock_execute(**_kwargs) -> None:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            msg: str = "429 Too many concurrent writes"
            raise ValueError(msg)

    merge_insert_mock.execute.side_effect = mock_execute
    mock_table.merge_insert.return_value = merge_insert_mock

    start_time: float = time.time()
    _execute_merge_insert_with_retry(
        tbl=mock_table,
        primary_key="id",
        data_to_upload=test_data,
        max_retries=3,
        base_delay=0.1,  # Shorter delay for testing
    )
    end_time: float = time.time()
    elapsed: float = end_time - start_time

    assert call_count == 3
    assert elapsed >= 0.3


def test_max_unindexed_rows_triggers_index_creation() -> None:
    """Test case 3: Max unindexed rows error triggers index creation."""
    from datetime import timedelta
    from unittest.mock import Mock

    test_data: list[dict[str, str]] = TestLanceUpload.get_common_test_data()
    mock_table: Mock = Mock()
    mock_table.create_scalar_index.return_value = None

    # Create a mock for merge_insert chain
    merge_insert_mock: Mock = TestLanceUpload.create_merge_insert_mock()
    merge_insert_mock.execute.return_value = None
    mock_table.merge_insert.return_value = merge_insert_mock

    max_rows_exception: ValueError = ValueError(
        "The number of un-indexed rows in the table exceeds the maximum allowed",
    )

    _handle_merge_insert_error(
        tbl=mock_table,
        primary_key="id",
        primary_key_index_type="BTREE",
        data_to_upload=test_data,
        exception=max_rows_exception,
    )
    mock_table.create_scalar_index.assert_called_once_with(
        column="id",
        index_type="BTREE",
        replace=True,
        wait_timeout=timedelta(minutes=10),
    )


def test_unexpected_errors_are_reraised() -> None:
    """Test case 4: Unexpected errors are properly re-raised."""
    from unittest.mock import Mock

    import pytest

    test_data: list[dict[str, str]] = TestLanceUpload.get_common_test_data()
    unexpected_error: RuntimeError = RuntimeError("Some unexpected database error")

    with pytest.raises(RuntimeError, match="Some unexpected database error"):
        _handle_merge_insert_error(
            tbl=Mock(),
            primary_key="id",
            primary_key_index_type="BTREE",
            data_to_upload=test_data,
            exception=unexpected_error,
        )


def test_end_to_end_upload() -> None:
    """Test case 5: End-to-end upload functionality."""
    test_model: type[BaseModel] = TestLanceUpload.setup_test_model()
    test_data: list[dict[str, str]] = TestLanceUpload.get_common_test_data()
    mock_db: Mock
    mock_table: Mock
    mock_db, mock_table = TestLanceUpload.create_mock_db_and_table()

    # Setup successful table opening
    mock_db.open_table.return_value = mock_table

    # Setup successful merge_insert
    merge_insert_mock: Mock = TestLanceUpload.create_merge_insert_mock()
    merge_insert_mock.execute.return_value = None
    mock_table.merge_insert.return_value = merge_insert_mock

    upload_to_lance(
        data_to_upload=test_data,
        base_model_type=test_model,
        db=mock_db,
        primary_key="id",
        primary_key_index_type="BTREE",
        table_name="test_table",
    )
    mock_db.open_table.assert_called_once_with(name="test_table")
    mock_table.merge_insert.assert_called_once_with(on="id")


def test_error_type_parsing() -> None:
    """Test case 6: Error type parsing."""
    test_cases: list[tuple[str, LanceTableExistenceErrorType]] = [
        ("Table 'test' already exists", LanceTableExistenceErrorType.EXISTS),
        ("Table 'test' was not found", LanceTableExistenceErrorType.NOT_FOUND),
        (
            "The number of un-indexed rows in the table exceeds the maximum allowed",
            LanceTableExistenceErrorType.MAX_UNINDEXED_ROWS_EXCEEDED,
        ),
        (
            "429 Too many concurrent writes",
            LanceTableExistenceErrorType.RATE_LIMITED,
        ),
        ("Connection timeout occurred", LanceTableExistenceErrorType.TIMEOUT),
    ]

    for error_msg, expected_type in test_cases:
        error_type: LanceTableExistenceErrorType = (
            LanceTableExistenceErrorType.parse_existence_error(
                Exception(error_msg),
            )
        )
        assert error_type == expected_type


def test_invalid_error_message_handling() -> None:
    """Test case 7: Invalid error message handling."""
    try:
        LanceTableExistenceErrorType.parse_existence_error(
            Exception("Unknown error message"),
        )
        msg: str = "Expected ValueError for unknown error"
        raise AssertionError(msg)
    except ValueError:

        pass  # Expected


def test_rate_limiting_max_retries_exceeded() -> None:
    """Test case 1: Rate limiting with max retries exceeded."""
    from unittest.mock import Mock

    import pytest

    test_data: list[dict[str, str]] = TestLanceUpload.get_common_test_data()
    mock_table: Mock = Mock()
    merge_insert_mock: Mock = TestLanceUpload.create_merge_insert_mock_with_error()
    mock_table.merge_insert.return_value = merge_insert_mock

    with pytest.raises(ValueError, match="429"):
        _execute_merge_insert_with_retry(
            tbl=mock_table,
            primary_key="id",
            data_to_upload=test_data,
            max_retries=2,  # Small number for faster test
            base_delay=0.05,  # Very short delay
        )


def test_mixed_error_patterns_in_retry() -> None:
    """Test case 2: Mixed error patterns in retry logic."""
    from unittest.mock import Mock

    test_data: list[dict[str, str]] = TestLanceUpload.get_common_test_data()
    mock_table: Mock = Mock()
    merge_insert_mock: Mock = Mock()
    merge_insert_mock.when_matched_update_all.return_value = merge_insert_mock
    merge_insert_mock.when_not_matched_insert_all.return_value = merge_insert_mock

    call_count: int = 0

    def mixed_error_execute(**_kwargs) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            msg: str = "rate limit exceeded, please retry"
            raise ValueError(msg)
        if call_count == 2:
            msg: str = "too many requests"
            raise ValueError(msg)

    merge_insert_mock.execute.side_effect = mixed_error_execute
    mock_table.merge_insert.return_value = merge_insert_mock

    _execute_merge_insert_with_retry(
        tbl=mock_table,
        primary_key="id",
        data_to_upload=test_data,
        max_retries=3,
        base_delay=0.05,
    )
    assert call_count == 3


def test_table_creation_failure() -> None:
    """Test case 3: Error handling with table creation failure."""
    from unittest.mock import Mock

    import pytest

    test_model: type[BaseModel] = TestLanceUpload.setup_test_model()
    test_data: list[dict[str, str]] = TestLanceUpload.get_common_test_data()
    mock_db: Mock = Mock()
    mock_db.open_table.side_effect = ValueError("Table 'test' was not found")
    mock_db.create_table.side_effect = RuntimeError("Failed to create table")

    with pytest.raises(RuntimeError, match="Failed to create table"):
        _get_or_create_table(
            db=mock_db,
            table_name="test_table",
            data_to_upload=test_data,
            base_model_type=test_model,
        )


def test_non_rate_limiting_errors_immediate_reraise() -> None:
    """Test case 4: Non-rate-limiting errors in retry logic."""
    from unittest.mock import Mock

    import pytest

    test_data: list[dict[str, str]] = TestLanceUpload.get_common_test_data()
    mock_table: Mock = Mock()
    merge_insert_mock: Mock = Mock()
    merge_insert_mock.when_matched_update_all.return_value = merge_insert_mock
    merge_insert_mock.when_not_matched_insert_all.return_value = merge_insert_mock
    merge_insert_mock.execute.side_effect = ValueError("Column 'invalid' not found")
    mock_table.merge_insert.return_value = merge_insert_mock

    with pytest.raises(ValueError, match="Column 'invalid' not found"):
        _execute_merge_insert_with_retry(
            tbl=mock_table,
            primary_key="id",
            data_to_upload=test_data,
            max_retries=3,
            base_delay=0.05,
        )


def test_index_creation_with_subsequent_failure() -> None:
    """Test case 5: Index creation with subsequent failure."""
    from unittest.mock import Mock

    test_data: list[dict[str, str]] = TestLanceUpload.get_common_test_data()
    mock_table: Mock = Mock()
    mock_table.create_scalar_index.return_value = None

    # Create a mock for merge_insert that fails even after index creation
    merge_insert_mock: Mock = Mock()
    merge_insert_mock.when_matched_update_all.return_value = merge_insert_mock
    merge_insert_mock.when_not_matched_insert_all.return_value = merge_insert_mock
    merge_insert_mock.execute.side_effect = RuntimeError("Database connection lost")
    mock_table.merge_insert.return_value = merge_insert_mock

    max_rows_exception: ValueError = ValueError(
        "The number of un-indexed rows in the table exceeds the maximum allowed",
    )

    try:
        _handle_merge_insert_error(
            tbl=mock_table,
            primary_key="id",
            primary_key_index_type="BTREE",
            data_to_upload=test_data,
            exception=max_rows_exception,
        )
        msg: str = "Expected failure after index creation and retry"
        raise AssertionError(msg)

    except (RuntimeError, ValueError) as e:
        error_msg: str = str(e)
        assert (
            "Database connection lost" in error_msg
            or "number of un-indexed rows" in error_msg
        )


def test_complex_error_message_parsing() -> None:
    """Test case 6: Complex error message parsing."""
    complex_cases: list[tuple[str, LanceTableExistenceErrorType]] = [
        (
            "HTTP 429: Too many concurrent writes. Retry limit exceeded.",
            LanceTableExistenceErrorType.RATE_LIMITED,
        ),
        (
            "Request timeout: connection timed out after 30s",
            LanceTableExistenceErrorType.TIMEOUT,
        ),
        (
            "The table 'test' was not found in database",
            LanceTableExistenceErrorType.NOT_FOUND,
        ),
        (
            "Table 'users' already exists in the database",
            LanceTableExistenceErrorType.EXISTS,
        ),
        (
            "Error: number of un-indexed rows (500000) exceeds the maximum allowed (100000)",
            LanceTableExistenceErrorType.MAX_UNINDEXED_ROWS_EXCEEDED,
        ),
    ]

    for error_msg, expected_type in complex_cases:
        error_type: LanceTableExistenceErrorType = (
            LanceTableExistenceErrorType.parse_existence_error(
                Exception(error_msg),
            )
        )
        assert error_type == expected_type


# trunk-ignore-end(ruff/ANN002,ruff/ANN003,ruff/BLE001,ruff/PLC0415,ruff/PLR0912,ruff/PLR0915,ruff/PLR2004,ruff/S101)
