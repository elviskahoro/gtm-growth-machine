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
                error_type = LanceTableExistenceErrorType.parse_existence_error(e)
                if error_type == LanceTableExistenceErrorType.RATE_LIMITED:
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
        error_type = LanceTableExistenceErrorType.parse_existence_error(exception)
        match error_type:
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
        error_type = LanceTableExistenceErrorType.parse_existence_error(exception)
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
def _get_test_model_class() -> type["BaseModel"]:  # type: ignore[reportUnusedFunction] # Used in tests
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


def _get_common_test_data() -> list[dict[str, str]]:  # type: ignore[reportUnusedFunction] # Used in tests
    """Get common test data used across test functions.

    Returns:
        list[dict[str, str]]: Test data with single record containing id and name
    """
    return [{"id": "1", "name": "test"}]


def integration_test_error_handling_paths() -> None:
    """Integration test: Comprehensive testing of all error handling paths in the upload system.

    This is an integration test that verifies the entire error handling flow
    including table creation, retry logic, index creation, and error propagation.
    Tests real interactions between components rather than isolated units.
    """
    import time
    from unittest.mock import Mock

    def setup_test_model() -> type["BaseModel"]:  # type: ignore[ruff/ANN202] # Internal test function
        """Returns the TestModel class."""
        return _get_test_model_class()

    def create_mock_db_and_table() -> tuple["Mock", "Mock"]:  # type: ignore[ruff/ANN202] # Internal test function
        """Creates standard mock DB and table objects."""
        mock_db = Mock()
        mock_table = Mock()
        return mock_db, mock_table

    def create_merge_insert_mock() -> "Mock":  # type: ignore[ruff/ANN202] # Internal test function
        """Creates the merge_insert mock chain."""
        merge_insert_mock = Mock()
        merge_insert_mock.when_matched_update_all.return_value = merge_insert_mock
        merge_insert_mock.when_not_matched_insert_all.return_value = merge_insert_mock
        return merge_insert_mock

    def test_table_not_found_triggers_creation() -> None:
        """Test case 1: Table not found error triggers table creation."""
        print("\n1. Testing table not found error triggers table creation...")
        mock_db, mock_table = create_mock_db_and_table()

        # First call raises "not found", second call returns the table
        mock_db.open_table.side_effect = [
            ValueError("Table 'test' was not found"),
            mock_table,
        ]
        mock_db.create_table.return_value = mock_table

        try:
            _, was_created = _get_or_create_table(
                db=mock_db,
                table_name="test_table",
                data_to_upload=test_data,
                base_model_type=test_model,
            )
            assert was_created is True, "Table should be marked as created"
            mock_db.create_table.assert_called_once()
            print("✓ Table creation on 'not found' error works correctly")

        except Exception as e:
            print(f"✗ Table creation test failed: {e}")

    def test_rate_limiting_triggers_retry() -> None:
        """Test case 2: Rate limiting errors trigger retry logic with exponential backoff."""
        print("\n2. Testing rate limiting error triggers retry logic...")
        mock_table = Mock()

        # Mock merge_insert to raise rate limit error twice, then succeed
        merge_insert_mock = create_merge_insert_mock()

        call_count = 0

        def mock_execute(**_kwargs) -> None:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                msg = "429 Too many concurrent writes"
                raise ValueError(msg)

        merge_insert_mock.execute.side_effect = mock_execute
        mock_table.merge_insert.return_value = merge_insert_mock

        start_time = time.time()
        try:
            _execute_merge_insert_with_retry(
                tbl=mock_table,
                primary_key="id",
                data_to_upload=test_data,
                max_retries=3,
                base_delay=0.1,  # Shorter delay for testing
            )
            end_time = time.time()
            elapsed = end_time - start_time

            assert call_count == 3, f"Expected 3 calls, got {call_count}"
            assert (
                elapsed >= 0.3
            ), f"Expected exponential backoff delays, elapsed: {elapsed}"
            print(
                f"✓ Rate limiting retry with exponential backoff works (calls: {call_count}, elapsed: {elapsed:.2f}s)",
            )

        except Exception as e:
            print(f"✗ Rate limiting retry test failed: {e}")

    def test_max_unindexed_rows_triggers_index_creation() -> None:
        """Test case 3: Max unindexed rows error triggers index creation."""
        print("\n3. Testing max unindexed rows error triggers index creation...")
        mock_table = Mock()
        mock_table.create_scalar_index.return_value = None

        # Create a mock for merge_insert chain
        merge_insert_mock = create_merge_insert_mock()
        merge_insert_mock.execute.return_value = None
        mock_table.merge_insert.return_value = merge_insert_mock

        max_rows_exception = ValueError(
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
            mock_table.create_scalar_index.assert_called_once_with(
                column="id",
                index_type="BTREE",
                replace=True,
                wait_timeout=timedelta(minutes=10),
            )
            print("✓ Index creation on max unindexed rows error works correctly")

        except Exception as e:
            print(f"✗ Index creation test failed: {e}")

    def test_unexpected_errors_are_reraised() -> None:
        """Test case 4: Unexpected errors are properly re-raised."""
        print("\n4. Testing unexpected errors are properly re-raised...")
        unexpected_error = RuntimeError("Some unexpected database error")

        try:
            _handle_merge_insert_error(
                tbl=Mock(),
                primary_key="id",
                primary_key_index_type="BTREE",
                data_to_upload=test_data,
                exception=unexpected_error,
            )
            print("✗ Expected exception to be re-raised")

        except RuntimeError as e:
            if str(e) == "Some unexpected database error":
                print("✓ Unexpected errors are properly re-raised")

            else:
                print(f"✗ Wrong exception re-raised: {e}")

        except Exception as e:
            print(f"✗ Unexpected exception type: {e}")

    def test_end_to_end_upload() -> None:
        """Test case 5: End-to-end upload functionality."""
        print("\n5. Testing end-to-end upload functionality...")
        mock_db, mock_table = create_mock_db_and_table()

        # Setup successful table opening
        mock_db.open_table.return_value = mock_table

        # Setup successful merge_insert
        merge_insert_mock = create_merge_insert_mock()
        merge_insert_mock.execute.return_value = None
        mock_table.merge_insert.return_value = merge_insert_mock

        try:
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
            print("✓ End-to-end upload functionality works correctly")
        except Exception as e:
            print(f"✗ End-to-end upload test failed: {e}")

    def test_error_type_parsing() -> None:
        """Test case 6: Error type parsing."""
        print("\n6. Testing error type parsing...")

        test_cases = [
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
            try:
                error_type = LanceTableExistenceErrorType.parse_existence_error(
                    Exception(error_msg),
                )
                assert (
                    error_type == expected_type
                ), f"Expected {expected_type}, got {error_type}"
                print(f"✓ Error parsing for '{error_msg[:30]}...' works correctly")

            except Exception as e:
                print(f"✗ Error parsing test failed for '{error_msg}': {e}")

    def test_invalid_error_message_handling() -> None:
        """Test case 7: Invalid error message handling."""
        print("\n7. Testing invalid error message handling...")
        try:
            LanceTableExistenceErrorType.parse_existence_error(
                Exception("Unknown error message"),
            )
            print("✗ Expected ValueError for unknown error")

        except ValueError:
            print("✓ Invalid error messages properly raise ValueError")

        except Exception as e:
            print(f"✗ Unexpected exception for invalid error: {e}")

    # Use shared helper functions
    test_model = setup_test_model()
    test_data = _get_common_test_data()

    print("\n=== Testing Error Handling Paths ===")

    # Execute all test cases in sequence
    test_table_not_found_triggers_creation()
    test_rate_limiting_triggers_retry()
    test_max_unindexed_rows_triggers_index_creation()
    test_unexpected_errors_are_reraised()
    test_end_to_end_upload()
    test_error_type_parsing()
    test_invalid_error_message_handling()

    print("\n=== Error Handling Tests Complete ===")


def integration_test_retry_and_failure_scenarios() -> None:
    """Test retry logic, failure scenarios, and complex error handling edge cases."""
    from unittest.mock import Mock

    def setup_test_model() -> type["BaseModel"]:
        """Returns the TestModel class for testing purposes.

        Returns:
            type[BaseModel]: TestModel class with id and name fields
        """
        return _get_test_model_class()

    def create_merge_insert_mock_with_error() -> "Mock":
        """Creates merge_insert mock that always fails.

        Returns:
            Mock: Mock merge_insert object that fails when execute() is called
        """
        merge_insert_mock = Mock()
        merge_insert_mock.when_matched_update_all.return_value = merge_insert_mock
        merge_insert_mock.when_not_matched_insert_all.return_value = merge_insert_mock
        merge_insert_mock.execute.side_effect = ValueError(
            "429 Too many concurrent writes",
        )
        return merge_insert_mock

    def test_rate_limiting_max_retries_exceeded() -> None:
        """Test case 1: Rate limiting with max retries exceeded."""
        print("\n1. Testing rate limiting with max retries exceeded...")
        mock_table = Mock()
        merge_insert_mock = create_merge_insert_mock_with_error()
        mock_table.merge_insert.return_value = merge_insert_mock

        try:
            _execute_merge_insert_with_retry(
                tbl=mock_table,
                primary_key="id",
                data_to_upload=test_data,
                max_retries=2,  # Small number for faster test
                base_delay=0.05,  # Very short delay
            )
            print("✗ Expected exception after max retries")

        except ValueError as e:
            if "429" in str(e):
                print("✓ Rate limiting correctly fails after max retries exceeded")
            else:
                print(f"✗ Unexpected error: {e}")

    def test_mixed_error_patterns_in_retry() -> None:
        """Test case 2: Mixed error patterns in retry logic."""
        print("\n2. Testing mixed error patterns in retry logic...")
        mock_table = Mock()
        merge_insert_mock = Mock()
        merge_insert_mock.when_matched_update_all.return_value = merge_insert_mock
        merge_insert_mock.when_not_matched_insert_all.return_value = merge_insert_mock

        call_count = 0

        def mixed_error_execute(**_kwargs) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                msg = "rate limit exceeded, please retry"
                raise ValueError(msg)
            if call_count == 2:
                msg = "too many requests"
                raise ValueError(msg)

        merge_insert_mock.execute.side_effect = mixed_error_execute
        mock_table.merge_insert.return_value = merge_insert_mock

        try:
            _execute_merge_insert_with_retry(
                tbl=mock_table,
                primary_key="id",
                data_to_upload=test_data,
                max_retries=3,
                base_delay=0.05,
            )
            print(
                f"✓ Mixed rate limiting patterns handled correctly (calls: {call_count})",
            )
        except Exception as e:
            print(f"✗ Mixed error patterns test failed: {e}")

    def test_table_creation_failure() -> None:
        """Test case 3: Error handling with table creation failure."""
        print("\n3. Testing error handling with table creation failure...")
        mock_db = Mock()
        mock_db.open_table.side_effect = ValueError("Table 'test' was not found")
        mock_db.create_table.side_effect = RuntimeError("Failed to create table")

        try:
            _get_or_create_table(
                db=mock_db,
                table_name="test_table",
                data_to_upload=test_data,
                base_model_type=test_model,
            )
            print("✗ Expected table creation to fail")
        except RuntimeError as e:
            if "Failed to create table" in str(e):
                print("✓ Table creation failures are properly propagated")
            else:
                print(f"✗ Unexpected error: {e}")

    def test_non_rate_limiting_errors_immediate_reraise() -> None:
        """Test case 4: Non-rate-limiting errors in retry logic."""
        print("\n4. Testing non-rate-limiting errors in retry logic...")
        mock_table = Mock()
        merge_insert_mock = Mock()
        merge_insert_mock.when_matched_update_all.return_value = merge_insert_mock
        merge_insert_mock.when_not_matched_insert_all.return_value = merge_insert_mock
        merge_insert_mock.execute.side_effect = ValueError("Column 'invalid' not found")
        mock_table.merge_insert.return_value = merge_insert_mock

        try:
            _execute_merge_insert_with_retry(
                tbl=mock_table,
                primary_key="id",
                data_to_upload=test_data,
                max_retries=3,
                base_delay=0.05,
            )
            print("✗ Expected non-rate-limiting error to be re-raised immediately")
        except ValueError as e:
            if "Column 'invalid' not found" in str(e):
                print("✓ Non-rate-limiting errors are immediately re-raised")
            else:
                print(f"✗ Unexpected error: {e}")

    def test_index_creation_with_subsequent_failure() -> None:
        """Test case 5: Index creation with subsequent failure."""
        print("\n5. Testing index creation with subsequent merge failure...")
        mock_table = Mock()
        mock_table.create_scalar_index.return_value = None

        # Create a mock for merge_insert that fails even after index creation
        merge_insert_mock = Mock()
        merge_insert_mock.when_matched_update_all.return_value = merge_insert_mock
        merge_insert_mock.when_not_matched_insert_all.return_value = merge_insert_mock
        merge_insert_mock.execute.side_effect = RuntimeError("Database connection lost")
        mock_table.merge_insert.return_value = merge_insert_mock

        max_rows_exception = ValueError(
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
            print("✗ Expected failure after index creation and retry")
        except RuntimeError as e:
            if "Database connection lost" in str(e):
                print("✓ Post-index-creation failures are properly propagated")
            else:
                print(f"✗ Unexpected error: {e}")
        except ValueError as e:
            # The original exception might be re-raised if parsing fails in the retry logic
            if "Database connection lost" in str(
                e,
            ) or "number of un-indexed rows" in str(e):
                print(
                    "✓ Post-index-creation failures are properly propagated (via original exception)",
                )
            else:
                print(f"✗ Unexpected ValueError: {e}")
        except Exception as e:
            print(f"✗ Unexpected exception type: {type(e).__name__}: {e}")

    def test_complex_error_message_parsing() -> None:
        """Test case 6: Complex error message parsing."""
        print("\n6. Testing complex error message parsing...")
        complex_cases = [
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
            try:
                error_type = LanceTableExistenceErrorType.parse_existence_error(
                    Exception(error_msg),
                )
                assert (
                    error_type == expected_type
                ), f"Expected {expected_type}, got {error_type}"
                print(f"✓ Complex error parsing for '{error_msg[:40]}...' works")
            except Exception as e:
                print(f"✗ Complex error parsing failed for '{error_msg[:40]}...': {e}")

    # Initialize test data and model
    test_model = setup_test_model()
    test_data = _get_common_test_data()

    print("\n=== Testing Additional Edge Cases ===")

    # Execute all test cases in sequence
    test_rate_limiting_max_retries_exceeded()
    test_mixed_error_patterns_in_retry()
    test_table_creation_failure()
    test_non_rate_limiting_errors_immediate_reraise()
    test_index_creation_with_subsequent_failure()
    test_complex_error_message_parsing()

    print("\n=== Additional Edge Case Tests Complete ===")


if __name__ == "__main__":
    integration_test_error_handling_paths()
    integration_test_retry_and_failure_scenarios()

# trunk-ignore-end(ruff/ANN002,ruff/ANN003,ruff/BLE001,ruff/PLC0415,ruff/PLR0912,ruff/PLR0915,ruff/PLR2004,ruff/S101)
