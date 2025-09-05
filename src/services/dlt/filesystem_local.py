from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

from src.services.local.filesystem import DestinationFileData

if TYPE_CHECKING:
    import unittest.mock


def to_filesystem_local(
    destination_file_data: Iterator[DestinationFileData],
) -> None:
    for file_data in destination_file_data:
        file_path: Path = Path(file_data.path)
        with file_path.open(
            mode="w+",
        ) as f:
            f.write(
                file_data.string,
            )


# trunk-ignore-begin(ruff/PLR2004,ruff/S101)
class TestContentGenerator:
    """Test content generator class providing various file types and scenarios for testing."""

    @staticmethod
    def generate_md_data() -> str:
        return """# Test Documentation

## Overview
This is a test markdown file for unit testing.

## Features
- Feature 1: Test feature
- Feature 2: Another test feature

## Code Example
```python
def hello_world():
    print("Hello, World!")
```"""

    @staticmethod
    def generate_csv_data() -> str:
        return """timestamp,event,user_id,action
2024-01-15T10:00:00Z,page_view,user_123,home
2024-01-15T10:01:00Z,page_view,user_123,products
2024-01-15T10:02:00Z,click,user_123,add_to_cart
2024-01-15T10:03:00Z,purchase,user_123,checkout
"""

    @staticmethod
    def generate_xml_data() -> str:
        return """<?xml version="1.0" encoding="UTF-8"?>
<catalog>
    <product id="1">
        <name>Test Product 1</name>
        <price>19.99</price>
        <category>Electronics</category>
    </product>
    <product id="2">
        <name>Test Product 2</name>
        <price>29.99</price>
        <category>Books</category>
    </product>
</catalog>
"""

    @staticmethod
    def generate_json_data() -> dict:
        return {
            "id": 1,
            "name": "Test Item",
            "description": "A sample test item for unit testing",
            "timestamp": "2024-01-15T10:30:00Z",
            "active": True,
            "tags": ["test", "sample", "unit-test"],
            "metadata": {
                "created_by": "test_user",
                "version": "1.0.0",
                "environment": "test",
            },
        }

    @staticmethod
    def generate_json01_data() -> dict[str, str]:
        return {
            "event_id": "evt_001",
            "event_type": "user_login",
            "timestamp": "2024-01-15T09:00:00Z",
            "user_id": "user_123",
        }

    @staticmethod
    def create_destination_file_data() -> DestinationFileData:
        import json

        return DestinationFileData(
            string=json.dumps(TestContentGenerator.generate_json_data(), indent=2),
            path="test_bucket/test_folder/test_file_2024_01_15.json",
        )

    @staticmethod
    def create_destination_file_data_multiple() -> Iterator[DestinationFileData]:
        import json
        from typing import Any

        yield DestinationFileData(
            string=json.dumps(TestContentGenerator.generate_json01_data(), indent=2),
            path="gs://test-bucket/events/login_events_2024_01_15.json",
        )

        # JSON file 2 - Local path
        json_data2: dict[str, Any] = {
            "config_version": "2.0",
            "settings": {"debug": False, "timeout": 300, "retries": 3},
        }
        yield DestinationFileData(
            string=json.dumps(json_data2, indent=2),
            path="configs/app_config_v2.json",
        )

        yield DestinationFileData(
            string=TestContentGenerator.generate_md_data(),
            path="docs/test_readme.md",
        )

        # CSV file

        yield DestinationFileData(
            string=TestContentGenerator.generate_csv_data(),
            path="gs://test-bucket/analytics/user_events_2024_01_15.csv",
        )

        # XML file

        yield DestinationFileData(
            string=TestContentGenerator.generate_xml_data(),
            path="data/products_catalog.xml",
        )

    @staticmethod
    def create_destination_file_data_empty() -> Iterator[DestinationFileData]:
        return iter([])

    @staticmethod
    def create_destination_file_data_large_content() -> DestinationFileData:
        import json
        from typing import Any

        # Create a large JSON array with repeated data
        base_record: dict[str, Any] = {
            "id": 1,
            "timestamp": "2024-01-15T12:00:00Z",
            "data": "x" * 1000,  # 1KB per record
            "metadata": {
                "source": "test_generator",
                "version": "1.0",
                "processed": False,
            },
        }

        # Create array with ~1100 records to exceed 1MB
        large_array: list[dict[str, Any]] = []
        for i in range(1100):
            record: dict[str, Any] = base_record.copy()
            record["id"] = i
            large_array.append(record)

        return DestinationFileData(
            string=json.dumps(large_array, indent=2),
            path="large_files/big_data_export_2024_01_15.json",
        )

    @staticmethod
    def create_destination_file_data_special_paths() -> Iterator[DestinationFileData]:
        # Path with spaces
        yield DestinationFileData(
            string='{"test": "Path with spaces"}',
            path="test folder/sub folder/file with spaces.json",
        )

        # Path with unicode characters
        yield DestinationFileData(
            string='{"test": "Unicode path"}',
            path="测试/文件名/数据.json",
        )

        # Path with special characters
        yield DestinationFileData(
            string='{"test": "Special chars"}',
            path="data@2024/test#1/file[v1].json",
        )

        # Deeply nested path
        yield DestinationFileData(
            string='{"test": "Deep nesting"}',
            path="level1/level2/level3/level4/level5/level6/level7/level8/deep_file.json",
        )

        # Path with dots and dashes
        yield DestinationFileData(
            string='{"test": "Dots and dashes"}',
            path="data-2024.01.15/test-file.v2.backup.json",
        )

    @staticmethod
    def create_destination_file_data_with_errors() -> Iterator[DestinationFileData]:
        # Invalid JSON content
        yield DestinationFileData(
            string='{"invalid": "json", missing_quote: "value"}',
            path="errors/invalid_json.json",
        )

        # Binary-like content
        yield DestinationFileData(
            string="\x00\x01\x02\x03\x04\x05Binary content\xff\xfe\xfd",
            path="errors/binary_data.bin",
        )

        # Empty string content
        yield DestinationFileData(string="", path="errors/empty_file.txt")

        # Very long filename
        yield DestinationFileData(
            string='{"test": "long filename"}',
            path="errors/" + "a" * 200 + ".json",
        )

        # Path traversal attempt
        yield DestinationFileData(
            string='{"test": "path traversal"}',
            path="../../../etc/passwd",
        )


def test_single_file_write(tmp_path: Path) -> None:
    """Test writing a single file using the helper function."""
    # Get test data from helper function
    destination_file_data: DestinationFileData = (
        TestContentGenerator.create_destination_file_data()
    )
    # Update the path in the fixture data to use tmp_path
    file_data: DestinationFileData = DestinationFileData(
        string=destination_file_data.string,
        path=str(tmp_path / "test_file.json"),
    )

    # Write the file
    to_filesystem_local(iter([file_data]))

    # Verify the file was written
    written_file: Path = tmp_path / "test_file.json"
    assert written_file.exists()
    assert written_file.read_text() == destination_file_data.string


def test_multiple_files_write(tmp_path: Path) -> None:
    """Test writing multiple files using the iterator helper function."""
    # Get test data from helper function
    destination_file_data_multiple: Iterator[DestinationFileData] = (
        TestContentGenerator.create_destination_file_data_multiple()
    )
    # Update paths to use tmp_path
    updated_data: list[DestinationFileData] = []
    for idx, data in enumerate(destination_file_data_multiple):
        updated_data.append(
            DestinationFileData(
                string=data.string,
                path=str(tmp_path / f"file_{idx}.txt"),
            ),
        )

    # Write all files
    to_filesystem_local(iter(updated_data))

    # Verify all files were written
    for idx, data in enumerate(updated_data):
        written_file: Path = tmp_path / f"file_{idx}.txt"
        assert written_file.exists()
        assert written_file.read_text() == data.string


def test_empty_iterator() -> None:
    """Test handling of empty iterator."""
    # Get empty iterator from helper function
    destination_file_data_empty: Iterator[DestinationFileData] = (
        TestContentGenerator.create_destination_file_data_empty()
    )
    # Should complete without errors
    to_filesystem_local(destination_file_data_empty)


def test_large_content_write(tmp_path: Path) -> None:
    """Test writing large content files."""
    # Get large content from helper function
    destination_file_data_large_content: DestinationFileData = (
        TestContentGenerator.create_destination_file_data_large_content()
    )
    # Update path to use tmp_path
    file_data: DestinationFileData = DestinationFileData(
        string=destination_file_data_large_content.string,
        path=str(tmp_path / "large_file.json"),
    )

    # Write the large file
    to_filesystem_local(iter([file_data]))

    # Verify the file was written correctly
    written_file: Path = tmp_path / "large_file.json"
    assert written_file.exists()
    assert written_file.stat().st_size > 1_000_000  # Should be over 1MB


def test_special_paths(tmp_path: Path) -> None:
    """Test handling of special characters in paths."""
    # Get special paths data from helper function
    destination_file_data_special_paths: Iterator[DestinationFileData] = (
        TestContentGenerator.create_destination_file_data_special_paths()
    )
    # Convert to list and update paths
    special_data: list[DestinationFileData] = list(destination_file_data_special_paths)

    for data in special_data:
        # Create safe path under tmp_path
        safe_path: Path = tmp_path / "special" / Path(data.path).name
        safe_path.parent.mkdir(parents=True, exist_ok=True)

        file_data: DestinationFileData = DestinationFileData(
            string=data.string,
            path=str(safe_path),
        )

        # Write and verify
        to_filesystem_local(iter([file_data]))
        assert safe_path.exists()
        assert safe_path.read_text() == data.string


def test_with_error_data(tmp_path: Path) -> None:
    """Test handling files that might contain problematic content."""
    # Get error data from helper function
    error_data_iter: Iterator[DestinationFileData] = (
        TestContentGenerator.create_destination_file_data_with_errors()
    )
    error_data: list[DestinationFileData] = list(error_data_iter)

    # Test only the valid files (skip path traversal attempts)
    for data in error_data[:-1]:  # Skip the last one which is path traversal
        # Create safe path under tmp_path
        safe_path: Path = tmp_path / "errors" / Path(data.path).name
        safe_path.parent.mkdir(parents=True, exist_ok=True)

        file_data: DestinationFileData = DestinationFileData(
            string=data.string,
            path=str(safe_path),
        )
        to_filesystem_local(iter([file_data]))

        # Verify the file was written (even with "invalid" content)
        assert safe_path.exists()
        assert safe_path.read_text() == data.string


def test_to_filesystem_local_single_file() -> None:
    """Test to_filesystem_local with single file using mock_open."""
    from unittest.mock import mock_open, patch

    # Create test data
    test_content: str = "This is test content for the file"
    test_path: str = "/path/to/test/file.txt"
    file_data: DestinationFileData = DestinationFileData(
        string=test_content,
        path=test_path,
    )

    # Mock Path.open() using mock_open()
    m: unittest.mock.MagicMock = mock_open()
    with patch("pathlib.Path.open", m):
        # Call to_filesystem_local() with the data
        to_filesystem_local(iter([file_data]))

        # Assert that open() was called with correct path and mode "w+"
        m.assert_called_once_with(mode="w+")

        # Verify write() was called with the correct content
        handle: unittest.mock.MagicMock = m()
        handle.write.assert_called_once_with(test_content)


def test_to_filesystem_local_multiple_files() -> None:
    """Test to_filesystem_local with multiple files using mock_open."""
    import unittest.mock
    from typing import Any
    from unittest.mock import mock_open, patch

    # Create iterator with 3-4 DestinationFileData instances
    file_data_list: list[DestinationFileData] = [
        DestinationFileData(string="Content for file 1", path="/path/to/file1.txt"),
        DestinationFileData(
            string="Content for file 2",
            path="/path/to/file2.json",
        ),
        DestinationFileData(
            string="Content for file 3",
            path="/path/to/subdir/file3.csv",
        ),
        DestinationFileData(
            string="Content for file 4",
            path="/path/to/another/file4.xml",
        ),
    ]

    # Mock Path.open() for multiple file operations
    m: unittest.mock.MagicMock = mock_open()
    with patch("pathlib.Path.open", m):
        # Call to_filesystem_local() with the iterator
        to_filesystem_local(iter(file_data_list))

        # Assert open() was called for each file with correct paths
        # The mock_open() call count should match the number of files
        assert m.call_count == len(file_data_list)

        # Verify each call was made with mode="w+"
        # Filter only the open calls (not context manager calls)
        open_calls: list[Any] = [
            call for call in m.call_args_list if call == unittest.mock.call(mode="w+")
        ]
        assert len(open_calls) == len(file_data_list)

        # Verify each file's content was written correctly
        # Get all write calls from the mock
        all_calls: list[Any] = (
            m.return_value.__enter__.return_value.write.call_args_list
        )
        assert len(all_calls) == len(file_data_list)

        # Verify each write call has the correct content
        for idx, (call, file_data) in enumerate(zip(all_calls, file_data_list)):
            assert call == unittest.mock.call(
                file_data.string,
            ), f"Write call {idx} doesn't match expected content"


def test_to_filesystem_local_empty_iterator() -> None:
    """Test to_filesystem_local with empty iterator to ensure open() is never called."""
    from typing import Any
    from unittest.mock import mock_open, patch

    # Mock Path.open()
    m: unittest.mock.MagicMock = mock_open()
    with patch("pathlib.Path.open", m):
        # Pass an empty iterator to to_filesystem_local()
        to_filesystem_local(iter([]))

        # Verify that open() was never called
        m.assert_not_called()

        # Also verify that the mock's return value (file handle) was never used
        handle: unittest.mock.MagicMock = m.return_value
        handle.write.assert_not_called()
        handle.__enter__.assert_not_called()
        handle.__exit__.assert_not_called()

    # Test with a different way of creating empty iterator
    with patch("pathlib.Path.open", m) as mock_path_open:
        # Pass empty generator expression
        empty_gen: Iterator[Any] = (x for x in [])
        to_filesystem_local(empty_gen)

        # Ensure the function handles empty input gracefully
        mock_path_open.assert_not_called()


def test_to_filesystem_local_with_nested_paths() -> None:
    """Test to_filesystem_local with nested directory paths."""
    import unittest.mock
    from typing import Any
    from unittest.mock import mock_open, patch

    # Create test data with various levels of nested paths
    nested_file_data: list[DestinationFileData] = [
        DestinationFileData(
            string="Root level file content",
            path="/root/file.txt",
        ),
        DestinationFileData(string="One level deep", path="/root/level1/file.txt"),
        DestinationFileData(
            string="Two levels deep",
            path="/root/level1/level2/file.txt",
        ),
        DestinationFileData(
            string="Three levels deep",
            path="/root/level1/level2/level3/file.txt",
        ),
        DestinationFileData(
            string="Different branch",
            path="/root/branch2/subbranch/file.txt",
        ),
        DestinationFileData(
            string="Deeply nested path",
            path="/root/a/b/c/d/e/f/g/h/i/j/file.txt",
        ),
    ]

    # Mock Path.open() and Path.mkdir()
    m: unittest.mock.MagicMock = mock_open()
    with patch("pathlib.Path.open", m), patch(
        "pathlib.Path.mkdir",
    ), patch("pathlib.Path.exists", return_value=False):

        # Call to_filesystem_local() with nested paths
        to_filesystem_local(iter(nested_file_data))

        # Verify open() was called for each file
        assert m.call_count == len(nested_file_data)

        # Verify all calls used mode="w+"
        open_calls: list[Any] = [
            call for call in m.call_args_list if call == unittest.mock.call(mode="w+")
        ]
        assert len(open_calls) == len(nested_file_data)

        # Verify content was written correctly
        all_write_calls: list[Any] = (
            m.return_value.__enter__.return_value.write.call_args_list
        )
        for _idx, (call, file_data) in enumerate(
            zip(all_write_calls, nested_file_data),
        ):
            assert call == unittest.mock.call(file_data.string)


def test_to_filesystem_local_file_operations() -> None:
    """Test to verify file is opened in 'w+' mode and file handle operations."""
    import unittest.mock
    from typing import Any
    from unittest.mock import mock_open, patch

    test_content: str = "Test content for file operations verification"
    test_path: str = "/test/file/operations.txt"
    file_data: DestinationFileData = DestinationFileData(
        string=test_content,
        path=test_path,
    )

    # Create a more detailed mock to track file operations
    mock_file_handle: unittest.mock.MagicMock = mock_open()
    mock_file_handle.return_value.closed = False

    with patch("pathlib.Path.open", mock_file_handle) as mock_path_open:
        # Call the function
        to_filesystem_local(iter([file_data]))

        # Verify file was opened with "w+" mode (read and write, truncate existing)
        mock_path_open.assert_called_once_with(mode="w+")

        # Verify the context manager was used (file handle entered and exited)
        mock_file_handle.return_value.__enter__.assert_called_once()
        mock_file_handle.return_value.__exit__.assert_called_once()

        # Verify write was called with correct content
        mock_file_handle.return_value.__enter__.return_value.write.assert_called_once_with(
            test_content,
        )

        # Verify file handle cleanup (context manager ensures this)
        # The __exit__ call ensures the file is properly closed
        exit_args: Any = mock_file_handle.return_value.__exit__.call_args
        # __exit__ should be called with (None, None, None) for successful execution
        assert exit_args == unittest.mock.call(None, None, None)


def test_to_filesystem_local_exception_handling() -> None:
    """Test exception handling during file operations."""
    from typing import Any
    from unittest.mock import patch

    import pytest

    # Test various exceptions that might occur
    test_cases: list[dict[str, Any]] = [
        {
            "exception": PermissionError("Permission denied"),
            "exception_type": PermissionError,
            "path": "/restricted/file.txt",
            "content": "Cannot write here",
        },
        {
            "exception": OSError("Disk full"),
            "exception_type": OSError,
            "path": "/full/disk/file.txt",
            "content": "No space left",
        },
        {
            "exception": OSError("I/O error"),
            "exception_type": OSError,
            "path": "/io/error/file.txt",
            "content": "IO problem",
        },
        {
            "exception": FileNotFoundError("Directory does not exist"),
            "exception_type": FileNotFoundError,
            "path": "/nonexistent/dir/file.txt",
            "content": "Missing directory",
        },
    ]

    for test_case in test_cases:
        file_data: DestinationFileData = DestinationFileData(
            string=str(test_case["content"]),
            path=str(test_case["path"]),
        )

        # Mock Path.open() to raise the specific exception
        exception_instance: Exception = test_case["exception"]
        exception_type: type[Exception] = test_case["exception_type"]
        with patch(
            "pathlib.Path.open",
            side_effect=exception_instance,
        ), pytest.raises(
            exception_type,
        ):
            to_filesystem_local(iter([file_data]))


def test_to_filesystem_local_file_handle_cleanup() -> None:
    """Test proper cleanup of file handles even when exceptions occur during write."""
    from typing import Any
    from unittest.mock import mock_open, patch

    import pytest

    test_content: str = "Content that will fail to write"
    test_path: str = "/test/cleanup/file.txt"
    file_data: DestinationFileData = DestinationFileData(
        string=test_content,
        path=test_path,
    )

    # Create a mock that simulates write failure
    mock_file: unittest.mock.MagicMock = mock_open()
    mock_file.return_value.__enter__.return_value.write.side_effect = OSError(
        "Write failed",
    )

    with patch("pathlib.Path.open", mock_file):
        # Should raise OSError
        with pytest.raises(OSError, match="Write failed"):
            to_filesystem_local(iter([file_data]))

        # Verify file was opened
        mock_file.assert_called_once_with(mode="w+")

        # Verify context manager was entered
        mock_file.return_value.__enter__.assert_called_once()

        # Verify __exit__ was still called despite the exception
        # This ensures the file handle is properly closed
        mock_file.return_value.__exit__.assert_called_once()

        # __exit__ should be called with exception info
        exit_args: tuple[Any, ...] = mock_file.return_value.__exit__.call_args[0]
        assert exit_args[0] is OSError  # Exception type
        assert isinstance(exit_args[1], OSError)  # Exception instance
        assert exit_args[2] is not None  # Traceback


def test_to_filesystem_local_partial_write_failure() -> None:
    """Test behavior when some files succeed and others fail."""
    from unittest.mock import mock_open, patch

    import pytest

    file_data_list: list[DestinationFileData] = [
        DestinationFileData(string="Success 1", path="/path/file1.txt"),
        DestinationFileData(string="Success 2", path="/path/file2.txt"),
        DestinationFileData(string="Will fail", path="/path/file3.txt"),
        DestinationFileData(string="Never reached", path="/path/file4.txt"),
    ]

    # Counter to track number of open calls
    call_count: int = 0

    def mock_open_side_effect(*args: object, **kwargs: object) -> object:
        nonlocal call_count
        call_count += 1

        # Fail on the third file
        if call_count == 3:
            msg = "Cannot write to file3.txt"
            raise PermissionError(msg)

        # Return normal mock for other files
        return mock_open()(*args, **kwargs)

    with patch("pathlib.Path.open", side_effect=mock_open_side_effect):
        # Should raise PermissionError when it hits file3.txt
        with pytest.raises(PermissionError):
            to_filesystem_local(iter(file_data_list))

        # Verify only the first 3 files were attempted
        # (The function stops at the first exception)
        assert call_count == 3


# trunk-ignore-end(ruff/PLR2004,ruff/S101)
