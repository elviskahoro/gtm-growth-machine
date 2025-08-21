from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from pydantic import BaseModel, ValidationError

from .filesystem_regex import sanitize_string

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from datetime import datetime


class FileUtility:

    @staticmethod
    def get_paths(
        input_folder: str,
        extension: Iterable[str] | None,
    ) -> Iterator[Path]:
        cwd: str = str(Path.cwd())
        input_folder_path: Path = Path(f"{cwd}/{input_folder}")
        if not input_folder_path.exists() or not input_folder_path.is_dir():
            error_msg: str = f"Input folder '{input_folder_path}' does not exist"
            raise AssertionError(
                error_msg,
            )

        return (
            f
            for f in input_folder_path.iterdir()
            if f.is_file() and (extension is None or f.suffix in extension)
        )

    @staticmethod
    def file_clean_timestamp_from_datetime(
        dt: datetime,
    ) -> str:
        return dt.strftime("%Y_%m_%d_%H_%M_%S")

    @staticmethod
    def file_clean_string(
        string: str,
    ) -> str:
        lowercase: str = string.lower()
        no_space_on_borders: str = lowercase.strip()
        return sanitize_string(string=no_space_on_borders)


class SourceFileData(NamedTuple):
    path: Path | None
    base_model: BaseModel

    @classmethod
    def from_json_data(
        cls: type[SourceFileData],
        json_data: str,
        base_model_type: type[BaseModel] | None,
    ) -> SourceFileData | None:
        if base_model_type is None:
            error_msg = "base_model_type cannot be None"
            raise ValueError(error_msg)

        return SourceFileData(
            path=None,
            base_model=base_model_type.model_validate_json(
                json_data=json_data,
            ),
        )

    @classmethod
    def from_local_storage_path(
        cls: type[SourceFileData],
        local_storage_path: str,
        base_model_type: type[BaseModel] | None,
    ) -> SourceFileData:
        cwd: str = str(Path.cwd())
        path: Path = Path(f"{cwd}/{local_storage_path}")
        if not path.exists():
            error: str = f"File not found at {path}"
            raise FileNotFoundError(error)

        json_data: str = path.read_text()
        if not json_data:
            error: str = "File is empty"
            raise ValueError(error)

        if base_model_type is None:
            error_msg = "base_model_type cannot be None"
            raise ValueError(error_msg)

        return SourceFileData(
            path=path,
            base_model=base_model_type.model_validate_json(
                json_data=json_data,
            ),
        )

    @staticmethod
    def from_jsonl_file(
        jsonl_path: str,
        base_model_type: type[BaseModel] | None,
    ) -> Iterator[SourceFileData]:
        if base_model_type is None:
            error_msg = "base_model_type cannot be None"
            raise ValueError(error_msg)

        cwd: str = str(Path.cwd())
        path: Path = Path(f"{cwd}/{jsonl_path}")

        # Check file extension
        if path.suffix.lower() != ".jsonl":
            error_msg = f"File must have .jsonl extension, got: {path.suffix}"
            raise ValueError(error_msg)

        if not path.exists():
            error_msg: str = f"File not found at {path}"
            raise FileNotFoundError(error_msg)

        try:
            file = path.open(encoding="utf-8")

        except OSError as e:
            error_msg: str = f"Error opening file {path}: {e}"
            raise FileNotFoundError(error_msg) from e

        with file:
            for line_number, raw_line in enumerate(file, start=1):
                line: str = raw_line.strip()
                if not line:  # Skip empty lines
                    continue

                try:
                    yield SourceFileData(
                        path=path,
                        base_model=base_model_type.model_validate_json(
                            json_data=line,
                        ),
                    )
                except ValidationError as e:
                    error_msg: str = (
                        f"Validation error on line {line_number} in {path}: {e}"
                    )
                    print(error_msg)
                    raise

    @staticmethod
    def from_input_folder(
        input_folder: str,
        base_model_type: type[BaseModel],
        extension: Iterable[str] | None,
    ) -> Iterator[SourceFileData]:
        paths: Iterator[Path] = FileUtility.get_paths(
            input_folder=input_folder,
            extension=extension,
        )
        current_path: Path | None = None
        try:
            path: Path
            for path in paths:
                current_path = path
                yield SourceFileData(
                    path=path,
                    base_model=base_model_type.model_validate_json(
                        json_data=path.read_text(),
                    ),
                )

        except ValidationError as e:
            print(e)
            print(current_path)
            if (
                current_path
                and current_path.suffix.lower() == ".jsonl"
                and any(
                    "trailing characters" in str(error.get("msg", ""))
                    for error in e.errors()
                )
            ):
                print(
                    "Error: JSONL files with multiple JSON objects per file are not supported "
                    "for input folders. Use from_jsonl_file() method instead for JSONL files.",
                )
            raise


class DestinationFileData(NamedTuple):
    string: str
    path: str

    @staticmethod
    def from_source_file_data(
        source_file_data: Iterator[SourceFileData],
        bucket_url: str,
        storage: BaseModel | None,
    ) -> Iterator[DestinationFileData]:
        for individual_file_data in source_file_data:
            try:
                yield DestinationFileData(
                    string=individual_file_data.base_model.etl_get_json(
                        storage=storage,
                    ),
                    path=f"{bucket_url}/{individual_file_data.base_model.etl_get_file_name()}",
                )

            except (AttributeError, ValueError, AssertionError):
                error_msg: str = f"Error processing file: {individual_file_data.path}"
                print(error_msg)
                raise


# trunk-ignore-begin(ruff/PLR2004,ruff/S101,ruff/PLC0415)
def test_file_utility_get_paths_valid_directory() -> None:
    """Test FileUtility.get_paths with a valid directory."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test files
        test_dir: Path = Path(temp_dir) / "test_input"
        test_dir.mkdir()

        # Create files with different extensions
        (test_dir / "file1.txt").write_text("content1")
        (test_dir / "file2.json").write_text("content2")
        (test_dir / "file3.csv").write_text("content3")
        (test_dir / "file4.log").write_text("content4")

        # Create a subdirectory (should be ignored)
        (test_dir / "subdir").mkdir()

        # Change to temp directory
        original_cwd: Path = Path.cwd()
        import os

        os.chdir(temp_dir)

        try:
            # Test without extension filter
            paths: list[Path] = list(FileUtility.get_paths("test_input", None))
            assert len(paths) == 4

            # Test with extension filter
            txt_paths: list[Path] = list(FileUtility.get_paths("test_input", [".txt"]))
            assert len(txt_paths) == 1
            assert txt_paths[0].name == "file1.txt"

            # Test with multiple extensions
            multi_paths: list[Path] = list(
                FileUtility.get_paths("test_input", [".txt", ".json"]),
            )
            assert len(multi_paths) == 2

        finally:
            os.chdir(original_cwd)


def test_file_utility_get_paths_nonexistent_directory() -> None:
    """Test FileUtility.get_paths with nonexistent directory."""
    import pytest

    with pytest.raises(AssertionError, match="does not exist"):
        list(FileUtility.get_paths("nonexistent_folder", None))


def test_file_utility_get_paths_empty_directory() -> None:
    """Test FileUtility.get_paths with empty directory."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir: Path = Path(temp_dir) / "empty_test"
        test_dir.mkdir()

        original_cwd: Path = Path.cwd()
        import os

        os.chdir(temp_dir)

        try:
            paths: list[Path] = list(FileUtility.get_paths("empty_test", None))
            assert len(paths) == 0

        finally:
            os.chdir(original_cwd)


def test_file_utility_get_paths_file_instead_of_directory() -> None:
    """Test FileUtility.get_paths with a file path instead of directory."""
    import tempfile
    from pathlib import Path

    import pytest

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a file instead of directory
        test_file: Path = Path(temp_dir) / "test_file.txt"
        test_file.write_text("content")

        original_cwd: Path = Path.cwd()
        import os

        os.chdir(temp_dir)

        try:
            with pytest.raises(AssertionError, match="does not exist"):
                list(FileUtility.get_paths("test_file.txt", None))

        finally:
            os.chdir(original_cwd)


def test_file_clean_timestamp_from_datetime() -> None:
    """Test FileUtility.file_clean_timestamp_from_datetime."""
    from datetime import datetime, timezone

    # Test with UTC datetime
    dt: datetime = datetime(2023, 12, 15, 14, 30, 45, tzinfo=timezone.utc)
    result: str = FileUtility.file_clean_timestamp_from_datetime(dt)
    assert result == "2023_12_15_14_30_45"

    # Test with different datetime
    dt2: datetime = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    result2: str = FileUtility.file_clean_timestamp_from_datetime(dt2)
    assert result2 == "2024_01_01_00_00_00"

    # Test with single digit values
    dt3: datetime = datetime(2024, 3, 5, 8, 7, 9, tzinfo=timezone.utc)
    result3: str = FileUtility.file_clean_timestamp_from_datetime(dt3)
    assert result3 == "2024_03_05_08_07_09"


def test_file_clean_string() -> None:
    """Test FileUtility.file_clean_string."""
    # Test basic functionality
    result: str = FileUtility.file_clean_string("Hello World!")
    assert result == "hello·world"  # Based on sanitize_string behavior

    # Test with mixed case and special characters
    result2: str = FileUtility.file_clean_string("  Test File (1).txt  ")
    assert result2 == "test·file·1txt"

    # Test empty string
    result3: str = FileUtility.file_clean_string("")
    assert result3 == ""

    # Test string that's already clean
    result4: str = FileUtility.file_clean_string("cleanfilename")
    assert result4 == "cleanfilename"


def test_source_file_data_from_json_data_valid() -> None:
    """Test SourceFileData.from_json_data with valid JSON."""
    from pydantic import BaseModel

    class TestModel(BaseModel):
        name: str
        value: int

    json_data: str = '{"name": "test", "value": 42}'
    result: SourceFileData | None = SourceFileData.from_json_data(json_data, TestModel)

    assert result is not None
    assert result.path is None
    assert isinstance(result.base_model, TestModel)
    assert result.base_model.name == "test"
    assert result.base_model.value == 42


def test_source_file_data_from_json_data_none_model_type() -> None:
    """Test SourceFileData.from_json_data with None model type."""
    import pytest

    json_data: str = '{"name": "test", "value": 42}'

    with pytest.raises(ValueError, match="base_model_type cannot be None"):
        SourceFileData.from_json_data(json_data, None)


def test_source_file_data_from_json_data_invalid_json() -> None:
    """Test SourceFileData.from_json_data with invalid JSON."""
    import pytest
    from pydantic import BaseModel, ValidationError

    class TestModel(BaseModel):
        name: str
        value: int

    invalid_json: str = '{"name": "test", "invalid": true}'

    with pytest.raises(ValidationError):
        SourceFileData.from_json_data(invalid_json, TestModel)


def test_source_file_data_from_local_storage_path_valid() -> None:
    """Test SourceFileData.from_local_storage_path with valid file."""
    import tempfile
    from pathlib import Path

    from pydantic import BaseModel

    class TestModel(BaseModel):
        name: str
        value: int

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test file
        test_file: Path = Path(temp_dir) / "test.json"
        test_file.write_text('{"name": "test_file", "value": 123}')

        original_cwd: Path = Path.cwd()
        import os

        os.chdir(temp_dir)

        try:
            result: SourceFileData = SourceFileData.from_local_storage_path(
                "test.json",
                TestModel,
            )

            assert result.path is not None
            assert result.path.name == "test.json"
            assert isinstance(result.base_model, TestModel)
            assert result.base_model.name == "test_file"
            assert result.base_model.value == 123

        finally:
            os.chdir(original_cwd)


def test_source_file_data_from_local_storage_path_file_not_found() -> None:
    """Test SourceFileData.from_local_storage_path with nonexistent file."""
    import pytest
    from pydantic import BaseModel

    class TestModel(BaseModel):
        name: str
        value: int

    with pytest.raises(FileNotFoundError, match="File not found"):
        SourceFileData.from_local_storage_path("nonexistent.json", TestModel)


def test_source_file_data_from_local_storage_path_empty_file() -> None:
    """Test SourceFileData.from_local_storage_path with empty file."""
    import tempfile
    from pathlib import Path

    import pytest
    from pydantic import BaseModel

    class TestModel(BaseModel):
        name: str
        value: int

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create empty file
        test_file: Path = Path(temp_dir) / "empty.json"
        test_file.write_text("")

        original_cwd: Path = Path.cwd()
        import os

        os.chdir(temp_dir)

        try:
            with pytest.raises(ValueError, match="File is empty"):
                SourceFileData.from_local_storage_path("empty.json", TestModel)

        finally:
            os.chdir(original_cwd)


def test_source_file_data_from_local_storage_path_none_model_type() -> None:
    """Test SourceFileData.from_local_storage_path with None model type."""
    import tempfile
    from pathlib import Path

    import pytest

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test file
        test_file: Path = Path(temp_dir) / "test.json"
        test_file.write_text('{"name": "test"}')

        original_cwd: Path = Path.cwd()
        import os

        os.chdir(temp_dir)

        try:
            with pytest.raises(ValueError, match="base_model_type cannot be None"):
                SourceFileData.from_local_storage_path("test.json", None)

        finally:
            os.chdir(original_cwd)


def test_source_file_data_from_input_folder_valid() -> None:
    """Test SourceFileData.from_input_folder with valid files."""
    import tempfile
    from pathlib import Path

    from pydantic import BaseModel

    class TestModel(BaseModel):
        name: str
        value: int

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test directory and files
        test_dir: Path = Path(temp_dir) / "input"
        test_dir.mkdir()

        (test_dir / "file1.json").write_text('{"name": "file1", "value": 1}')
        (test_dir / "file2.json").write_text('{"name": "file2", "value": 2}')

        original_cwd: Path = Path.cwd()
        import os

        os.chdir(temp_dir)

        try:
            results: list[SourceFileData] = list(
                SourceFileData.from_input_folder(
                    "input",
                    TestModel,
                    [".json"],
                ),
            )

            assert len(results) == 2

            # Sort by name for consistent testing
            results.sort(key=lambda x: x.base_model.name)  # type: ignore[misc]

            assert results[0].base_model.name == "file1"
            assert results[0].base_model.value == 1
            assert results[1].base_model.name == "file2"
            assert results[1].base_model.value == 2

        finally:
            os.chdir(original_cwd)


def test_source_file_data_from_input_folder_validation_error() -> None:
    """Test SourceFileData.from_input_folder with validation error."""
    import tempfile
    from pathlib import Path

    import pytest
    from pydantic import BaseModel, ValidationError

    class TestModel(BaseModel):
        name: str
        value: int

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test directory and files
        test_dir: Path = Path(temp_dir) / "input"
        test_dir.mkdir()

        # Create file with invalid data
        (test_dir / "invalid.json").write_text(
            '{"name": "test", "value": "not_a_number"}',
        )

        original_cwd: Path = Path.cwd()
        import os

        os.chdir(temp_dir)

        try:
            with pytest.raises(ValidationError):
                list(SourceFileData.from_input_folder("input", TestModel, [".json"]))

        finally:
            os.chdir(original_cwd)


def test_destination_file_data_from_source_file_data_valid() -> None:
    """Test DestinationFileData.from_source_file_data with valid data."""
    from pydantic import BaseModel

    class MockModel(BaseModel):
        name: str
        value: int

        def etl_get_json(self, storage: BaseModel | None = None) -> str:  # noqa: ARG002
            return (
                f'{{"processed_name": "{self.name}", "processed_value": {self.value}}}'
            )

        def etl_get_file_name(self) -> str:
            return f"{self.name}.json"

    # Create source data
    source_data: list[SourceFileData] = [
        SourceFileData(
            path=Path("test1.json"),
            base_model=MockModel(name="test1", value=1),
        ),
        SourceFileData(
            path=Path("test2.json"),
            base_model=MockModel(name="test2", value=2),
        ),
    ]

    bucket_url: str = "gs://test-bucket"
    results: list[DestinationFileData] = list(
        DestinationFileData.from_source_file_data(
            iter(source_data),
            bucket_url,
            None,
        ),
    )

    assert len(results) == 2

    assert results[0].string == '{"processed_name": "test1", "processed_value": 1}'
    assert results[0].path == "gs://test-bucket/test1.json"

    assert results[1].string == '{"processed_name": "test2", "processed_value": 2}'
    assert results[1].path == "gs://test-bucket/test2.json"


def test_destination_file_data_from_source_file_data_error() -> None:
    """Test DestinationFileData.from_source_file_data with processing error."""
    import pytest
    from pydantic import BaseModel

    class MockModelWithError(BaseModel):
        name: str

        def etl_get_json(self, storage: BaseModel | None = None) -> str:  # noqa: ARG002
            msg = "Processing error"
            raise ValueError(msg)

        def etl_get_file_name(self) -> str:
            return f"{self.name}.json"

    # Create source data with error
    source_data: list[SourceFileData] = [
        SourceFileData(
            path=Path("error.json"),
            base_model=MockModelWithError(name="error"),
        ),
    ]

    with pytest.raises(ValueError, match="Processing error"):
        list(
            DestinationFileData.from_source_file_data(
                iter(source_data),
                "gs://bucket",
                None,
            ),
        )


def test_destination_file_data_from_source_file_data_missing_methods() -> None:
    """Test DestinationFileData.from_source_file_data with missing ETL methods."""
    import pytest
    from pydantic import BaseModel

    class MockModelWithoutMethods(BaseModel):
        name: str

    # Create source data with model missing ETL methods
    source_data: list[SourceFileData] = [
        SourceFileData(
            path=Path("test.json"),
            base_model=MockModelWithoutMethods(name="test"),
        ),
    ]

    with pytest.raises(AttributeError):
        list(
            DestinationFileData.from_source_file_data(
                iter(source_data),
                "gs://bucket",
                None,
            ),
        )


def test_source_file_data_named_tuple_properties() -> None:
    """Test SourceFileData NamedTuple properties."""
    from pydantic import BaseModel

    class TestModel(BaseModel):
        name: str
        value: int

    model: TestModel = TestModel(name="test", value=42)
    path: Path = Path("/test/path.json")

    source_data: SourceFileData = SourceFileData(path=path, base_model=model)

    # Test named tuple properties
    assert source_data.path == path
    assert source_data.base_model == model
    assert isinstance(source_data.base_model, TestModel)

    # Test tuple unpacking
    unpacked_path: Path | None
    unpacked_model: BaseModel
    unpacked_path, unpacked_model = source_data
    assert unpacked_path == path
    assert unpacked_model == model


def test_destination_file_data_named_tuple_properties() -> None:
    """Test DestinationFileData NamedTuple properties."""
    string_content: str = "test content"
    file_path: str = "gs://bucket/file.json"

    dest_data: DestinationFileData = DestinationFileData(
        string=string_content,
        path=file_path,
    )

    # Test named tuple properties
    assert dest_data.string == string_content
    assert dest_data.path == file_path

    # Test tuple unpacking
    unpacked_string: str
    unpacked_path: str
    unpacked_string, unpacked_path = dest_data
    assert unpacked_string == string_content
    assert unpacked_path == file_path


def test_source_file_data_with_none_path() -> None:
    """Test SourceFileData with None path (from JSON data)."""
    from pydantic import BaseModel

    class TestModel(BaseModel):
        name: str
        value: int

    model: TestModel = TestModel(name="test", value=42)
    source_data: SourceFileData = SourceFileData(path=None, base_model=model)

    assert source_data.path is None
    assert source_data.base_model == model


def test_file_utility_static_methods() -> None:
    """Test that FileUtility methods are properly static."""
    # Test that methods can be called without instance
    from datetime import datetime, timezone

    # Test file_clean_timestamp_from_datetime
    dt: datetime = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    timestamp: str = FileUtility.file_clean_timestamp_from_datetime(dt)
    assert timestamp == "2024_01_01_12_00_00"

    # Test file_clean_string
    cleaned: str = FileUtility.file_clean_string("Test String")
    assert cleaned == "test·string"


def test_destination_file_data_static_method() -> None:
    """Test that DestinationFileData.from_source_file_data is properly static."""
    # This is tested in other tests, but verify it's static
    from pydantic import BaseModel

    class MockModel(BaseModel):
        name: str

        def etl_get_json(self, storage: BaseModel | None = None) -> str:  # noqa: ARG002
            return '{"test": "value"}'

        def etl_get_file_name(self) -> str:
            return "test.json"

    source_data: list[SourceFileData] = [
        SourceFileData(path=None, base_model=MockModel(name="test")),
    ]

    # Should be callable without instance
    results: list[DestinationFileData] = list(
        DestinationFileData.from_source_file_data(
            iter(source_data),
            "gs://bucket",
            None,
        ),
    )

    assert len(results) == 1
    assert results[0].string == '{"test": "value"}'
    assert results[0].path == "gs://bucket/test.json"


def test_source_file_data_from_input_folder_empty_directory() -> None:
    """Test SourceFileData.from_input_folder with empty directory."""
    import tempfile
    from pathlib import Path

    from pydantic import BaseModel

    class TestModel(BaseModel):
        name: str
        value: int

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create empty test directory
        test_dir: Path = Path(temp_dir) / "empty_input"
        test_dir.mkdir()

        original_cwd: Path = Path.cwd()
        import os

        os.chdir(temp_dir)

        try:
            results: list[SourceFileData] = list(
                SourceFileData.from_input_folder(
                    "empty_input",
                    TestModel,
                    [".json"],
                ),
            )

            assert len(results) == 0

        finally:
            os.chdir(original_cwd)


def test_destination_file_data_with_storage_parameter() -> None:
    """Test DestinationFileData.from_source_file_data with storage parameter."""
    from pydantic import BaseModel

    class StorageModel(BaseModel):
        config: str

    class MockModel(BaseModel):
        name: str

        def etl_get_json(self, storage: BaseModel | None = None) -> str:
            if storage:
                return (
                    f'{{"name": "{self.name}", "storage_config": "{storage.config}"}}'
                )
            return f'{{"name": "{self.name}"}}'

        def etl_get_file_name(self) -> str:
            return f"{self.name}.json"

    storage: StorageModel = StorageModel(config="test_config")
    source_data: list[SourceFileData] = [
        SourceFileData(path=None, base_model=MockModel(name="test")),
    ]

    results: list[DestinationFileData] = list(
        DestinationFileData.from_source_file_data(
            iter(source_data),
            "gs://bucket",
            storage,
        ),
    )

    assert len(results) == 1
    assert results[0].string == '{"name": "test", "storage_config": "test_config"}'
    assert results[0].path == "gs://bucket/test.json"


# trunk-ignore-end(ruff/PLR2004,ruff/S101,ruff/PLC0415)
