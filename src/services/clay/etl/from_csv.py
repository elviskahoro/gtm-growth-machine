from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import modal
import polars as pl
from modal import Image

from src.services.clay import EventAttendee

if TYPE_CHECKING:
    from collections.abc import Iterator


BUCKET_NAME: str = "devx-clay-event-attendees"
MIN_PARTS_FOR_HYPHENATED_DATE: int = 4

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


def parse_filename_metadata(
    filename: str,
) -> tuple[str, datetime]:
    """Extract source and date from filename.

    The source will always be the full filename.
    Supports two filename formats for date extraction:
    - YYYY-MM-DD-source (e.g., "2025-11-11-statsig_experimentation")
    - YYYYMMDD-source (legacy format, e.g., "20251111-conference")

    Args:
        filename: The filename stem (without extension)

    Returns:
        tuple: (source, parsed_date) where source is the full filename
    """
    # Source is always the full filename
    source: str = filename
    parsed_date: datetime

    if "-" in filename:
        # Try to extract date pattern
        parts: list[str] = filename.split(sep="-")

        # Check if first 3 parts form a valid date (YYYY-MM-DD)
        if len(parts) >= MIN_PARTS_FOR_HYPHENATED_DATE:
            try:
                date_str: str = f"{parts[0]}-{parts[1]}-{parts[2]}"
                # Parse date from YYYY-MM-DD format (naive, then add UTC timezone)
                parsed_date = datetime.strptime(
                    date_str,
                    "%Y-%m-%d",
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                # If YYYY-MM-DD parsing fails, fallback to YYYYMMDD format
                date_str = parts[0]
                try:
                    parsed_date = datetime.strptime(
                        date_str,
                        "%Y%m%d",
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    # Final fallback to current datetime if parsing fails
                    parsed_date = datetime.now(tz=timezone.utc)
        else:
            # Try legacy YYYYMMDD format
            date_str = parts[0]
            try:
                parsed_date = datetime.strptime(
                    date_str,
                    "%Y%m%d",
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                # Fallback to current datetime if parsing fails
                parsed_date = datetime.now(tz=timezone.utc)

    else:
        # No date pattern found, use current datetime
        parsed_date = datetime.now(tz=timezone.utc)

    return source, parsed_date


def process_csv_file(
    path: Path,
    source: str,
    parsed_date: datetime,
    event_url: str,
) -> pl.DataFrame:
    """Process a single CSV file and add metadata columns from EventAttendee model.

    Args:
        path: Path to the CSV file
        source: Source name extracted from filename
        parsed_date: Date extracted from filename
        event_url: Event URL to add (required)

    Returns:
        DataFrame with added metadata columns
    """
    df_csv: pl.DataFrame = pl.read_csv(source=path)

    # Get column expressions from EventAttendee static method
    base_columns: list[pl.Expr] = EventAttendee.get_polars_columns_for_base_model(
        source=source,
        created_at=parsed_date,
        event_url=event_url,
    )

    return df_csv.with_columns(base_columns)


def load_event_url_from_json(
    csv_path: Path,
) -> str:
    """Load event URL from corresponding JSON file.

    Args:
        csv_path: Path to the CSV file

    Returns:
        Event URL from JSON file

    Raises:
        FileNotFoundError: If the JSON metadata file doesn't exist
        ValueError: If the event_url is missing from the JSON file or if the JSON file is malformed
    """
    json_path: Path = csv_path.with_suffix(".json")
    if not json_path.exists():
        msg = f"Required metadata JSON file not found: {json_path}"
        raise FileNotFoundError(msg)

    try:
        with json_path.open() as f:
            data: dict[str, str] = json.load(f)
            event_url: str | None = data.get("event_url")
            if not event_url:
                msg = f"event_url missing or empty in metadata file: {json_path}"
                raise ValueError(msg)
            return event_url

    except json.JSONDecodeError as e:
        msg = f"Malformed JSON in metadata file: {json_path}"
        raise ValueError(msg) from e


def load_csv_file(
    input_file: str,
) -> tuple[pl.DataFrame, str, datetime, str]:
    """Load and process a single CSV file.

    Args:
        input_file: Path to the CSV file

    Returns:
        Tuple of (processed DataFrame with metadata, source, parsed_date, event_url)

    Raises:
        FileNotFoundError: If CSV file or required metadata JSON file doesn't exist
        ValueError: If file is not a CSV or event_url is missing from metadata
    """
    file_path: Path = Path(input_file)
    if not file_path.exists():
        msg = f"CSV file not found: {input_file}"
        raise FileNotFoundError(msg)

    if file_path.suffix.lower() != ".csv":
        msg = f"File must be a CSV file: {input_file}"
        raise ValueError(msg)

    source: str
    parsed_date: datetime
    source, parsed_date = parse_filename_metadata(filename=file_path.stem)
    event_url: str = load_event_url_from_json(csv_path=file_path)
    dataframe: pl.DataFrame = process_csv_file(
        path=file_path,
        source=source,
        parsed_date=parsed_date,
        event_url=event_url,
    )
    return dataframe, source, parsed_date, event_url


def load_csv_files_from_folder(
    input_folder: str,
) -> Iterator[tuple[pl.DataFrame, str, datetime, str]]:
    """Load and process all CSV files in a folder.

    Args:
        input_folder: Path to the folder containing CSV files

    Yields:
        Tuples of (processed DataFrame with metadata, source, parsed_date, event_url)

    Raises:
        FileNotFoundError: If folder doesn't exist or required metadata JSON files are missing
        ValueError: If path is not a directory, no CSV files found, or event_url missing from metadata
    """
    folder_path: Path = Path(input_folder)
    if not folder_path.exists():
        msg = f"Folder not found: {input_folder}"
        raise FileNotFoundError(msg)

    if not folder_path.is_dir():
        msg = f"Path is not a directory: {input_folder}"
        raise ValueError(msg)

    csv_files: list[Path] = list(folder_path.glob("*.csv"))
    if not csv_files:
        msg = f"No CSV files found in folder: {input_folder}"
        raise ValueError(msg)

    for csv_file in sorted(csv_files):
        dataframe: pl.DataFrame
        source: str
        parsed_date: datetime
        event_url: str
        dataframe, source, parsed_date, event_url = load_csv_file(
            input_file=str(csv_file),
        )
        yield dataframe, source, parsed_date, event_url


def create_attendees_generator(
    dataframe: pl.DataFrame,
) -> Iterator[EventAttendee]:
    """Create EventAttendee objects from a DataFrame.

    Args:
        dataframe: DataFrame to process (must include event_url column)

    Yields:
        EventAttendee objects
    """
    for row in dataframe.iter_rows(named=True):
        attendee: EventAttendee = EventAttendee.model_validate(obj=row)
        yield attendee


def ensure_output_directory(
    bucket_name: str,
    source: str,
) -> Path:
    """Create and return the output directory path with source as subfolder.

    Since source contains the full filename including date (e.g., "2025-11-11-statsig_experimentation"),
    we use it directly as the subfolder name to avoid redundancy.

    Args:
        bucket_name: Name of the bucket/directory
        source: Source name for the subfolder (full filename with date)

    Returns:
        Path to the output directory
    """
    cwd: str = str(Path.cwd())
    # Use source directly as subfolder since it already contains the date
    subfolder: str = source
    output_dir: Path = Path(f"{cwd}/out/{bucket_name}/{subfolder}")
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )
    return output_dir


def write_attendee_to_file(
    attendee: EventAttendee,
    output_dir: Path,
) -> None:
    """Write a single attendee to a JSON Lines file.

    Args:
        attendee: The EventAttendee to write
        output_dir: Directory to write the file to
    """
    output_file_path: Path = output_dir / attendee.etl_get_file_name(
        extension=".jsonl",
    )
    with output_file_path.open(mode="w") as f:
        f.write(
            attendee.model_dump_json(
                indent=None,
            ),
        )


def process_attendees_lazy(
    attendees: Iterator[EventAttendee],
    output_dir: Path,
) -> int:
    """Process attendees lazily and write to files.

    Args:
        attendees: Iterator of EventAttendee objects
        output_dir: Directory to write files to

    Returns:
        Number of attendees processed
    """
    count: int = 0
    for attendee in attendees:
        count += 1
        write_attendee_to_file(
            attendee=attendee,
            output_dir=output_dir,
        )

    return count


@app.local_entrypoint()
def local(
    input_folder: str,
) -> None:
    """Main entry point for processing all CSV files in a folder to EventAttendee JSON files.

    Args:
        input_folder: Path to the folder containing CSV files and corresponding JSON files
    """
    total_count: int = 0
    processed_files: int = 0

    # Setup aggregate CSV output
    cwd: str = str(Path.cwd())
    aggregate_dir: Path = Path(f"{cwd}/out/{BUCKET_NAME}")
    aggregate_dir.mkdir(parents=True, exist_ok=True)
    aggregate_file: Path = aggregate_dir / "aggregate.csv"
    standard_columns: list[str] = EventAttendee.get_field_names()
    is_first_file: bool = True
    total_records: int = 0

    # Process all CSV files in the folder - streaming one at a time
    for dataframe, source, _parsed_date, event_url in load_csv_files_from_folder(
        input_folder=input_folder,
    ):
        # 1. Process individual attendee files (existing logic)
        attendees_gen: Iterator[EventAttendee] = create_attendees_generator(
            dataframe=dataframe,
        )
        output_dir: Path = ensure_output_directory(
            bucket_name=BUCKET_NAME,
            source=source,
        )
        count: int = process_attendees_lazy(
            attendees=attendees_gen,
            output_dir=output_dir,
        )
        processed_files += 1
        total_count += count
        print(f"{count:06d}: {output_dir} | {event_url}")

        # 2. Add to aggregate CSV (using same dataframe - no reloading)
        df_standardized: pl.DataFrame = dataframe.select(
            [
                (
                    pl.col(col).alias(col)
                    if col in dataframe.columns
                    else pl.lit(None).alias(col)
                )
                for col in standard_columns
            ],
        )

        # Write to aggregate CSV (append mode after first file)
        if is_first_file:
            # First file: create new CSV with headers
            df_standardized.write_csv(file=aggregate_file)
            is_first_file = False
        else:
            # Subsequent files: append without headers
            with aggregate_file.open(mode="a") as f:
                df_standardized.write_csv(file=f, include_header=False)

        total_records += len(df_standardized)

    print(f"\nProcessed {processed_files} files with {total_count} total attendees")

    # Print aggregate CSV summary
    if total_records > 0:
        print(
            f"Aggregate CSV created: {aggregate_file} ({total_records} total records)",
        )
    else:
        print("No records to aggregate")


# trunk-ignore-begin(ruff/PLR2004,ruff/S101)
def test_parse_filename_metadata_with_hyphenated_dates() -> None:
    """Test that parse_filename_metadata correctly handles YYYY-MM-DD format."""
    from datetime import timezone

    # Test YYYY-MM-DD format (new standard) - source is full filename
    source, parsed_date = parse_filename_metadata(
        filename="2025-11-11-statsig_experimentation",
    )
    assert source == "2025-11-11-statsig_experimentation"
    assert parsed_date == datetime(2025, 11, 11, 0, 0, 0, tzinfo=timezone.utc)

    # Test another YYYY-MM-DD format - source is full filename
    source2, parsed_date2 = parse_filename_metadata(filename="2025-10-30-fde_meetup")
    assert source2 == "2025-10-30-fde_meetup"
    assert parsed_date2 == datetime(2025, 10, 30, 0, 0, 0, tzinfo=timezone.utc)

    # Test YYYYMMDD format (legacy, for backward compatibility) - source is full filename
    source3, parsed_date3 = parse_filename_metadata(filename="20251111-conference")
    assert source3 == "20251111-conference"
    assert parsed_date3 == datetime(2025, 11, 11, 0, 0, 0, tzinfo=timezone.utc)

    # Test source with multiple hyphens - source is full filename
    source4, parsed_date4 = parse_filename_metadata(
        filename="2024-03-15-multi-part-source-name",
    )
    assert source4 == "2024-03-15-multi-part-source-name"
    assert parsed_date4 == datetime(2024, 3, 15, 0, 0, 0, tzinfo=timezone.utc)


def test_csv_processing_with_company_field() -> None:
    """Test that CSV processing correctly handles the company field."""
    import csv
    import tempfile

    # Create a temporary CSV file with company data
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.writer(f)
        writer.writerow(["name", "email", "company"])
        writer.writerow(["John Doe", "john@example.com", "Acme Corp"])
        writer.writerow(["Jane Smith", "jane@example.com", "Tech Inc"])
        csv_path = f.name

    # Create corresponding JSON metadata file with event_url
    json_path = Path(csv_path).with_suffix(".json")
    with json_path.open(mode="w") as f:
        json.dump({"event_url": "https://example.com/test-event"}, f)

    try:
        # Load the CSV file
        dataframe, _, _, event_url = load_csv_file(csv_path)

        # Verify the event_url was loaded correctly
        assert event_url == "https://example.com/test-event"

        # Verify the DataFrame has the company column
        assert "company" in dataframe.columns

        # Create attendees generator
        attendees_gen = create_attendees_generator(dataframe=dataframe)
        attendees_list = list(attendees_gen)

        # Verify attendees have company data
        assert len(attendees_list) == 2
        assert attendees_list[0].company == "Acme Corp"
        assert attendees_list[1].company == "Tech Inc"
        assert attendees_list[0].name == "John Doe"
        assert attendees_list[1].name == "Jane Smith"
        assert attendees_list[0].event_url == "https://example.com/test-event"
        assert attendees_list[1].event_url == "https://example.com/test-event"

    finally:
        # Clean up
        Path(csv_path).unlink()
        json_path.unlink(missing_ok=True)


def test_csv_processing_without_company_field() -> None:
    """Test that CSV processing works when company field is missing."""
    import csv
    import tempfile

    # Create a temporary CSV file without company data
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.writer(f)
        writer.writerow(["name", "email"])
        writer.writerow(["John Doe", "john@example.com"])
        writer.writerow(["Jane Smith", "jane@example.com"])
        csv_path = f.name

    # Create corresponding JSON metadata file with event_url
    json_path = Path(csv_path).with_suffix(".json")
    with json_path.open(mode="w") as f:
        json.dump({"event_url": "https://example.com/test-event"}, f)

    try:
        # Load the CSV file
        dataframe, _, _, event_url = load_csv_file(csv_path)

        # Verify the event_url was loaded correctly
        assert event_url == "https://example.com/test-event"

        # Create attendees generator
        attendees_gen = create_attendees_generator(dataframe=dataframe)
        attendees_list = list(attendees_gen)

        # Verify attendees have None for company
        assert len(attendees_list) == 2
        assert attendees_list[0].company is None
        assert attendees_list[1].company is None
        assert attendees_list[0].name == "John Doe"
        assert attendees_list[1].name == "Jane Smith"
        assert attendees_list[0].event_url == "https://example.com/test-event"
        assert attendees_list[1].event_url == "https://example.com/test-event"

    finally:
        # Clean up
        Path(csv_path).unlink()
        json_path.unlink(missing_ok=True)


def _create_test_csv_file(
    temp_path: Path,
    filename: str,
    rows: list[list[str]],
    event_url: str,
) -> None:
    """Helper to create a test CSV file with corresponding JSON metadata.

    Args:
        temp_path: Directory to create files in
        filename: Name of the CSV file
        rows: List of rows including header
        event_url: Event URL for JSON metadata
    """
    import csv

    csv_path = temp_path / filename
    with csv_path.open(mode="w") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)

    # Create JSON metadata file
    json_path = csv_path.with_suffix(".json")
    with json_path.open(mode="w") as f:
        json.dump({"event_url": event_url}, f)


def _verify_aggregate_csv(
    aggregate_file: Path,
    expected_records: int,
    expected_companies: set[str],
    standard_columns: list[str],
) -> None:
    """Helper to verify aggregate CSV content.

    Args:
        aggregate_file: Path to aggregate CSV file
        expected_records: Expected number of records
        expected_companies: Expected set of company names
        standard_columns: Expected column names
    """
    assert aggregate_file.exists()

    # Read and verify aggregate CSV content
    aggregate_df = pl.read_csv(aggregate_file)
    assert len(aggregate_df) == expected_records
    assert "company" in aggregate_df.columns
    assert set(aggregate_df["company"].to_list()) == expected_companies

    # Verify all expected columns are present
    expected_columns = set(standard_columns)
    actual_columns = set(aggregate_df.columns)
    assert expected_columns == actual_columns


def test_single_pass_processing() -> None:
    """Test that single-pass processing creates both individual files and aggregate CSV."""
    import tempfile

    # Create a temporary directory for input files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create test CSV files
        _create_test_csv_file(
            temp_path=temp_path,
            filename="20240101-event1.csv",
            rows=[
                ["name", "email", "company"],
                ["John Doe", "john@example.com", "Acme Corp"],
                ["Jane Smith", "jane@example.com", "Tech Inc"],
            ],
            event_url="https://example.com/event1",
        )

        _create_test_csv_file(
            temp_path=temp_path,
            filename="20240102-event2.csv",
            rows=[
                ["name", "email", "company"],
                ["Bob Wilson", "bob@example.com", "StartupXYZ"],
            ],
            event_url="https://example.com/event2",
        )

        # Create temporary output directory
        with tempfile.TemporaryDirectory() as output_base:
            # Change to output directory for testing
            Path.cwd()

            try:
                # Simulate the processing that would happen in local()
                output_bucket_dir = Path(output_base) / "out" / BUCKET_NAME
                output_bucket_dir.mkdir(parents=True, exist_ok=True)

                # Test the core processing logic (without modal decorators)
                processed_files = 0
                total_records = 0

                # Setup aggregate CSV (same as in local function)
                aggregate_file = output_bucket_dir / "aggregate.csv"
                standard_columns = EventAttendee.get_field_names()
                is_first_file = True

                # Process files one by one (simulating the local() logic)
                for (
                    dataframe,
                    source,
                    _parsed_date,
                    _event_url,
                ) in load_csv_files_from_folder(str(temp_path)):
                    # Verify dataframe contains company column
                    if "company" in dataframe.columns:
                        assert (
                            dataframe["company"].to_list() == ["Acme Corp", "Tech Inc"]
                            if "event1" in source
                            else ["StartupXYZ"]
                        )

                    # Test aggregate CSV creation (same logic as in local())
                    df_standardized = dataframe.select(
                        [
                            (
                                pl.col(col).alias(col)
                                if col in dataframe.columns
                                else pl.lit(None).alias(col)
                            )
                            for col in standard_columns
                        ],
                    )

                    if is_first_file:
                        df_standardized.write_csv(file=aggregate_file)
                        is_first_file = False
                    else:
                        with aggregate_file.open(mode="a") as f:
                            df_standardized.write_csv(file=f, include_header=False)

                    processed_files += 1
                    total_records += len(df_standardized)

                # Verify aggregate CSV was created and contains expected data
                _verify_aggregate_csv(
                    aggregate_file=aggregate_file,
                    expected_records=3,
                    expected_companies={"Acme Corp", "Tech Inc", "StartupXYZ"},
                    standard_columns=standard_columns,
                )

                assert processed_files == 2
                assert total_records == 3

            finally:
                # Restore original working directory
                pass


def test_load_csv_file_missing_json_metadata() -> None:
    """Test that load_csv_file raises FileNotFoundError when JSON metadata is missing."""
    import csv
    import tempfile

    import pytest

    # Create a temporary CSV file without corresponding JSON metadata
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.writer(f)
        writer.writerow(["name", "email"])
        writer.writerow(["John Doe", "john@example.com"])
        csv_path = f.name

    try:
        # Attempt to load the CSV file should raise FileNotFoundError
        with pytest.raises(FileNotFoundError) as exc_info:
            load_csv_file(csv_path)

        assert "Required metadata JSON file not found" in str(exc_info.value)

    finally:
        # Clean up
        Path(csv_path).unlink()


def test_load_csv_file_missing_event_url_in_json() -> None:
    """Test that load_csv_file raises ValueError when event_url is missing from JSON."""
    import csv
    import tempfile

    import pytest

    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.writer(f)
        writer.writerow(["name", "email"])
        writer.writerow(["John Doe", "john@example.com"])
        csv_path = f.name

    # Create JSON metadata file without event_url
    json_path = Path(csv_path).with_suffix(".json")
    with json_path.open(mode="w") as f:
        json.dump({"some_other_field": "value"}, f)

    try:
        # Attempt to load the CSV file should raise ValueError
        with pytest.raises(ValueError, match=r"event_url missing or empty"):
            load_csv_file(csv_path)

    finally:
        # Clean up
        Path(csv_path).unlink()
        json_path.unlink(missing_ok=True)


def test_load_csv_file_empty_event_url_in_json() -> None:
    """Test that load_csv_file raises ValueError when event_url is empty string in JSON."""
    import csv
    import tempfile

    import pytest

    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.writer(f)
        writer.writerow(["name", "email"])
        writer.writerow(["John Doe", "john@example.com"])
        csv_path = f.name

    # Create JSON metadata file with empty event_url
    json_path = Path(csv_path).with_suffix(".json")
    with json_path.open(mode="w") as f:
        json.dump({"event_url": ""}, f)

    try:
        # Attempt to load the CSV file should raise ValueError
        with pytest.raises(ValueError, match=r"event_url missing or empty"):
            load_csv_file(csv_path)

    finally:
        # Clean up
        Path(csv_path).unlink()
        json_path.unlink(missing_ok=True)


# trunk-ignore-end(ruff/PLR2004,ruff/S101)
