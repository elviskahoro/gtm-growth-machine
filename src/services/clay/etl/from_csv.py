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


BUCKET_NAME: str = "chalk-ai-devx-clay-event-attendees"

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

    Args:
        filename: The filename stem (without extension)

    Returns:
        tuple: (source, parsed_date)
    """
    if "-" in filename:
        date_str: str
        source: str
        date_str, source = filename.split(
            sep="-",
            maxsplit=1,
        )
        try:
            # Parse date from YYYYMMDD format (naive, then add UTC timezone)
            parsed_date: datetime = datetime.strptime(
                date_str,
                "%Y%m%d",
            ).replace(tzinfo=timezone.utc)

        except ValueError:
            # Fallback to current datetime if parsing fails
            parsed_date = datetime.now(tz=timezone.utc)

    else:
        # Use filename as source if no date prefix
        source = filename
        parsed_date = datetime.now(tz=timezone.utc)

    return source, parsed_date


def process_csv_file(
    path: Path,
    source: str,
    parsed_date: datetime,
    event_url: str | None = None,
) -> pl.DataFrame:
    """Process a single CSV file and add metadata columns from EventAttendee model.

    Args:
        path: Path to the CSV file
        source: Source name extracted from filename
        parsed_date: Date extracted from filename
        event_url: Optional event URL to add

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
) -> str | None:
    """Load event URL from corresponding JSON file.

    Args:
        csv_path: Path to the CSV file

    Returns:
        Event URL from JSON file, or None if not found
    """
    json_path: Path = csv_path.with_suffix(".json")
    if not json_path.exists():
        return None

    try:
        with json_path.open() as f:
            data: dict[str, str] = json.load(f)
            return data.get("event_url")

    except (json.JSONDecodeError, KeyError):
        return None


def load_csv_file(
    input_file: str,
) -> tuple[pl.DataFrame, str, datetime, str | None]:
    """Load and process a single CSV file.

    Args:
        input_file: Path to the CSV file

    Returns:
        Tuple of (processed DataFrame with metadata, source, parsed_date, event_url)
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
    event_url: str | None = load_event_url_from_json(csv_path=file_path)
    dataframe: pl.DataFrame = process_csv_file(
        path=file_path,
        source=source,
        parsed_date=parsed_date,
        event_url=event_url,
    )
    return dataframe, source, parsed_date, event_url


def load_csv_files_from_folder(
    input_folder: str,
) -> Iterator[tuple[pl.DataFrame, str, datetime, str | None]]:
    """Load and process all CSV files in a folder.

    Args:
        input_folder: Path to the folder containing CSV files

    Yields:
        Tuples of (processed DataFrame with metadata, source, parsed_date, event_url)
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
        event_url: str | None
        dataframe, source, parsed_date, event_url = load_csv_file(
            input_file=str(csv_file),
        )
        yield dataframe, source, parsed_date, event_url


def create_attendees_generator(
    dataframe: pl.DataFrame,
    event_url: str | None,
) -> Iterator[EventAttendee]:
    """Create EventAttendee objects from a DataFrame.

    Args:
        dataframe: DataFrame to process
        event_url: Optional event URL to set on attendees

    Yields:
        EventAttendee objects
    """
    for row in dataframe.iter_rows(named=True):
        attendee: EventAttendee = EventAttendee.model_validate(obj=row)
        if event_url is not None:
            attendee.event_url = event_url

        yield attendee


def ensure_output_directory(
    bucket_name: str,
    source: str,
    parsed_date: datetime,
) -> Path:
    """Create and return the output directory path with date-source subfolder.

    Args:
        bucket_name: Name of the bucket/directory
        source: Source name for the subfolder
        parsed_date: Date for the subfolder

    Returns:
        Path to the output directory
    """
    cwd: str = str(Path.cwd())
    date_str: str = parsed_date.strftime("%Y%m%d")
    subfolder: str = f"{date_str}-{source}"
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


def write_aggregate_csv_streaming(
    input_folder: str,
) -> None:
    """Create an aggregate CSV file by streaming through files without loading all into memory.

    Args:
        input_folder: Path to the folder containing CSV files
    """
    # Create aggregate output directory
    cwd: str = str(Path.cwd())
    aggregate_dir: Path = Path(f"{cwd}/out/{BUCKET_NAME}")
    aggregate_dir.mkdir(parents=True, exist_ok=True)
    aggregate_file: Path = aggregate_dir / "aggregate.csv"

    # Get all EventAttendee field names as standard columns
    standard_columns: list[str] = EventAttendee.get_field_names()

    total_records: int = 0
    is_first_file: bool = True

    # Stream through files one at a time
    for dataframe, _source, _parsed_date, _event_url in load_csv_files_from_folder(
        input_folder=input_folder,
    ):
        # Standardize the schema for this dataframe
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

        # Write to CSV (append mode after first file)
        if is_first_file:
            # First file: create new CSV with headers
            df_standardized.write_csv(file=aggregate_file)
            is_first_file = False
        else:
            # Subsequent files: append without headers
            with aggregate_file.open(mode="a") as f:
                df_standardized.write_csv(file=f, include_header=False)

        total_records += len(df_standardized)

    if total_records > 0:
        print(
            f"Aggregate CSV created: {aggregate_file} ({total_records} total records)",
        )
    else:
        print("No records to aggregate")


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

    # Process all CSV files in the folder - streaming one at a time
    for dataframe, source, parsed_date, event_url in load_csv_files_from_folder(
        input_folder=input_folder,
    ):
        attendees_gen: Iterator[EventAttendee] = create_attendees_generator(
            dataframe=dataframe,
            event_url=event_url,
        )
        output_dir: Path = ensure_output_directory(
            bucket_name=BUCKET_NAME,
            source=source,
            parsed_date=parsed_date,
        )
        count: int = process_attendees_lazy(
            attendees=attendees_gen,
            output_dir=output_dir,
        )
        processed_files += 1
        total_count += count
        event_url_display: str = event_url or "No event URL"
        print(f"{count:06d}: {output_dir} | {event_url_display}")

    print(f"\nProcessed {processed_files} files with {total_count} total attendees")

    # Create aggregate CSV using streaming approach (no dataframes stored in memory)
    write_aggregate_csv_streaming(input_folder=input_folder)


# trunk-ignore-begin(ruff/PLR2004,ruff/S101)
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

    try:
        # Load the CSV file
        dataframe, _, _, event_url = load_csv_file(csv_path)

        # Verify the DataFrame has the company column
        assert "company" in dataframe.columns

        # Create attendees generator
        attendees_gen = create_attendees_generator(dataframe, event_url)
        attendees_list = list(attendees_gen)

        # Verify attendees have company data
        assert len(attendees_list) == 2
        assert attendees_list[0].company == "Acme Corp"
        assert attendees_list[1].company == "Tech Inc"
        assert attendees_list[0].name == "John Doe"
        assert attendees_list[1].name == "Jane Smith"

    finally:
        # Clean up
        Path(csv_path).unlink()


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

    try:
        # Load the CSV file
        dataframe, _, _, event_url = load_csv_file(csv_path)

        # Create attendees generator
        attendees_gen = create_attendees_generator(dataframe, event_url)
        attendees_list = list(attendees_gen)

        # Verify attendees have None for company
        assert len(attendees_list) == 2
        assert attendees_list[0].company is None
        assert attendees_list[1].company is None
        assert attendees_list[0].name == "John Doe"
        assert attendees_list[1].name == "Jane Smith"

    finally:
        # Clean up
        Path(csv_path).unlink()


# trunk-ignore-end(ruff/PLR2004,ruff/S101)
