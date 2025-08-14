from __future__ import annotations

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
        date_str, source = filename.split(sep="-", maxsplit=1)
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
) -> pl.DataFrame:
    """Process a single CSV file and add metadata columns.

    Args:
        path: Path to the CSV file

    Returns:
        DataFrame with added metadata columns
    """
    df_csv: pl.DataFrame = pl.read_csv(source=path)
    source: str
    parsed_date: datetime
    source, parsed_date = parse_filename_metadata(filename=path.stem)
    return df_csv.with_columns(
        [
            pl.lit(source).alias("source"),
            pl.lit(parsed_date).alias("created_at"),
        ],
    )


def load_csv_file(
    input_file: str,
) -> pl.DataFrame:
    """Load and process a single CSV file.

    Args:
        input_file: Path to the CSV file

    Returns:
        Processed DataFrame with metadata
    """
    file_path: Path = Path(input_file)
    if not file_path.exists():
        msg = f"CSV file not found: {input_file}"
        raise FileNotFoundError(msg)

    if file_path.suffix.lower() != ".csv":
        msg = f"File must be a CSV file: {input_file}"
        raise ValueError(msg)

    return process_csv_file(path=file_path)


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
) -> Path:
    """Create and return the output directory path.

    Args:
        bucket_name: Name of the bucket/directory

    Returns:
        Path to the output directory
    """
    cwd: str = str(Path.cwd())
    output_dir: Path = Path(f"{cwd}/out/{bucket_name}")
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
        write_attendee_to_file(attendee=attendee, output_dir=output_dir)

    return count


@app.local_entrypoint()
def local(
    input_file: str,
    event_url: str | None,
) -> None:
    """Main entry point for processing a single CSV file to EventAttendee JSON files.

    Args:
        input_file: Path to the CSV file
    """
    # Load and process the single CSV file
    dataframe: pl.DataFrame = load_csv_file(input_file=input_file)
    attendees_gen: Iterator[EventAttendee] = create_attendees_generator(
        dataframe=dataframe,
        event_url=event_url,
    )
    output_dir: Path = ensure_output_directory(bucket_name=BUCKET_NAME)
    count: int = process_attendees_lazy(attendees=attendees_gen, output_dir=output_dir)

    print(f"{count:06d}: {output_dir}")
