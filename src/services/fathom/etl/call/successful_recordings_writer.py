from __future__ import annotations

import json
from pathlib import Path


class SuccessfulRecordingsWriter:
    """Writer for immediate persistence of successful recording IDs."""

    def __init__(self) -> None:
        """Initialize the immediate writer."""
        self.output_path: Path = Path(
            "out/fathom-call-backfill_successful_recordings.json",
        )
        self._existing_recordings: set[str] = self._load_existing_recordings()

        # Ensure output directory exists
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def _load_existing_recordings(self) -> set[str]:
        """Load existing successful recordings once at initialization."""
        if not self.output_path.exists():
            return set()

        try:
            with self.output_path.open(mode="r") as f:
                recordings: list[str] = json.load(f)
                return set(recordings)

        except (json.JSONDecodeError, ValueError):
            print("Warning: Could not load successful recordings file, starting fresh")
            return set()

    def add(self, recording_id: str) -> None:
        """Add a recording ID and immediately write to disk."""
        if recording_id not in self._existing_recordings:
            self._existing_recordings.add(recording_id)
            with self.output_path.open(mode="w") as f:
                json.dump(sorted(self._existing_recordings), f, indent=2)

            print(f"Written successful recording {recording_id} to disk")

    def __enter__(self) -> "SuccessfulRecordingsWriter":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # trunk-ignore(ruff/ANN001)
        """Exit the context manager (no cleanup needed for immediate writes)."""
