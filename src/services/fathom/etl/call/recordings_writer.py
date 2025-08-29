from __future__ import annotations

import json
from pathlib import Path


class RecordingWriter:
    """Writer for immediate persistence of recording IDs with configurable status."""

    def __init__(self, status: str) -> None:
        """Initialize the immediate writer.

        Args:
            status: The status type (e.g., 'successful', 'failed')
        """
        self.status = status
        self.output_path: Path = Path(
            f"out/fathom-call-backfill_{status}_recordings.json",
        )
        self._existing_recordings: set[str] = self._load_existing_recordings()

        # Ensure output directory exists
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def _load_existing_recordings(self) -> set[str]:
        """Load existing recordings once at initialization."""
        if not self.output_path.exists():
            return set()

        try:
            with self.output_path.open(mode="r") as f:
                recordings: list[str] = json.load(f)
                return set(recordings)

        except (json.JSONDecodeError, ValueError):
            print(
                f"Warning: Could not load {self.status} recordings file, starting fresh",
            )
            return set()

    def add(self, recording_id: str) -> None:
        """Add a recording ID and immediately write to disk."""
        if recording_id not in self._existing_recordings:
            self._existing_recordings.add(recording_id)
            try:
                with self.output_path.open(mode="w") as f:
                    json.dump(sorted(self._existing_recordings), f, indent=2)
                print(f"Written {self.status} recording {recording_id} to disk")

            except (OSError, json.JSONEncodeError) as e:
                print(
                    f"ERROR: Failed to write {self.status} recording {recording_id} to disk: {e}",
                )
                # Remove from in-memory set since write failed
                self._existing_recordings.discard(recording_id)
                raise

    def __enter__(self) -> "RecordingWriter":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # trunk-ignore(ruff/ANN001)
        """Exit the context manager (no cleanup needed for immediate writes)."""
