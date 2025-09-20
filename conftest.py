"""Root conftest.py file for pytest configuration.
This file contains project-wide pytest configuration and hooks.
"""

# Files to exclude from pytest collection
collect_ignore = [
    "src/services/runner/webhook_etl.py",
    "src/services/runner/webhook_raw.py",
    "src/services/octolens/etl/from_csv.py",
    "src/services/fathom/etl/message/from_srt.py",
    "src/services/runner/export_to_modal_storage.py",
    "src/services/runner/export_to_gcp_etl.py",
    "src/services/fathom/etl/call/backfill.py",
]
