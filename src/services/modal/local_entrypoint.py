from __future__ import annotations

from enum import Enum
from pathlib import Path


class DestinationType(str, Enum):
    LOCAL = "local"
    GCP = "gcp"

    @staticmethod
    def get_bucket_url_for_local(
        pipeline_name: str,
    ) -> str:
        cwd: str = str(Path.cwd())
        return f"{cwd}/out/{pipeline_name}"
