from __future__ import annotations

from enum import Enum
from pathlib import Path


class DestinationType(str, Enum):
    LOCAL = "local"
    GCP = "gcp"

    @staticmethod
    def get_bucket_url_from_bucket_name_for_local(
        bucket_name: str,
    ) -> str:
        cwd: str = str(Path.cwd())
        return f"{cwd}/out/{bucket_name}"
