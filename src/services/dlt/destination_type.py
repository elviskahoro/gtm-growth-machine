from __future__ import annotations

from enum import Enum
from pathlib import Path

from src.services.dlt.filesystem_gcp import CloudGoogle


class DestinationType(str, Enum):
    LOCAL = "local"
    GCP = "gcp"

    @staticmethod
    def get_bucket_url_from_bucket_name_for_local(
        bucket_name: str,
    ) -> str:
        cwd: str = str(Path.cwd())
        return f"{cwd}/out/{bucket_name}"

    def get_bucket_url_from_bucket_name(
        self: DestinationType,
        bucket_name: str,
    ) -> str:
        match self:
            case DestinationType.LOCAL:
                return DestinationType.get_bucket_url_from_bucket_name_for_local(
                    bucket_name=bucket_name,
                )
            case DestinationType.GCP:
                return CloudGoogle.bucket_url_from_bucket_name(
                    bucket_name=bucket_name,
                )
            case _:
                error_msg: str = f"Invalid destination type: {self}"
                raise ValueError(error_msg)
