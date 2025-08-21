from __future__ import annotations

from datetime import datetime  # trunk-ignore(ruff/TC003)
from typing import TYPE_CHECKING

import pyarrow as pa
from pydantic import BaseModel

if TYPE_CHECKING:
    from collections.abc import Iterator


class Webhook(BaseModel):
    id: int
    at: datetime
    user_id: int
    body: str

    @staticmethod
    def modal_get_secret_collection_names() -> list[str]:
        return [
            "devx-growth-gcp",
        ]

    @staticmethod
    def etl_get_bucket_name() -> str:
        return "chalk-ai-devx-transaction-receipts"

    @staticmethod
    def storage_get_app_name() -> str:
        return f"{Webhook.etl_get_bucket_name()}-storage"

    @staticmethod
    def storage_get_base_model_type() -> type[BaseModel]:
        return Webhook

    @staticmethod
    def lance_get_project_name() -> str:
        return "transaction-example-6125m1"

    @staticmethod
    def lance_get_table_name() -> str:
        return "transaction_receipts"

    @staticmethod
    def lance_get_primary_key() -> str:
        return "id"

    @staticmethod
    def lance_get_primary_key_index_type() -> str:
        return "btree"

    @staticmethod
    def lance_get_vector_column_name() -> str:
        return "embedding"

    @staticmethod
    def lance_get_vector_dimension() -> int:
        return 768

    @staticmethod
    def lance_get_base_model_type() -> type[Webhook]:
        return Webhook

    def gemini_get_column_to_embed(self) -> str:
        return self.body

    @staticmethod
    def lance_get_schema() -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field(
                    "at",
                    pa.timestamp("us"),
                ),
                pa.field("user_id", pa.int32()),
                pa.field("body", pa.string()),
                pa.field(
                    "embedding",
                    pa.list_(
                        pa.float32(),
                        Webhook.lance_get_vector_dimension(),
                    ),
                    nullable=True,
                ),
            ],
        )

    @staticmethod
    def etl_expects_storage_file() -> bool:
        return False

    def etl_is_valid_webhook(
        self: Webhook,
    ) -> bool:
        return self.id > 0 and self.user_id > 0 and len(self.body.strip()) > 0

    def etl_get_invalid_webhook_error_msg(
        self: Webhook,
    ) -> str:
        errors = []
        if self.id <= 0:
            errors.append("Invalid transaction ID")
        if self.user_id <= 0:
            errors.append("Invalid user ID")
        if len(self.body.strip()) == 0:
            errors.append("Empty receipt body")

        return f"Invalid webhook: {', '.join(errors)}"

    def etl_get_json(
        self: Webhook,
        storage: BaseModel | None,
    ) -> str:
        return "\n".join(
            receipt.model_dump_json(
                indent=None,
            )
            for receipt in self.etl_get_base_models(
                storage=storage,
            )
        )

    def etl_get_file_name(
        self: Webhook,
    ) -> str:
        timestamp: str = self.at.strftime("%Y%m%d-%H%M%S")
        return f"{timestamp}-txn-{self.id}-user-{self.user_id}.jsonl"

    def etl_get_base_models(
        self: Webhook,
        storage: BaseModel | None,
    ) -> Iterator[Webhook]:
        del storage
        yield self
