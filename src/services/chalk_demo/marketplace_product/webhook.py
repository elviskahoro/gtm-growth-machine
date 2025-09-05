from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
from pydantic import BaseModel

if TYPE_CHECKING:
    from collections.abc import Iterator


class Webhook(BaseModel):
    title: str
    id: int

    @staticmethod
    def modal_get_secret_collection_names() -> list[str]:
        return [
            "devx-growth-gcp",
        ]

    @staticmethod
    def etl_get_bucket_name() -> str:
        return "chalk-ai-devx-marketplace-products"

    @staticmethod
    def storage_get_app_name() -> str:
        return f"{Webhook.etl_get_bucket_name()}-storage"

    @staticmethod
    def storage_get_base_model_type() -> type[BaseModel]:
        return Webhook

    @staticmethod
    def lance_get_project_name() -> str:
        return "marketplace-x205j4"

    @staticmethod
    def lance_get_table_name() -> str:
        return "marketplace_products"

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
        return self.title

    @staticmethod
    def lance_get_schema() -> pa.Schema:
        return pa.schema(
            [
                pa.field("title", pa.string()),
                pa.field("id", pa.int32()),
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
        return self.id > 0 and len(self.title.strip()) > 0

    def etl_get_invalid_webhook_error_msg(
        self: Webhook,
    ) -> str:
        errors = []
        if self.id <= 0:
            errors.append("Invalid product ID")

        if len(self.title.strip()) == 0:
            errors.append("Empty product title")

        return f"Invalid webhook: {', '.join(errors)}"

    def etl_get_json(
        self: Webhook,
        storage: BaseModel | None,
    ) -> str:
        return "\n".join(
            product.model_dump_json(
                indent=None,
            )
            for product in self.etl_get_base_models(
                storage=storage,
            )
        )

    def etl_get_file_name(
        self: Webhook,
    ) -> str:
        return f"product-{self.id}.jsonl"

    def etl_get_base_models(
        self: Webhook,
        storage: BaseModel | None,
    ) -> Iterator[Webhook]:
        del storage
        yield self
