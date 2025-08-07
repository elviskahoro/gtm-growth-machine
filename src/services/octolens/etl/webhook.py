from __future__ import annotations

from pydantic import BaseModel

from src.services.octolens.mention import Mention  # trunk-ignore(ruff/TC001)


class Webhook(BaseModel):
    action: str = "mention_created"
    data: Mention

    @staticmethod
    def modal_get_secret_collection_names() -> list[str]:
        return [
            "devx-growth-gcp",
        ]

    @staticmethod
    def etl_get_bucket_name() -> str:
        return "chalk-ai-devx-octolens-mentions-etl"

    @staticmethod
    def storage_get_app_name() -> None:
        error: str = "Storage app name is not defined for Webhook."
        raise NotImplementedError(error)

    @staticmethod
    def storage_get_base_model_type() -> None:
        return None

    def etl_get_file_name(
        self: Webhook,
        extension: str = ".jsonl",
    ) -> str:
        return self.data.get_file_name(
            extension=extension,
        )

    def etl_is_valid_webhook(
        self: Webhook,
    ) -> bool:
        match self.action:
            case "mention_created":
                return True

            case _:
                return False

    def etl_get_invalid_webhook_error_msg(
        self: Webhook,
    ) -> str:
        return "Invalid webhook: " + self.action

    def etl_get_json(
        self: Webhook,
        storage: None,
    ) -> str:
        del storage
        return self.data.model_dump_json(
            indent=None,
        )

    def etl_get_base_models(
        self: Webhook,
        storage: None,
    ) -> None:
        del storage
        error: str = "Webhook does not support getting base models."
        raise NotImplementedError(error)
