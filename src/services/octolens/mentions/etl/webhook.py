from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from src.services.octolens.mentions.mention import Mention


class Webhook(BaseModel):
    action: str = "mention_created"
    data: Mention

    def etl_get_file_name(
        self,
        extension: str = ".jsonl",
    ) -> str:
        return self.data.get_file_name(
            extension=extension,
        )

    def etl_is_valid_webhook(
        self,
    ) -> bool:
        match self.action:
            case "mention_created":
                return True

            case _:
                return False

    def etl_get_invalid_webhook_error_msg(
        self,
    ) -> str:
        return "Invalid webhook: " + self.action

    def etl_get_data(
        self,
    ) -> Mention:
        return self.data
