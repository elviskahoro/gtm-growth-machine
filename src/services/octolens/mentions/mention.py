# trunk-ignore-all(ruff/ANN401)
from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from pydantic import AliasChoices, BaseModel, Field, field_validator

from src.services.local.regex import FILE_SYSTEM_TRANSLATION


class MentionData(BaseModel):
    url: str = Field(
        validation_alias=AliasChoices(
            "url",
            "URL",
        ),
    )
    title: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "title",
            "Title",
        ),
    )
    body: str = Field(
        validation_alias=AliasChoices(
            "body",
            "Body",
        ),
    )
    timestamp: datetime = Field(
        validation_alias=AliasChoices(
            "timestamp",
            "Timestamp",
        ),
    )
    image_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "image_url",
            "Image URL",
            "imageUrl",
        ),
    )
    source: str = Field(
        validation_alias=AliasChoices(
            "source",
            "Source",
        ),
    )
    source_id: str = Field(
        validation_alias=AliasChoices(
            "source_id",
            "Source ID",
            "sourceId",
        ),
    )
    author: str = Field(
        validation_alias=AliasChoices(
            "author",
            "Author",
        ),
    )
    author_avatar_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "author_avatar_url",
            "Author Avatar URL",
            "authorAvatarUrl",
        ),
    )
    author_profile_link: str = Field(
        validation_alias=AliasChoices(
            "author_profile_link",
            "Author Profile Link",
            "authorProfileLink",
        ),
    )
    relevance_score: str = Field(
        validation_alias=AliasChoices(
            "relevance_score",
            "Relevance Score",
            "relevanceScore",
        ),
    )
    relevance_comment: str = Field(
        validation_alias=AliasChoices(
            "relevance_comment",
            "Relevance Comment",
            "relevanceComment",
        ),
    )
    language: str = Field(
        validation_alias=AliasChoices(
            "language",
            "Language",
        ),
    )
    keyword: str = Field(
        validation_alias=AliasChoices(
            "keyword",
            "Keyword",
        ),
    )
    bookmarked: bool = False

    @field_validator(
        "timestamp",
        mode="before",
    )
    @classmethod
    def parse_timestamp(
        cls,
        value: Any,
    ) -> datetime:
        if not isinstance(value, str):
            error_msg: str = f"Invalid timestamp format: {value}"
            raise TypeError(error_msg)

        # Try original format first
        try:
            return datetime.strptime(value, "%a %b %d %Y %H:%M:%S GMT%z")

        except ValueError:
            # Try ISO 8601 format
            try:
                return datetime.fromisoformat(value)

            except ValueError as e:
                error_msg: str = f"Invalid timestamp format: {value}"
                raise ValueError(error_msg) from e

    def get_file_name(
        self,
        extension: str = ".jsonl",
    ) -> str:
        source: str = self.source.replace(" ", "·")
        keyword: str = self.keyword.replace(" ", "·")
        # Clean author string using regex with a translation dictionary
        author: str = re.sub(
            r"[ /\\()]",
            lambda m: FILE_SYSTEM_TRANSLATION[m.group(0)],
            self.author,
        )
        timestamp: str = self.timestamp.strftime("%Y%m%d_%H%M%S")
        return f"{source}-{keyword}-{author}-{timestamp}{extension}"


class Mention(BaseModel):
    action: str = "mention_created"
    data: MentionData

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
    ) -> MentionData:
        return self.data
