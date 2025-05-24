# trunk-ignore-all(ruff/ANN401)
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import AliasChoices, BaseModel, Field, field_validator

from src.services.local.filesystem import (
    file_clean_string,
    file_clean_timestamp_from_datetime,
)


class Mention(BaseModel):
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
    author_profile_link: str | None = Field(
        default=None,
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
        source: str = file_clean_string(self.source)
        keyword: str = file_clean_string(self.keyword)
        author: str = file_clean_string(self.author)
        timestamp: str = file_clean_timestamp_from_datetime(self.timestamp)
        return f"{source}-{keyword}-{timestamp}-{author}{extension}"
