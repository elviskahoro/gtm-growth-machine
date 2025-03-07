from __future__ import annotations

from datetime import datetime  # trunk-ignore(ruff/TC003)
from typing import Any

from pydantic import AliasChoices, BaseModel, Field, field_validator


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

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, value: Any) -> datetime:
        if isinstance(value, str):
            # Try original format first
            try:
                return datetime.strptime(value, "%a %b %d %Y %H:%M:%S GMT%z")

            except ValueError:
                # Try ISO 8601 format
                try:
                    return datetime.fromisoformat(value)

                except ValueError as e:
                    raise ValueError(f"Invalid timestamp format: {value}") from e

        return value


class Mention(BaseModel):
    action: str = "mention_created"
    data: MentionData
