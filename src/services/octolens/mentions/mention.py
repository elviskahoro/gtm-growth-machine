from __future__ import annotations

from datetime import datetime  # trunk-ignore(ruff/TC003)
from typing import Any

from pydantic import BaseModel, Field, field_validator


class MentionData(BaseModel):
    url: str = Field(
        alias="URL",
    )
    title: str = Field(
        alias="Title",
    )
    body: str = Field(
        alias="Body",
    )
    timestamp: datetime = Field(
        alias="Timestamp",
    )
    imageUrl: str = Field(
        alias="Image URL",
    )
    source: str = Field(
        alias="Source",
    )
    sourceId: str = Field(
        alias="Source ID",
    )
    author: str = Field(
        alias="Author",
    )
    authorAvatarUrl: str = Field(
        alias="Author Avatar URL",
    )
    authorProfileLink: str = Field(
        alias="Author Profile Link",
    )
    relevanceScore: str = Field(
        alias="Relevance Score",
    )
    relevanceComment: str = Field(
        alias="Relevance Comment",
    )
    language: str = Field(
        alias="Language",
    )
    keyword: str = Field(
        alias="Keyword",
    )
    bookmarked: bool = False

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, value: Any) -> datetime:
        if isinstance(value, str):
            try:
                return datetime.strptime(value, "%a %b %d %Y %H:%M:%S GMT%z")

            except ValueError as e:
                raise ValueError(f"Invalid timestamp format: {value}") from e

        return value


class Mention(BaseModel):
    action: str = "mention_created"
    data: MentionData
