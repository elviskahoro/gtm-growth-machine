from __future__ import annotations

from datetime import datetime  # trunk-ignore(ruff/TC003)

from pydantic import BaseModel


class MentionData(BaseModel):
    title: str | None = None
    body: str | None = None
    url: str | None = None
    timestamp: datetime | None = None
    imageUrl: str | None = None
    author: str | None = None
    authorAvatarUrl: str | None = None
    authorProfileLink: str | None = None
    authorName: str | None = None
    source: str | None = None
    sourceId: str | None = None
    relevanceScore: str | None = None
    relevanceComment: str | None = None
    keyword: str | None = None
    bookmarked: bool | None = None
    language: str | None = None


class Mention(BaseModel):
    action: str | None = None
    data: MentionData | None = None
