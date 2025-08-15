from __future__ import annotations

from pydantic import BaseModel, Field

from .speaker import Speaker  # trunk-ignore(ruff/TC001)


class Storage(BaseModel):
    speakers_internal: list[Speaker] = Field(
        default_factory=list,
        description="List of speakers with their emails and aliases",
    )
