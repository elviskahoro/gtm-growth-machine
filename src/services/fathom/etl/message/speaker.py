from __future__ import annotations

from pydantic import BaseModel, EmailStr, Field


class Speaker(BaseModel):
    name: str = Field(
        ...,
        description="Name of the speaker",
    )
    email: EmailStr = Field(
        ...,
        description="Email address of the entity",
    )
    aliases: list[str] = Field(
        default_factory=list,
        description="List of alternative names or aliases",
    )

    @staticmethod
    def build_speaker_lookup_map(
        speakers: list[Speaker],
    ) -> dict[str, str]:
        lookup_map: dict[str, str] = {}
        for speaker in speakers:
            lookup_map[speaker.name.lower()] = speaker.email
            for alias in speaker.aliases:
                lookup_map[alias.lower()] = speaker.email

        return lookup_map

    @staticmethod
    def get_email_by_name_with_lookup(
        lookup_map: dict[str, str],
        search_name: str,
    ) -> str:
        return lookup_map.get(search_name.lower(), search_name)
