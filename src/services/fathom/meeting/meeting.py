from datetime import datetime

from pydantic import BaseModel


class ExternalDomain(BaseModel):
    domain_name: str


class Invitee(BaseModel):
    name: str
    email: str
    is_external: bool


class Meeting(BaseModel):
    scheduled_start_time: datetime
    scheduled_end_time: datetime
    scheduled_duration_in_minutes: int
    join_url: str
    title: str
    has_external_invitees: bool | None
    external_domains: list[ExternalDomain]
    invitees: list[Invitee]
