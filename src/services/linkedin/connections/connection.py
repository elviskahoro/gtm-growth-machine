from __future__ import annotations

import datetime

from pydantic import (
    BaseModel,
    EmailStr,
    FieldSerializationInfo,
    HttpUrl,
    field_serializer,
)


class LinkedinConnection(BaseModel):
    """Represents a LinkedIn connection with details like name, contact info,
    company, and connection date.
    """

    first_name: str
    last_name: str
    url: HttpUrl
    email_address: EmailStr
    company: str
    position: str
    connected_on: datetime.datetime
    timestamp: datetime.datetime

    @staticmethod
    def parse_linkedin_date(
        date_str: str,
    ) -> str:
        parsed_date = datetime.datetime.strptime(
            date_str,
            "%d %b %Y",
        )
        parsed_date = parsed_date.replace(
            tzinfo=datetime.timezone.utc,
        )
        return parsed_date.isoformat()

    @field_serializer("connected_on")
    def serialize_priority(
        self: LinkedinConnection,
        connected_on: str,
        _info: FieldSerializationInfo,
    ) -> str:
        del _info
        return LinkedinConnection.parse_linkedin_date(
            date_str=connected_on,
        )
