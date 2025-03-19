from pydantic import BaseModel


class Recording(BaseModel):
    url: str
    duration_in_minutes: float
