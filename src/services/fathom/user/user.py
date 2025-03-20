from pydantic import BaseModel


class FathomUser(BaseModel):
    name: str
    email: str
    team: str
