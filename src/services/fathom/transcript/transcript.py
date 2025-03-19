from pydantic import BaseModel


class Transcript(BaseModel):
    plaintext: str
