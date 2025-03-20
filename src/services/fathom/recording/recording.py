import re

from pydantic import BaseModel


class Recording(BaseModel):
    url: str
    duration_in_minutes: float

    def get_recording_id_from_url(
        self,
    ) -> int | None:
        match: re.Match[str] | None = re.search(
            pattern=r"/calls/(\d+)",
            string=self.url,
        )
        if match:
            return int(match.group(1))

        return None
